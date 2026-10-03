"""
Online machine learning on the sensor stream.

Task: one-step-ahead forecasting. Before each new reading arrives, predict its
value from the device's recent history; then learn from the true value.
This is "prequential" (test-then-train) evaluation, the standard way to
evaluate a model that learns from a stream.

Model: one River linear regression per device (with its own StandardScaler),
updated with learn_one() on every new reading. It predicts the CHANGE from
the previous reading:  target = y - last_value.

Features (all computed from past readings only, so there is no leakage):
    ewm_gap : exponentially weighted mean minus last value (mean reversion)
    d12     : last value minus the one before
    d23     : the two before that
    sin/cos : hour of day (captures the daily cycle)

Baselines it is compared against:
    naive : predict "same as last reading"
    ewm   : predict the exponentially weighted mean

Only rows that the outlier detector has scored AND did not flag are used, so
the model never learns from sensor faults. Run outlier_detection.py first.

Predictions are saved to stream.ml_predictions for the dashboard.

Usage:
    python online_ml.py           # runs forever, a pass every 30 s
    python online_ml.py --once    # catch up on available rows, then exit
"""

import argparse
import math
import time
from collections import deque
from datetime import timezone

from psycopg2.extras import execute_values

from common import connect, ensure_analytics_schema

EWM_ALPHA = 0.1
WARM_START_ROWS = 20000   # on startup, re-learn from this many recent rows
BATCH_LIMIT = 5000
ROLLING_WINDOW = 1000
SLEEP_SECONDS = 30


def make_model():
    from river import linear_model, optim, preprocessing

    return preprocessing.StandardScaler() | linear_model.LinearRegression(
        optimizer=optim.SGD(0.005)
    )


class DeviceState:
    def __init__(self, model):
        self.model = model
        self.lags = deque(maxlen=3)  # most recent value last
        self.ewm = None

    def ready(self):
        return len(self.lags) == 3 and self.ewm is not None

    def features(self, ts):
        l3, l2, l1 = self.lags
        hour = ts.hour + ts.minute / 60
        angle = 2 * math.pi * hour / 24
        return {
            "ewm_gap": self.ewm - l1,
            "d12": l1 - l2,
            "d23": l2 - l3,
            "sin": math.sin(angle),
            "cos": math.cos(angle),
        }

    def update(self, value):
        self.lags.append(value)
        self.ewm = value if self.ewm is None else (
            EWM_ALPHA * value + (1 - EWM_ALPHA) * self.ewm
        )


class OnlineForecaster:
    """Per-device online models + running error metrics."""

    def __init__(self, model_factory=make_model, rolling_window=ROLLING_WINDOW):
        self.model_factory = model_factory
        self.devices = {}
        self.n = 0
        self.sum_model = 0.0
        self.sum_naive = 0.0
        self.sum_ewm = 0.0
        self.recent = deque(maxlen=rolling_window)  # (model_err, naive_err)

    def step(self, device_id, ts, value):
        """
        Predict this reading, then learn from it.
        Returns (predicted, naive_pred) or None while the device is warming up.
        """
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc)  # same clock the generator uses
        st = self.devices.get(device_id)
        if st is None:
            st = self.devices[device_id] = DeviceState(self.model_factory())

        result = None
        if st.ready():
            last = st.lags[-1]
            x = st.features(ts)
            predicted = last + st.model.predict_one(x)
            naive = last

            e_model, e_naive = abs(value - predicted), abs(value - naive)
            self.n += 1
            self.sum_model += e_model
            self.sum_naive += e_naive
            self.sum_ewm += abs(value - st.ewm)
            self.recent.append((e_model, e_naive))

            st.model.learn_one(x, value - last)
            result = (predicted, naive)

        st.update(value)
        return result

    def summary(self):
        if self.n == 0:
            return "no predictions yet"
        r = len(self.recent)
        rm = sum(a for a, _ in self.recent) / r
        rn = sum(b for _, b in self.recent) / r
        gain = 100 * (1 - rm / rn) if rn else 0.0
        return (
            f"n={self.n} | MAE model={self.sum_model / self.n:.3f} "
            f"naive={self.sum_naive / self.n:.3f} ewm={self.sum_ewm / self.n:.3f} | "
            f"last {r}: model={rm:.3f} naive={rn:.3f} ({gain:+.1f}% vs naive)"
        )


# ---------------------------------------------------------------------------
# Database side
# ---------------------------------------------------------------------------

def fetch_rows(cur, after_event_id):
    cur.execute(
        """
        SELECT event_id, device_id, ts, reading_value::float8
        FROM stream.fact_event_stream
        WHERE scored AND NOT is_flagged AND event_id > %s
        ORDER BY event_id
        LIMIT %s
        """,
        (after_event_id, BATCH_LIMIT),
    )
    return cur.fetchall()


def save_predictions(cur, rows):
    execute_values(
        cur,
        """
        INSERT INTO stream.ml_predictions
            (event_id, device_id, ts, predicted, actual, naive_pred)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
        """,
        rows,
        page_size=1000,
    )


def starting_point(cur):
    cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM stream.fact_event_stream")
    return max(0, cur.fetchone()[0] - WARM_START_ROWS)


def one_pass(conn, forecaster, last_id):
    """Process available rows. Returns (new_last_id, rows_seen)."""
    with conn, conn.cursor() as cur:
        rows = fetch_rows(cur, last_id)
        if not rows:
            return last_id, 0
        out = []
        for event_id, device_id, ts, value in rows:
            res = forecaster.step(device_id, ts, value)
            if res is not None:
                out.append((event_id, device_id, ts, res[0], value, res[1]))
        if out:
            save_predictions(cur, out)
    print(f"{len(rows):5d} rows, {len(out):5d} predictions | {forecaster.summary()}",
          flush=True)
    return rows[-1][0], len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    conn = connect()
    ensure_analytics_schema(conn)
    with conn, conn.cursor() as cur:
        last_id = starting_point(cur)
    forecaster = OnlineForecaster()
    print("Online forecaster started (River linear regression per device)")

    while True:
        try:
            while True:
                last_id, seen = one_pass(conn, forecaster, last_id)
                if seen < BATCH_LIMIT:
                    break
        except Exception as exc:
            print("pass failed:", exc, flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
        if args.once:
            break
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
