"""
Cloud versions of the outlier detector and the online forecaster.

Called once a minute by the `analyze` timer function in function_app.py:
  1. detector   - robust z-score (median/MAD) per device, writes is_flagged
  2. forecaster - River online regression, writes stream.ml_predictions

Same methods as analytics/outlier_detection.py and analytics/online_ml.py, with
one important difference: a function instance can restart between runs, so the
forecaster's learned state is saved to stream.ml_state after every batch and
reloaded at the start of the next run. If the saved state cannot be loaded
(first run, or a library upgrade), it re-learns from the most recent rows.

The forecaster only learns from rows that are scored, not flagged, and whose
sensor status is not 'error', so a faulty reading can never corrupt the model.

Heavy libraries (river) are imported only when needed, so a problem there can
never stop the data generator from starting.
"""

import logging
import math
import pickle
import statistics
import time
import zlib
from collections import defaultdict, deque
from datetime import timezone

import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

# Detector
THRESHOLD = 3.5        # |robust z| above this = outlier
HISTORY_SIZE = 300     # baseline readings per device
MIN_HISTORY = 12       # below this, bootstrap the baseline from the batch too
DET_BATCH_LIMIT = 1000
LOOKBACK_ROWS = 50000
MAD_FLOOR = 1e-6

# Forecaster
EWM_ALPHA = 0.1
WARM_START_ROWS = 20000
FC_BATCH_LIMIT = 5000
ROLLING_WINDOW = 1000
STATE_NAME = "forecaster_v1"

_schema_ready = False


# ---------------------------------------------------------------------------
# Schema (runs once per worker process)
# ---------------------------------------------------------------------------

def ensure_schema(conn):
    global _schema_ready
    if _schema_ready:
        return
    with conn, conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE stream.fact_event_stream "
            "ADD COLUMN IF NOT EXISTS scored BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_unscored "
            "ON stream.fact_event_stream (event_id) WHERE NOT scored"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stream.ml_predictions (
                event_id   BIGINT PRIMARY KEY
                           REFERENCES stream.fact_event_stream(event_id),
                device_id  INT NOT NULL,
                ts         TIMESTAMPTZ NOT NULL,
                predicted  DOUBLE PRECISION NOT NULL,
                actual     DOUBLE PRECISION NOT NULL,
                naive_pred DOUBLE PRECISION NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_pred_ts ON stream.ml_predictions (ts)"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stream.ml_state (
                name          TEXT PRIMARY KEY,
                state         BYTEA NOT NULL,
                last_event_id BIGINT NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    _schema_ready = True


# ---------------------------------------------------------------------------
# Outlier detector
# ---------------------------------------------------------------------------

def robust_z(value, baseline):
    med = statistics.median(baseline)
    mad = statistics.median(abs(b - med) for b in baseline)
    return 0.6745 * (value - med) / max(mad, MAD_FLOOR)


def flag_batch(batch, history):
    """batch: [(event_id, device_id, value)], history: {device_id: [clean values]}"""
    by_device = defaultdict(list)
    for _, device_id, value in batch:
        by_device[device_id].append(value)

    baselines = {}
    for device_id, values in by_device.items():
        base = list(history.get(device_id, []))
        if len(base) < MIN_HISTORY:
            base = base + values
        baselines[device_id] = base if len(base) >= MIN_HISTORY else None

    results = []
    for event_id, device_id, value in batch:
        base = baselines[device_id]
        flagged = base is not None and abs(robust_z(value, base)) > THRESHOLD
        results.append((event_id, bool(flagged)))
    return results


def detector_pass(conn):
    """Score one batch of new rows. Returns (rows_scored, rows_flagged)."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, device_id, reading_value::float8
            FROM stream.fact_event_stream
            WHERE NOT scored
            ORDER BY event_id
            LIMIT %s
            """,
            (DET_BATCH_LIMIT,),
        )
        batch = cur.fetchall()
        if not batch:
            return 0, 0

        first_id = batch[0][0]
        cur.execute(
            """
            SELECT device_id, reading_value FROM (
                SELECT device_id, reading_value::float8 AS reading_value,
                       ROW_NUMBER() OVER (PARTITION BY device_id
                                          ORDER BY event_id DESC) AS rn
                FROM stream.fact_event_stream
                WHERE scored AND NOT is_flagged
                  AND event_id < %s AND event_id >= %s
            ) t
            WHERE rn <= %s
            """,
            (first_id, first_id - LOOKBACK_ROWS, HISTORY_SIZE),
        )
        history = defaultdict(list)
        for device_id, value in cur.fetchall():
            history[device_id].append(value)

        results = flag_batch(batch, history)
        execute_values(
            cur,
            """
            UPDATE stream.fact_event_stream AS t
            SET is_flagged = v.f, scored = TRUE
            FROM (VALUES %s) AS v(event_id, f)
            WHERE t.event_id = v.event_id
            """,
            results,
            page_size=1000,
        )
    return len(batch), sum(f for _, f in results)


# ---------------------------------------------------------------------------
# Online forecaster
# ---------------------------------------------------------------------------

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
    """One online model per device, plus running error metrics."""

    def __init__(self, model_factory=make_model, rolling_window=ROLLING_WINDOW):
        self.model_factory = model_factory
        self.devices = {}
        self.n = 0
        self.sum_model = 0.0
        self.sum_naive = 0.0
        self.sum_ewm = 0.0
        self.recent = deque(maxlen=rolling_window)

    def step(self, device_id, ts, value):
        """Predict this reading, then learn from it. None while warming up."""
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc)
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
            f"n={self.n} MAE model={self.sum_model / self.n:.3f} "
            f"naive={self.sum_naive / self.n:.3f} ewm={self.sum_ewm / self.n:.3f} | "
            f"last {r}: model={rm:.3f} naive={rn:.3f} ({gain:+.1f}% vs naive)"
        )


def dump_state(forecaster):
    return zlib.compress(pickle.dumps(forecaster, protocol=4))


def load_state_blob(blob):
    return pickle.loads(zlib.decompress(bytes(blob)))


def load_state(conn):
    """Returns (forecaster, last_event_id) or (None, None) if unavailable."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT state, last_event_id FROM stream.ml_state WHERE name = %s",
            (STATE_NAME,),
        )
        row = cur.fetchone()
    if not row:
        return None, None
    try:
        return load_state_blob(row[0]), int(row[1])
    except Exception:
        logging.exception("Saved model state could not be loaded; re-learning")
        return None, None


def warm_start_id(conn):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM stream.fact_event_stream")
        return max(0, cur.fetchone()[0] - WARM_START_ROWS)


def forecaster_pass(conn, forecaster, last_id):
    """Process one batch. Returns (new_last_id, rows_seen, predictions_made)."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, device_id, ts, reading_value::float8
            FROM stream.fact_event_stream
            WHERE scored AND NOT is_flagged AND status <> 'error'
              AND event_id > %s
            ORDER BY event_id
            LIMIT %s
            """,
            (last_id, FC_BATCH_LIMIT),
        )
        rows = cur.fetchall()
        if not rows:
            return last_id, 0, 0

        out = []
        for event_id, device_id, ts, value in rows:
            res = forecaster.step(device_id, ts, value)
            if res is not None:
                out.append((event_id, device_id, ts, res[0], value, res[1]))
        if out:
            execute_values(
                cur,
                """
                INSERT INTO stream.ml_predictions
                    (event_id, device_id, ts, predicted, actual, naive_pred)
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                out,
                page_size=1000,
            )
        new_last = rows[-1][0]
        # Predictions and model state are committed together.
        cur.execute(
            """
            INSERT INTO stream.ml_state (name, state, last_event_id, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (name) DO UPDATE
            SET state = EXCLUDED.state,
                last_event_id = EXCLUDED.last_event_id,
                updated_at = now()
            """,
            (STATE_NAME, psycopg2.Binary(dump_state(forecaster)), new_last),
        )
    return new_last, len(rows), len(out)


# ---------------------------------------------------------------------------
# Entry point used by the timer function
# ---------------------------------------------------------------------------

def run_analysis(conn, budget_s=90):
    start = time.monotonic()
    ensure_schema(conn)

    # 1) Outlier detection. A failure here must not stop the forecaster.
    try:
        total = flagged = 0
        while time.monotonic() - start < budget_s / 2:
            n, f = detector_pass(conn)
            total += n
            flagged += f
            if n < DET_BATCH_LIMIT:
                break
        if total:
            logging.info("Detector scored %d rows, flagged %d", total, flagged)
    except Exception:
        logging.exception("Outlier detection failed")

    # 2) Online forecasting.
    try:
        forecaster, last_id = load_state(conn)
        if forecaster is None:
            forecaster, last_id = OnlineForecaster(), warm_start_id(conn)
            logging.info("Forecaster starting fresh from event_id %d", last_id)
        seen = made = 0
        while time.monotonic() - start < budget_s:
            last_id, n, p = forecaster_pass(conn, forecaster, last_id)
            seen += n
            made += p
            if n < FC_BATCH_LIMIT:
                break
        if seen:
            logging.info("Forecaster saw %d rows, %d predictions | %s",
                         seen, made, forecaster.summary())
    except Exception:
        logging.exception("Online forecasting failed")
