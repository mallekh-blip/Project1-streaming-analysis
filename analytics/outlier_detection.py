"""
Outlier detection on the incoming sensor stream.

Method: robust z-score per device (median + MAD), also called the modified
z-score (Iglewicz & Hoaglin). For each new reading we compare it with the
recent readings of the SAME device:

    z = 0.6745 * (x - median) / MAD          flag if |z| > THRESHOLD (3.5)

Median and MAD are not distorted by the outliers themselves, unlike the
mean and standard deviation, which makes this a good fit for a stream
where errors and spikes arrive at random.

How it works:
  * rows with scored = FALSE are new; they are scored in event_id order
  * the baseline for each device is its last HISTORY_SIZE readings that were
    scored earlier and NOT flagged (so anomalies never pollute the baseline)
  * results go to is_flagged; scored is set to TRUE
  * after each pass it prints precision / recall against is_injected, the
    ground truth the data generator wrote (the detector never reads it)

Usage:
    python outlier_detection.py           # runs forever, a pass every 30 s
    python outlier_detection.py --once    # a single pass, then exit
"""

import argparse
import statistics
import time
from collections import defaultdict

from psycopg2.extras import execute_values

from common import connect, ensure_analytics_schema

THRESHOLD = 3.5        # |robust z| above this = outlier
HISTORY_SIZE = 300     # baseline readings per device (~19 min of data)
MIN_HISTORY = 20       # below this, bootstrap the baseline from the batch too
BATCH_LIMIT = 1000     # max rows scored per pass
LOOKBACK_ROWS = 50000  # only search this many recent event_ids for history
SLEEP_SECONDS = 30
MAD_FLOOR = 1e-6       # avoids division by zero when MAD == 0


# ---------------------------------------------------------------------------
# Pure logic (no database) - easy to test
# ---------------------------------------------------------------------------

def robust_z(value, baseline):
    """Modified z-score of value against a list of baseline readings."""
    med = statistics.median(baseline)
    mad = statistics.median(abs(b - med) for b in baseline)
    return 0.6745 * (value - med) / max(mad, MAD_FLOOR)


def flag_batch(batch, history):
    """
    batch:   list of (event_id, device_id, value)
    history: {device_id: [clean earlier values]}
    returns: list of (event_id, is_flagged)
    """
    by_device = defaultdict(list)
    for _, device_id, value in batch:
        by_device[device_id].append(value)

    baselines = {}
    for device_id, values in by_device.items():
        base = list(history.get(device_id, []))
        if len(base) < MIN_HISTORY:
            # Not enough history yet: borrow the batch itself. The median/MAD
            # are robust to a few outliers, so this still works.
            base = base + values
        baselines[device_id] = base if len(base) >= MIN_HISTORY else None

    results = []
    for event_id, device_id, value in batch:
        base = baselines[device_id]
        flagged = base is not None and abs(robust_z(value, base)) > THRESHOLD
        results.append((event_id, flagged))
    return results


# ---------------------------------------------------------------------------
# Database side
# ---------------------------------------------------------------------------

def fetch_batch(cur):
    cur.execute(
        """
        SELECT event_id, device_id, reading_value::float8
        FROM stream.fact_event_stream
        WHERE NOT scored
        ORDER BY event_id
        LIMIT %s
        """,
        (BATCH_LIMIT,),
    )
    return cur.fetchall()


def fetch_history(cur, first_event_id):
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
        (first_event_id, first_event_id - LOOKBACK_ROWS, HISTORY_SIZE),
    )
    history = defaultdict(list)
    for device_id, value in cur.fetchall():
        history[device_id].append(value)
    return history


def save_results(cur, results):
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


def evaluation(cur):
    cur.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM((is_flagged AND is_injected)::int), 0),
               COALESCE(SUM((is_flagged AND NOT is_injected)::int), 0),
               COALESCE(SUM((NOT is_flagged AND is_injected)::int), 0)
        FROM stream.fact_event_stream WHERE scored
        """
    )
    n, tp, fp, fn = cur.fetchone()
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return n, tp, fp, fn, precision, recall, f1


def one_pass(conn):
    """Score one batch. Returns the number of rows scored."""
    with conn, conn.cursor() as cur:
        batch = fetch_batch(cur)
        if not batch:
            return 0
        history = fetch_history(cur, batch[0][0])
        results = flag_batch(batch, history)
        save_results(cur, results)
        n_flagged = sum(f for _, f in results)
        n, tp, fp, fn, p, r, f1 = evaluation(cur)
    print(
        f"scored {len(batch):5d} rows, flagged {n_flagged:3d} | "
        f"total {n}: TP={tp} FP={fp} FN={fn} | "
        f"precision={p:.3f} recall={r:.3f} F1={f1:.3f}",
        flush=True,
    )
    return len(batch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="single pass then exit")
    args = parser.parse_args()

    conn = connect()
    ensure_analytics_schema(conn)
    print("Outlier detector started (robust z-score, threshold %.1f)" % THRESHOLD)

    while True:
        try:
            # Catch up quickly if there is a backlog, then wait.
            while one_pass(conn) == BATCH_LIMIT:
                pass
        except Exception as exc:  # keep running through network blips
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
