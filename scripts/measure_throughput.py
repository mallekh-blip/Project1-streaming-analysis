"""
Measures ingestion throughput from the database itself (not from the dashboard).

Part 1: per-minute row counts for the last N hours (complete minutes only):
        average, min, max, standard deviation, minutes with no data (gaps),
        and how many minutes meet the "hundreds per minute" target.
Part 2: a live sample. Reads the highest event_id, waits SAMPLE_SECONDS, then
        counts the rows that arrived in between.

Note: `ts` is the event time the generator stamps on each reading (spread over
the minute before the insert), not the moment the row was committed. Part 1
therefore shows how many readings each minute of event time contains; Part 2 is
the real arrival rate at the database.

Usage:  python measure_throughput.py [hours] [sample_seconds]
        python measure_throughput.py 6 120
Writes docs/throughput_report.md.
"""

import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from dotenv import load_dotenv

load_dotenv()

HOURS = float(sys.argv[1]) if len(sys.argv) > 1 else 6
SAMPLE_SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 120
TARGET = 100  # "hundreds of new records per minute"

PER_MINUTE_SQL = """
SELECT date_trunc('minute', ts) AS minute, COUNT(*)
FROM stream.fact_event_stream
WHERE ts >= date_trunc('minute', NOW() - make_interval(secs => %s)) + INTERVAL '1 minute'
  AND ts <  date_trunc('minute', NOW())
GROUP BY 1 ORDER BY 1
"""


def summarize(counts):
    """counts: list of per-minute row counts, oldest first."""
    n = len(counts)
    if n == 0:
        return None
    return {
        "minutes": n,
        "total": sum(counts),
        "avg": sum(counts) / n,
        "min": min(counts),
        "max": max(counts),
        "stdev": statistics.pstdev(counts) if n > 1 else 0.0,
        "median": statistics.median(counts),
        "meeting_target": sum(c >= TARGET for c in counts),
    }


def find_gaps(minutes):
    """Minutes (datetimes, oldest first) missing between the first and last."""
    if len(minutes) < 2:
        return []
    have = set(minutes)
    gaps = []
    one = timedelta(minutes=1)
    t = minutes[0]
    while t < minutes[-1]:
        if t not in have:
            gaps.append(t)
        t += one
    return gaps


def main():
    conn = psycopg2.connect(
        host=os.environ["DATABASE_HOST"], dbname=os.environ["DATABASE_NAME"],
        user=os.environ["DATABASE_USER"], password=os.environ["DATABASE_PASSWORD"],
        port=os.environ.get("DATABASE_PORT", "5432"), sslmode="require", connect_timeout=15,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # ---- Part 1: per-minute history ----
    cur.execute(PER_MINUTE_SQL, (HOURS * 3600,))
    rows = cur.fetchall()
    minutes = [r[0] for r in rows]
    counts = [r[1] for r in rows]
    s = summarize(counts)
    gaps = find_gaps(minutes)

    cur.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM stream.fact_event_stream")
    total_rows, first_ts, last_ts = cur.fetchone()
    cur.execute("SELECT pg_size_pretty(pg_total_relation_size('stream.fact_event_stream'))")
    table_size = cur.fetchone()[0]

    # ---- Part 2: live sample ----
    cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM stream.fact_event_stream")
    base_id = cur.fetchone()[0]
    print(f"Sampling live arrivals for {SAMPLE_SECONDS}s ...", flush=True)
    t0 = time.monotonic()
    time.sleep(SAMPLE_SECONDS)
    elapsed = time.monotonic() - t0
    cur.execute("SELECT COUNT(*) FROM stream.fact_event_stream WHERE event_id > %s", (base_id,))
    arrived = cur.fetchone()[0]
    live_rate = arrived / elapsed * 60
    conn.close()

    # ---- print ----
    print(f"\nTable: {total_rows:,} rows, {table_size}, from {first_ts:%Y-%m-%d %H:%M} to {last_ts:%Y-%m-%d %H:%M} UTC")
    if s:
        print(f"\nLast {HOURS:g} h, {s['minutes']} complete minutes:")
        print(f"  average {s['avg']:.1f}/min | median {s['median']:.0f} | min {s['min']} | max {s['max']} | stdev {s['stdev']:.1f}")
        print(f"  minutes at or above {TARGET}/min: {s['meeting_target']} of {s['minutes']}")
        print(f"  minutes with no data (gaps): {len(gaps)}")
    else:
        print("\nNo complete minutes in the window.")
    print(f"\nLive sample: {arrived} rows in {elapsed:.0f}s = {live_rate:.0f} rows/min")

    # ---- markdown ----
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out = ["# Ingestion throughput report", "",
           f"Generated {now} by `measure_throughput.py` directly from the database.", "",
           f"- Table `stream.fact_event_stream`: {total_rows:,} rows, {table_size}",
           f"- Data from {first_ts:%Y-%m-%d %H:%M} to {last_ts:%Y-%m-%d %H:%M} UTC", ""]
    if s:
        out += [f"## Per-minute counts, last {HOURS:g} hours ({s['minutes']} complete minutes)", "",
                "| Metric | Value |", "|---|---|",
                f"| Average rows/min | {s['avg']:.1f} |", f"| Median | {s['median']:.0f} |",
                f"| Min / Max | {s['min']} / {s['max']} |", f"| Std. deviation | {s['stdev']:.1f} |",
                f"| Minutes at or above {TARGET}/min | {s['meeting_target']} of {s['minutes']} |",
                f"| Minutes with no data | {len(gaps)} |", ""]
    out += [f"## Live sample", "",
            f"{arrived} rows arrived in {elapsed:.0f} s, which is **{live_rate:.0f} rows/min**.", "",
            "`ts` is the event time stamped by the generator, so the per-minute table counts "
            "readings per minute of event time. The live sample measures actual arrival at the database.", ""]
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "throughput_report.md")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    print(f"\nWritten to {path}")


if __name__ == "__main__":
    main()