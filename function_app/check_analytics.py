"""Confirms that the Azure analyze function is scoring rows and updating the model."""

import os

import psycopg2
import psycopg2.errors
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    host=os.environ["DATABASE_HOST"], dbname=os.environ["DATABASE_NAME"],
    user=os.environ["DATABASE_USER"], password=os.environ["DATABASE_PASSWORD"],
    port=os.environ.get("DATABASE_PORT", "5432"), sslmode="require", connect_timeout=15,
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE NOT scored),
           COUNT(*) FILTER (WHERE scored AND is_flagged AND is_injected),
           COUNT(*) FILTER (WHERE scored AND is_flagged AND NOT is_injected),
           COUNT(*) FILTER (WHERE scored AND NOT is_flagged AND is_injected),
           EXTRACT(EPOCH FROM (NOW() - MAX(ts)))::int
    FROM stream.fact_event_stream WHERE ts > NOW() - INTERVAL '1 hour'
""")
rows, unscored, tp, fp, fn, age = cur.fetchone()
precision = tp / (tp + fp) if tp + fp else 0
recall = tp / (tp + fn) if tp + fn else 0
print(f"Last hour: {rows} rows, newest {age}s old")
print(f"Detector : {unscored} rows still unscored | precision={precision:.3f} recall={recall:.3f}")

try:
    cur.execute("SELECT COUNT(*), EXTRACT(EPOCH FROM (NOW() - MAX(ts)))::int "
                "FROM stream.ml_predictions WHERE ts > NOW() - INTERVAL '1 hour'")
    n, p_age = cur.fetchone()
    print(f"Forecast : {n} predictions in the last hour, newest {p_age}s old")
    cur.execute("SELECT EXTRACT(EPOCH FROM (NOW() - updated_at))::int, last_event_id "
                "FROM stream.ml_state WHERE name = 'forecaster_v1'")
    row = cur.fetchone()
    if row:
        print(f"Model    : state saved {row[0]}s ago (this table is written only by the Azure function)")
    else:
        print("Model    : no saved state yet (the Azure function has not run the forecaster)")
except psycopg2.errors.UndefinedTable:
    print("Model    : stream.ml_state does not exist yet (the Azure function has not run)")

print()
print("Healthy = unscored stays below ~700, newest row/prediction/state are under ~120s old.")
conn.close()
