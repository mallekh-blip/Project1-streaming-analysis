import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS stream;

CREATE TABLE IF NOT EXISTS stream.dim_device (
    device_id   SERIAL PRIMARY KEY,
    device_code VARCHAR(50) UNIQUE NOT NULL,
    device_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS stream.dim_location (
    location_id SERIAL PRIMARY KEY,
    city        VARCHAR(100) UNIQUE NOT NULL,
    region      VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS stream.fact_event_stream (
    event_id      BIGSERIAL PRIMARY KEY,
    device_id     INT NOT NULL REFERENCES stream.dim_device(device_id),
    location_id   INT NOT NULL REFERENCES stream.dim_location(location_id),
    reading_value NUMERIC NOT NULL,
    status        VARCHAR(20),
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    extra         JSONB,
    is_injected   BOOLEAN DEFAULT FALSE,
    is_flagged    BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_fact_ts ON stream.fact_event_stream (ts);
CREATE INDEX IF NOT EXISTS idx_fact_device ON stream.fact_event_stream (device_id);
"""

conn = psycopg2.connect(
    host=os.environ["DATABASE_HOST"],
    dbname=os.environ["DATABASE_NAME"],
    user=os.environ["DATABASE_USER"],
    password=os.environ["DATABASE_PASSWORD"],
    port=os.environ.get("DATABASE_PORT", "5432"),
    sslmode="require",
)
with conn, conn.cursor() as cur:
    cur.execute(SCHEMA_SQL)
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'stream' ORDER BY table_name;
    """)
    print("Tables created:", [r[0] for r in cur.fetchall()])
conn.close()