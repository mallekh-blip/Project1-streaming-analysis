"""Shared helpers for the outlier-detection and online-ML scripts."""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def connect():
    return psycopg2.connect(
        host=os.environ["DATABASE_HOST"],
        dbname=os.environ["DATABASE_NAME"],
        user=os.environ["DATABASE_USER"],
        password=os.environ["DATABASE_PASSWORD"],
        port=os.environ.get("DATABASE_PORT", "5432"),
        sslmode="require",
        connect_timeout=15,
    )


def ensure_analytics_schema(conn):
    """Add what the analytics scripts need. Safe to run many times."""
    with conn, conn.cursor() as cur:
        # 'scored' marks rows the outlier detector has already looked at.
        cur.execute(
            "ALTER TABLE stream.fact_event_stream "
            "ADD COLUMN IF NOT EXISTS scored BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_unscored "
            "ON stream.fact_event_stream (event_id) WHERE NOT scored"
        )
        # One-step-ahead predictions from the online model.
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
