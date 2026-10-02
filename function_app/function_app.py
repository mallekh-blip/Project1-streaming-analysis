"""
Azure Function: IoT sensor data generator for Project 1.

Every 30 seconds it inserts 160 readings (20 devices x 8 readings) into
stream.fact_event_stream, i.e. ~320 records per minute.

Each device has a FIXED type and city, and its readings follow a realistic
pattern (city baseline + daily cycle + noise). About 2% of readings are
deliberately corrupted (spike, drop, or sensor fault) and marked with
is_injected = TRUE, so the outlier detector can be evaluated against them.
"""

import logging
import math
import os
import random
from datetime import datetime, timedelta, timezone

import azure.functions as func
import psycopg2
from psycopg2.extras import Json, execute_values

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

READINGS_PER_DEVICE = 8      # per run -> 20 devices x 8 = 160 rows per run
WINDOW_SECONDS = 30          # readings are spread across the 30 s between runs
ANOMALY_RATE = 0.02          # ~2% of readings are injected anomalies

DEVICE_TYPES = ["thermometer", "barometer", "hygrometer", "anemometer"]
CITY_TO_REGION = {
    "New York": "Northeast",
    "Los Angeles": "West",
    "Chicago": "Midwest",
    "Houston": "South",
    "Phoenix": "Southwest",
}
CITIES = list(CITY_TO_REGION)

# 20 devices: every city gets one sensor of each type. Fixed for all time.
DEVICES = [
    {"code": f"DEVICE_{i + 1:03d}", "type": DEVICE_TYPES[i % 4], "city": CITIES[i // 4]}
    for i in range(20)
]

# Typical value for each sensor type in each city
CITY_BASE = {
    "thermometer": {"New York": 18, "Los Angeles": 22, "Chicago": 15, "Houston": 27, "Phoenix": 32},  # deg C
    "barometer":   {"New York": 1015, "Los Angeles": 1013, "Chicago": 1016, "Houston": 1012, "Phoenix": 1010},  # hPa
    "hygrometer":  {"New York": 60, "Los Angeles": 55, "Chicago": 65, "Houston": 75, "Phoenix": 25},  # % RH
    "anemometer":  {"New York": 15, "Los Angeles": 10, "Chicago": 20, "Houston": 12, "Phoenix": 9},  # km/h
}

# amp = size of the daily cycle (negative = moves opposite to temperature)
# noise = standard deviation of random noise
TYPE_PARAMS = {
    "thermometer": {"amp": 5.0, "noise": 0.6},
    "barometer":   {"amp": 2.0, "noise": 0.8},
    "hygrometer":  {"amp": -10.0, "noise": 2.0},
    "anemometer":  {"amp": 4.0, "noise": 2.5},
}

# Domain alert thresholds (business rules, separate from anomaly detection)
ALERT_RULES = {
    "thermometer": lambda v: v > 35 or v < 0,
    "barometer":   lambda v: v < 990 or v > 1030,
    "hygrometer":  lambda v: v > 90 or v < 15,
    "anemometer":  lambda v: v > 40,
}


def get_db_config():
    """Read connection settings from the Function App's Application Settings."""
    return {
        "host": os.environ["DATABASE_HOST"],
        "dbname": os.environ["DATABASE_NAME"],
        "user": os.environ["DATABASE_USER"],
        "password": os.environ["DATABASE_PASSWORD"],
        "port": os.environ.get("DATABASE_PORT", "5432"),
        "sslmode": "require",
        "connect_timeout": 10,
    }


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def normal_value(device, ts):
    """Realistic reading: city baseline + daily cycle (peak ~3 pm) + noise."""
    params = TYPE_PARAMS[device["type"]]
    base = CITY_BASE[device["type"]][device["city"]]
    hour = ts.hour + ts.minute / 60
    daily = math.sin(2 * math.pi * (hour - 9) / 24)
    value = base + params["amp"] * daily + random.gauss(0, params["noise"])

    if device["type"] == "anemometer":
        value = max(0.0, value)
    elif device["type"] == "hygrometer":
        value = min(100.0, max(0.0, value))
    return value


def inject_anomaly(device, value):
    """Corrupt a reading. Returns (new_value, anomaly_kind)."""
    noise = TYPE_PARAMS[device["type"]]["noise"]
    kind = random.choice(["spike", "drop", "sensor_fault"])
    if kind == "spike":
        value += noise * random.uniform(8, 15)
    elif kind == "drop":
        value -= noise * random.uniform(8, 15)
    else:  # sensor_fault: an impossible error value
        value = -999.0
    return value, kind


def generate_batch(now):
    """Generate READINGS_PER_DEVICE readings per device across the last window."""
    readings = []
    step = WINDOW_SECONDS / READINGS_PER_DEVICE
    for device in DEVICES:
        for k in range(READINGS_PER_DEVICE):
            ts = now - timedelta(seconds=WINDOW_SECONDS) + timedelta(seconds=k * step)
            value = normal_value(device, ts)
            is_injected, kind = False, None

            if random.random() < ANOMALY_RATE:
                value, kind = inject_anomaly(device, value)
                is_injected = True

            if kind == "sensor_fault":
                status = "error"
            elif ALERT_RULES[device["type"]](value):
                status = "alert"
            else:
                status = "normal"

            readings.append({
                "device": device,
                "ts": ts,
                "value": round(value, 2),
                "status": status,
                "is_injected": is_injected,
                "extra": {
                    "device_type": device["type"],
                    "city": device["city"],
                    "battery_level": random.randint(20, 100),
                    "anomaly_kind": kind,
                },
            })
    return readings


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def ensure_dimensions(cur):
    """Insert devices/locations if missing, then return code->id lookups."""
    execute_values(
        cur,
        "INSERT INTO stream.dim_device (device_code, device_type) VALUES %s "
        "ON CONFLICT (device_code) DO NOTHING",
        [(d["code"], d["type"]) for d in DEVICES],
    )
    execute_values(
        cur,
        "INSERT INTO stream.dim_location (city, region) VALUES %s "
        "ON CONFLICT (city) DO NOTHING",
        list(CITY_TO_REGION.items()),
    )
    cur.execute("SELECT device_code, device_id FROM stream.dim_device")
    device_ids = dict(cur.fetchall())
    cur.execute("SELECT city, location_id FROM stream.dim_location")
    location_ids = dict(cur.fetchall())
    return device_ids, location_ids


# ---------------------------------------------------------------------------
# Timer trigger: runs every 30 seconds
# ---------------------------------------------------------------------------

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer",
                   run_on_startup=False, use_monitor=False)
def generatedata(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("Timer is past due")

    now = datetime.now(timezone.utc)
    readings = generate_batch(now)

    conn = psycopg2.connect(**get_db_config())
    try:
        with conn, conn.cursor() as cur:
            device_ids, location_ids = ensure_dimensions(cur)
            rows = [
                (
                    device_ids[r["device"]["code"]],
                    location_ids[r["device"]["city"]],
                    r["value"],
                    r["status"],
                    r["ts"],
                    Json(r["extra"]),
                    r["is_injected"],
                )
                for r in readings
            ]
            execute_values(
                cur,
                "INSERT INTO stream.fact_event_stream "
                "(device_id, location_id, reading_value, status, ts, extra, is_injected) "
                "VALUES %s",
                rows,
                page_size=500,
            )
        injected = sum(r["is_injected"] for r in readings)
        logging.info("Inserted %d readings (%d injected anomalies)", len(rows), injected)
    finally:
        conn.close()