"""
Real-time dashboard for the Project 1 streaming analytics pipeline.

Reads from Azure PostgreSQL:
  stream.fact_event_stream  - sensor readings (written by the Azure Function)
  is_flagged / scored       - outlier detector output (analytics/outlier_detection.py)
  stream.ml_predictions     - online model output     (analytics/online_ml.py)

Settings come from environment variables (never hardcoded):
  DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_PORT
Locally they are read from the project's .env file; on Azure App Service they
come from the app's Environment variables.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.errors
import streamlit as st

try:  # only needed when running on your own computer
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except ImportError:
    pass

st.set_page_config(page_title="IoT Streaming Analytics", page_icon="📡", layout="wide")

REQUIRED_SETTINGS = [
    "DATABASE_HOST", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD",
]
missing = [k for k in REQUIRED_SETTINGS if not os.environ.get(k)]
if missing:
    st.error("Missing settings: " + ", ".join(missing)
             + ". Add them to .env (local) or the App Service environment variables.")
    st.stop()

WINDOWS = {"5 minutes": 5, "15 minutes": 15, "30 minutes": 30,
           "1 hour": 60, "6 hours": 360}


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

@st.cache_resource
def get_conn():
    conn = psycopg2.connect(
        host=os.environ["DATABASE_HOST"],
        dbname=os.environ.get("DATABASE_NAME", "stream"),
        user=os.environ["DATABASE_USER"],
        password=os.environ["DATABASE_PASSWORD"],
        port=os.environ.get("DATABASE_PORT", "5432"),
        sslmode="require",
        connect_timeout=10,
    )
    conn.autocommit = True  # read-only dashboard: never leave a transaction open
    return conn


def query(sql, params=None):
    """Run a query and return a DataFrame. Reconnects once if the link dropped."""
    for attempt in (1, 2):
        try:
            with get_conn().cursor() as cur:
                cur.execute(sql, params)
                cols = [c[0] for c in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            get_conn.clear()
            if attempt == 2:
                raise


def optional_query(sql, params=None):
    """Like query(), but returns None if the ML table does not exist yet."""
    try:
        return query(sql, params)
    except psycopg2.errors.UndefinedTable:
        return None


@st.cache_data(ttl=300)
def load_devices():
    return query(
        "SELECT device_id, device_code, device_type "
        "FROM stream.dim_device ORDER BY device_code"
    )


KPI_SQL = """
SELECT
    COUNT(*)                                                          AS events,
    COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '1 minute')          AS last_minute,
    COUNT(DISTINCT device_id)
          FILTER (WHERE ts > NOW() - INTERVAL '2 minutes')            AS active_devices,
    COUNT(*) FILTER (WHERE status = 'alert')                          AS alerts,
    COUNT(*) FILTER (WHERE is_flagged)                                AS flagged,
    COUNT(*) FILTER (WHERE scored AND is_flagged AND is_injected)     AS tp,
    COUNT(*) FILTER (WHERE scored AND is_flagged AND NOT is_injected) AS fp,
    COUNT(*) FILTER (WHERE scored AND NOT is_flagged AND is_injected) AS fn,
    MAX(ts)                                                           AS latest
FROM stream.fact_event_stream
WHERE ts > NOW() - make_interval(mins => %s)
"""

DEVICE_SQL = """
SELECT f.ts, f.reading_value::float8 AS value, f.status, f.is_flagged,
       p.predicted
FROM stream.fact_event_stream f
LEFT JOIN stream.ml_predictions p ON p.event_id = f.event_id
WHERE f.device_id = %s AND f.ts > NOW() - make_interval(mins => %s)
ORDER BY f.ts
"""

DEVICE_SQL_NO_ML = """
SELECT f.ts, f.reading_value::float8 AS value, f.status, f.is_flagged,
       NULL::float8 AS predicted
FROM stream.fact_event_stream f
WHERE f.device_id = %s AND f.ts > NOW() - make_interval(mins => %s)
ORDER BY f.ts
"""

BY_TYPE_SQL = """
SELECT date_trunc('minute', f.ts) AS minute, d.device_type,
       AVG(f.reading_value::float8) FILTER (WHERE NOT f.is_flagged) AS avg_value
FROM stream.fact_event_stream f
JOIN stream.dim_device d ON d.device_id = f.device_id
WHERE f.ts > NOW() - make_interval(mins => %s)
GROUP BY 1, 2
ORDER BY 1
"""

OUTLIERS_SQL = """
SELECT date_trunc('minute', ts) AS minute,
       COUNT(*) FILTER (WHERE is_flagged)  AS flagged,
       COUNT(*) FILTER (WHERE is_injected) AS injected
FROM stream.fact_event_stream
WHERE ts > NOW() - make_interval(mins => %s)
GROUP BY 1
ORDER BY 1
"""

ML_SQL = """
SELECT date_trunc('minute', ts) AS minute,
       AVG(ABS(actual - predicted))  AS model_mae,
       AVG(ABS(actual - naive_pred)) AS naive_mae,
       COUNT(*)                      AS n
FROM stream.ml_predictions
WHERE ts > NOW() - make_interval(mins => %s)
GROUP BY 1
ORDER BY 1
"""

THROUGHPUT_SQL = """
SELECT date_trunc('minute', ts) AS minute, COUNT(*) AS rows_inserted
FROM stream.fact_event_stream
WHERE ts >= date_trunc('minute', NOW() - make_interval(mins => %s)) + INTERVAL '1 minute'
  AND ts <  date_trunc('minute', NOW())
GROUP BY 1
ORDER BY 1
"""

RECENT_FLAGGED_SQL = """
SELECT f.ts, d.device_code, d.device_type, l.city,
       f.reading_value::float8 AS reading, f.status,
       f.is_injected AS truly_injected
FROM stream.fact_event_stream f
JOIN stream.dim_device d   ON d.device_id   = f.device_id
JOIN stream.dim_location l ON l.location_id = f.location_id
WHERE f.is_flagged AND f.ts > NOW() - make_interval(mins => %s)
ORDER BY f.ts DESC
LIMIT 15
"""


# ---------------------------------------------------------------------------
# Small helpers and charts
# ---------------------------------------------------------------------------

def ratio(num, den):
    return num / den if den else None


def pct(x):
    return "n/a" if x is None else f"{x:.1%}"


def device_chart(df):
    """Readings of one device, with flagged outliers and the model's predictions."""
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["predicted"] = pd.to_numeric(df["predicted"], errors="coerce")
    df["is_flagged"] = df["is_flagged"].fillna(False).astype(bool)

    # Sensor faults report an impossible value (-999); they would flatten the
    # chart, so they are counted but not plotted.
    faults = int((df["status"] == "error").sum())
    df = df[df["status"] != "error"]
    normal = df[~df["is_flagged"]]
    flagged = df[df["is_flagged"]]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=normal["ts"], y=normal["value"], mode="lines",
                             name="Reading"))
    if normal["predicted"].notna().any():
        fig.add_trace(go.Scatter(x=normal["ts"], y=normal["predicted"], mode="lines",
                                 name="Model prediction", line=dict(dash="dot")))
    if not flagged.empty:
        fig.add_trace(go.Scatter(x=flagged["ts"], y=flagged["value"], mode="markers",
                                 name="Flagged outlier",
                                 marker=dict(color="red", size=10, symbol="x")))
    fig.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10),
                      legend=dict(orientation="h", y=1.12),
                      xaxis_title="Time (UTC)", yaxis_title="Reading",
                      hovermode="x unified")
    return fig, faults


def by_type_chart(df):
    df = df.copy()
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    df["avg_value"] = pd.to_numeric(df["avg_value"], errors="coerce")
    fig = px.line(df, x="minute", y="avg_value", facet_row="device_type", height=520,
                  labels={"minute": "Time (UTC)", "avg_value": ""})
    fig.update_yaxes(matches=None, showticklabels=True, title_text="")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(margin=dict(l=10, r=10, t=30, b=10))
    return fig


def outliers_chart(df):
    df = df.copy()
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["minute"], y=df["flagged"], name="Flagged by detector"))
    fig.add_trace(go.Bar(x=df["minute"], y=df["injected"],
                         name="Actually injected (ground truth)"))
    fig.update_layout(barmode="group", height=520, margin=dict(l=10, r=10, t=30, b=10),
                      legend=dict(orientation="h", y=1.1),
                      xaxis_title="Time (UTC)", yaxis_title="Readings per minute")
    return fig


def ml_chart(df):
    df = df.copy()
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["minute"], y=df["model_mae"], mode="lines",
                             name="Online model"))
    fig.add_trace(go.Scatter(x=df["minute"], y=df["naive_mae"], mode="lines",
                             name="Naive (same as last reading)",
                             line=dict(dash="dot")))
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=30, b=10),
                      legend=dict(orientation="h", y=1.15),
                      xaxis_title="Time (UTC)",
                      yaxis_title="Mean absolute error per minute")
    return fig


def throughput_chart(df):
    """Rows per minute (complete minutes only) against the 'hundreds' requirement."""
    df = df.copy()
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["minute"], y=df["rows_inserted"], name="Rows per minute"))
    fig.add_hline(y=100, line_dash="dash", line_color="red",
                  annotation_text="100/min: minimum for 'hundreds per minute'",
                  annotation_position="top left")
    fig.update_layout(height=340, margin=dict(l=10, r=10, t=30, b=10),
                      showlegend=False, xaxis_title="Time (UTC)",
                      yaxis_title="Records inserted per minute")
    fig.update_yaxes(rangemode="tozero")
    return fig


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

st.title("📡 Streaming Analytics Dashboard")
st.caption("IoT sensor stream on Azure PostgreSQL: live KPIs, outlier detection and "
           "online machine learning")

st.sidebar.header("Settings")
window_label = st.sidebar.selectbox("Time window", list(WINDOWS), index=1)
window_min = WINDOWS[window_label]

try:
    devices = load_devices()
except Exception as exc:
    st.error(f"Could not read from the database: {exc}")
    st.caption("If this is a timeout, add your current IP address in the PostgreSQL "
               "server's Networking firewall rules.")
    st.stop()
if devices.empty:
    st.warning("No devices yet. Is the Azure Function running?")
    st.stop()

device_names = {int(r.device_id): f"{r.device_code} ({r.device_type})"
                for r in devices.itertuples()}
device_id = st.sidebar.selectbox("Device (for the detail chart)",
                                 options=list(device_names),
                                 format_func=lambda i: device_names[i])

auto = st.sidebar.checkbox("Auto-refresh", value=True)
every = st.sidebar.select_slider("Refresh every (seconds)", options=[10, 30, 60],
                                 value=30)


def render():
    try:
        k = query(KPI_SQL, (window_min,)).iloc[0]
    except psycopg2.errors.UndefinedColumn:
        st.error("The detector's columns are missing. Run "
                 "`python analytics\\outlier_detection.py` once, then reload.")
        return
    except Exception as exc:
        st.error(f"Could not read from the database: {exc}")
        return

    events = int(k["events"])
    if events == 0:
        st.warning("No data in this time window. Check that the Azure Function is running.")
        return

    tp, fp, fn = int(k["tp"]), int(k["fp"]), int(k["fn"])
    precision, recall = ratio(tp, tp + fp), ratio(tp, tp + fn)
    f1 = (ratio(2 * precision * recall, precision + recall)
          if precision and recall else None)

    ml = optional_query(ML_SQL, (window_min,))
    model_mae = naive_mae = gain = None
    if ml is not None and not ml.empty and ml["n"].sum() > 0:
        weights = ml["n"].astype(float)
        model_mae = float((ml["model_mae"].astype(float) * weights).sum() / weights.sum())
        naive_mae = float((ml["naive_mae"].astype(float) * weights).sum() / weights.sum())
        gain = ratio(naive_mae - model_mae, naive_mae)

    # --- KPIs ---------------------------------------------------------------
    c = st.columns(5)
    c[0].metric("Events in window", f"{events:,}")
    c[1].metric("Events in last minute", f"{int(k['last_minute']):,}")
    c[2].metric("Active devices (2 min)", int(k["active_devices"]))
    c[3].metric("Rule-based alerts", f"{int(k['alerts']):,}",
                help="Readings outside the fixed thresholds set in the generator")
    c[4].metric("Outliers flagged", f"{int(k['flagged']):,}",
                help="Flagged by the robust z-score detector")

    c = st.columns(5)
    c[0].metric("Detector precision", pct(precision),
                help="Of the readings flagged, the share that really were injected")
    c[1].metric("Detector recall", pct(recall),
                help="Of the injected anomalies, the share that were caught")
    c[2].metric("Detector F1", pct(f1))
    c[3].metric("Model error (MAE)", "n/a" if model_mae is None else f"{model_mae:.3f}",
                help="Mean absolute error of the online model's one-step-ahead forecast")
    c[4].metric("Model vs naive", "n/a" if gain is None else f"{gain:+.1%}",
                help="How much lower the model's error is than 'same as last reading'")

    st.divider()

    # --- Device detail ------------------------------------------------------
    st.subheader(f"Live readings: {device_names[device_id]}")
    try:
        dev_df = query(DEVICE_SQL, (device_id, window_min))
    except psycopg2.errors.UndefinedTable:
        dev_df = query(DEVICE_SQL_NO_ML, (device_id, window_min))
    if dev_df.empty:
        st.info("No readings for this device in the selected window.")
    else:
        fig, faults = device_chart(dev_df)
        st.plotly_chart(fig)
        if faults:
            st.caption(f"{faults} sensor-fault reading(s) (status 'error', value -999) "
                       "are not plotted.")

    # --- Aggregates and outliers ---------------------------------------------
    left, right = st.columns(2)
    with left:
        st.subheader("Average reading per minute, by sensor type")
        by_type = query(BY_TYPE_SQL, (window_min,))
        if not by_type.empty:
            st.plotly_chart(by_type_chart(by_type))
        st.caption("Flagged outliers are excluded from these averages.")
    with right:
        st.subheader("Detected vs injected anomalies per minute")
        outliers = query(OUTLIERS_SQL, (window_min,))
        if not outliers.empty:
            st.plotly_chart(outliers_chart(outliers))
        st.caption("The newest minute can show fewer flagged than injected until the "
                   "detector's next pass.")

    # --- Online ML and ingestion throughput ----------------------------------
    left, right = st.columns(2)
    with left:
        st.subheader("Online model error vs naive baseline")
        if ml is None or ml.empty:
            st.info("No predictions yet. Run `python analytics\\online_ml.py`.")
        else:
            st.plotly_chart(ml_chart(ml))
    with right:
        st.subheader("Ingestion throughput")
        thr = query(THROUGHPUT_SQL, (window_min,))
        if thr.empty:
            st.info("Not enough complete minutes in this window yet.")
        else:
            st.plotly_chart(throughput_chart(thr))
            st.caption(f"Average over complete minutes: "
                       f"{thr['rows_inserted'].astype(float).mean():,.0f} records per minute.")

    # --- Recent outliers -----------------------------------------------------
    st.subheader("Most recent flagged outliers")
    recent = query(RECENT_FLAGGED_SQL, (window_min,))
    if recent.empty:
        st.success("No outliers flagged in this window.")
    else:
        recent["ts"] = pd.to_datetime(recent["ts"], utc=True).dt.strftime("%H:%M:%S")
        st.dataframe(recent, hide_index=True)

    st.caption(f"Last refreshed {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC | "
               f"latest reading {pd.to_datetime(k['latest'], utc=True):%H:%M:%S} UTC")


@st.fragment(run_every=every if auto else None)
def live_view():
    render()


live_view()
