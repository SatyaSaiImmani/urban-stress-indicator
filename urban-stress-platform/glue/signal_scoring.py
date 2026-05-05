import sys
import pandas as pd
from datetime import date, timedelta

from glue_db import get_connection, upsert  # injected via --extra-py-files

# Parse TARGET_DATE via getResolvedOptions (official Glue API — handles both
# default job args and per-run override args correctly).
# Falls back to yesterday if not provided (normal daily run).
try:
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["TARGET_DATE"])
    obs_date = date.fromisoformat(_args["TARGET_DATE"])
except BaseException:
    obs_date = date.today() - timedelta(days=1)

SEVERITY  = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1, "Unknown": 0}
URGENCY   = {"Immediate": 3, "Expected": 2, "Future": 1, "Past": 0, "Unknown": 0}
CERTAINTY = {"Observed": 3, "Likely": 2, "Possible": 1, "Unlikely": 0, "Unknown": 0}

WEIGHTS = {"anomaly": 0.35, "heat": 0.30, "alert": 0.20, "context": 0.15}

def signal_311(conn, obs_date: date) -> float:
    df = pd.read_sql(
        "SELECT DATE(requested_datetime) as d, COUNT(*) as cnt FROM requests_311 GROUP BY d ORDER BY d",
        conn
    )
    today = df[df["d"] == str(obs_date)]["cnt"].values
    if len(today) == 0:
        return 50.0
    today_count = float(today[0])
    baseline = df[df["d"] < str(obs_date)].tail(30)["cnt"].mean()
    if pd.isna(baseline) or baseline == 0:
        return 50.0
    anomaly_pct = (today_count - baseline) / baseline
    return round(min(100, max(0, (anomaly_pct * 80) + 50)), 2)

def signal_heat(conn, obs_date: date) -> float:
    df = pd.read_sql(
        "SELECT obs_date, heat_index_max, tmax FROM weather_daily ORDER BY obs_date",
        conn
    )
    today = df[df["obs_date"].astype(str) == str(obs_date)]
    if today.empty:
        return 50.0
    hi = today["heat_index_max"].values[0]
    tmax = today["tmax"].values[0]
    val = hi if hi is not None and not pd.isna(hi) else tmax
    if val is None or pd.isna(val):
        return 50.0
    baseline = df[df["obs_date"].astype(str) < str(obs_date)].tail(30)
    col = "heat_index_max" if hi is not None and not pd.isna(hi) else "tmax"
    mean = baseline[col].mean()
    std  = baseline[col].std()
    if pd.isna(mean) or pd.isna(std) or std == 0:
        return 50.0
    z = (float(val) - float(mean)) / float(std)
    return round(min(100, max(0, (z * 15) + 50)), 2)

def signal_alert(conn, obs_date: date) -> float:
    df = pd.read_sql(
        "SELECT severity, urgency, certainty FROM nws_alerts WHERE obs_date = %s",
        conn, params=[str(obs_date)]
    )
    if df.empty:
        return 0.0
    scores = []
    for _, row in df.iterrows():
        s = SEVERITY.get(row["severity"], 0)
        u = URGENCY.get(row["urgency"], 0)
        c = CERTAINTY.get(row["certainty"], 0)
        scores.append((s * u * c) / 36 * 100)
    return round(max(scores), 2)

def signal_context(conn, obs_date: date) -> float:
    df = pd.read_sql(
        "SELECT avg_tone_7d, protest_event_count FROM gdelt_daily WHERE obs_date = %s",
        conn, params=[str(obs_date)]
    )
    if df.empty:
        return 50.0
    tone    = df["avg_tone_7d"].values[0]
    protest = df["protest_event_count"].values[0]
    if tone is None or pd.isna(tone):
        return 50.0
    tone_score    = min(100, abs(float(tone)) / 8 * 100)
    protest_score = min(100, float(protest) / 20 * 100)
    return round((tone_score * 0.7) + (protest_score * 0.3), 2)

def readiness_mode(composite: float, baseline: float) -> str:
    delta = composite - baseline
    if delta < 10:
        return "Normal"
    elif delta < 25:
        return "Elevated"
    else:
        return "High"

def run_signal_scoring(conn, obs_date: date):
    print(f"Running signal scoring for {obs_date}")

    anomaly_score = signal_311(conn, obs_date)
    heat_score    = signal_heat(conn, obs_date)
    alert_score   = signal_alert(conn, obs_date)
    context_score = signal_context(conn, obs_date)

    composite = round(
        (anomaly_score  * WEIGHTS["anomaly"]) +
        (heat_score     * WEIGHTS["heat"])    +
        (alert_score    * WEIGHTS["alert"])   +
        (context_score  * WEIGHTS["context"]),
        2
    )

    # 30-day baseline composite — only rows BEFORE obs_date to avoid forward-leak
    df_hist = pd.read_sql(
        "SELECT composite FROM signal_scores WHERE city = 'Boston' AND score_date < %s ORDER BY score_date DESC LIMIT 30",
        conn, params=[str(obs_date)]
    )
    baseline_30d = round(float(df_hist["composite"].mean()), 2) if not df_hist.empty else composite

    mode = readiness_mode(composite, baseline_30d)

    row = {
        "city":                   "Boston",
        "score_date":             str(obs_date),
        "anomaly_score":          anomaly_score,
        "heat_score":             heat_score,
        "alert_score":            alert_score,
        "context_score":          context_score,
        "composite":              composite,
        "composite_baseline_30d": baseline_30d,
        "readiness_mode":         mode,
    }

    upsert(conn, "signal_scores", [row], ["city", "score_date"])

    print(f"  anomaly_score  : {anomaly_score}")
    print(f"  heat_score     : {heat_score}")
    print(f"  alert_score    : {alert_score}")
    print(f"  context_score  : {context_score}")
    print(f"  composite      : {composite}")
    print(f"  baseline_30d   : {baseline_30d}")
    print(f"  readiness_mode : {mode}")
    return row

if __name__ == "__main__":
    conn = get_connection()
    run_signal_scoring(conn, obs_date)
    conn.close()
    print("Signal scoring complete.")