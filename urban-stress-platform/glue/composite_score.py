import sys
import pandas as pd
from datetime import date, timedelta

from glue_db import get_connection, upsert  # injected via --extra-py-files

# Optional --TARGET_DATE=YYYY-MM-DD; defaults to yesterday
_argv_map = {}
for _token in sys.argv:
    if _token.startswith("--") and "=" in _token:
        _k, _v = _token[2:].split("=", 1)
        _argv_map[_k] = _v

_target = _argv_map.get("TARGET_DATE")
obs_date = date.fromisoformat(_target) if _target else date.today() - timedelta(days=1)

def run_composite_score(conn, obs_date: date):
    print(f"Running composite score (metric cards) for {obs_date}")

    rows = []

    # 1. 311 open backlog
    df = pd.read_sql("SELECT COUNT(*) as cnt FROM requests_311 WHERE status = 'open'", conn)
    baseline = pd.read_sql(
        "SELECT AVG(value_num) as b FROM signal_metrics WHERE metric_name = 'open_backlog' AND city = 'Boston'", conn
    )
    open_count = int(df["cnt"].values[0])
    b = baseline["b"].values[0]
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "open_backlog", "value_num": open_count,
        "value_text": None,
        "delta_pct": round((open_count - float(b)) / float(b) * 100, 2) if b and not pd.isna(b) else 0.0,
        "baseline": float(b) if b and not pd.isna(b) else open_count,
    })

    # 2. Mean close time (hrs)
    df = pd.read_sql(
        "SELECT AVG(resolution_hrs) as avg_hrs FROM requests_311 WHERE status = 'closed' AND DATE(requested_datetime) = %s",
        conn, params=[str(obs_date)]
    )
    mean_close = round(float(df["avg_hrs"].values[0]), 2) if not pd.isna(df["avg_hrs"].values[0]) else None
    baseline = pd.read_sql(
        "SELECT AVG(value_num) as b FROM signal_metrics WHERE metric_name = 'mean_close_hrs' AND city = 'Boston'", conn
    )
    b = baseline["b"].values[0]
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "mean_close_hrs", "value_num": mean_close,
        "value_text": None,
        "delta_pct": round((mean_close - float(b)) / float(b) * 100, 2) if b and not pd.isna(b) and mean_close else 0.0,
        "baseline": float(b) if b and not pd.isna(b) else mean_close,
    })

    # 3. Top complaint category today
    df = pd.read_sql(
        "SELECT service_name, COUNT(*) as cnt FROM requests_311 WHERE DATE(requested_datetime) = %s GROUP BY service_name ORDER BY cnt DESC LIMIT 1",
        conn, params=[str(obs_date)]
    )
    top_cat = df["service_name"].values[0] if not df.empty else "N/A"
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "top_category", "value_num": None,
        "value_text": top_cat, "delta_pct": None, "baseline": None,
    })

    # 4. TMAX today
    df = pd.read_sql(
        "SELECT tmax FROM weather_daily WHERE obs_date = %s", conn, params=[str(obs_date)]
    )
    tmax = round(float(df["tmax"].values[0]), 1) if not df.empty else None
    baseline = pd.read_sql(
        "SELECT AVG(value_num) as b FROM signal_metrics WHERE metric_name = 'tmax' AND city = 'Boston'", conn
    )
    b = baseline["b"].values[0]
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "tmax", "value_num": tmax,
        "value_text": None,
        "delta_pct": round((tmax - float(b)) / float(b) * 100, 2) if b and not pd.isna(b) and tmax else 0.0,
        "baseline": float(b) if b and not pd.isna(b) else tmax,
    })

    # 5. Heat day streak
    df = pd.read_sql(
        "SELECT obs_date, heat_index_max, tmax FROM weather_daily ORDER BY obs_date DESC", conn
    )
    streak = 0
    for _, row in df.iterrows():
        hi = row["heat_index_max"]
        tx = row["tmax"]
        if (hi is not None and not pd.isna(hi) and float(hi) > 40) or \
           (tx is not None and not pd.isna(tx) and float(tx) > 35):
            streak += 1
        else:
            break
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "heat_streak_days", "value_num": streak,
        "value_text": None, "delta_pct": None, "baseline": None,
    })

    # 6. Media tone 7d avg
    df = pd.read_sql(
        "SELECT avg_tone_7d FROM gdelt_daily WHERE obs_date = %s", conn, params=[str(obs_date)]
    )
    tone = round(float(df["avg_tone_7d"].values[0]), 3) if not df.empty and not pd.isna(df["avg_tone_7d"].values[0]) else None
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "media_tone_7d", "value_num": tone,
        "value_text": None, "delta_pct": None, "baseline": None,
    })

    # 7. Protest mentions
    df = pd.read_sql(
        "SELECT protest_event_count FROM gdelt_daily WHERE obs_date = %s", conn, params=[str(obs_date)]
    )
    protest = int(df["protest_event_count"].values[0]) if not df.empty else 0
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "protest_mentions", "value_num": protest,
        "value_text": None, "delta_pct": None, "baseline": None,
    })

    # 8. Precip last 72 hrs
    df = pd.read_sql(
        "SELECT SUM(precip_6hr_max) as total FROM weather_daily WHERE obs_date >= %s::date - interval '2 days' AND obs_date <= %s",
        conn, params=[str(obs_date), str(obs_date)]
    )
    precip = round(float(df["total"].values[0]), 2) if not df.empty and not pd.isna(df["total"].values[0]) else 0.0
    rows.append({
        "city": "Boston", "metric_date": str(obs_date),
        "metric_name": "precip_72hr_mm", "value_num": precip,
        "value_text": None, "delta_pct": None, "baseline": None,
    })

    upsert(conn, "signal_metrics", rows, ["city", "metric_date", "metric_name"])
    print(f"  Upserted {len(rows)} metric cards to signal_metrics")
    for r in rows:
        val = r["value_text"] or r["value_num"]
        print(f"    {r['metric_name']:<22} {val}")

if __name__ == "__main__":
    conn = get_connection()
    run_composite_score(conn, obs_date)
    conn.close()
    print("Composite score ETL complete.")