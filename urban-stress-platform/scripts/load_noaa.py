"""
load_noaa.py — Fetch NWS weather observations + alerts and load into RDS
Usage: python3 scripts/load_noaa.py [DAYS]
Default: 30 days back from yesterday
"""

import os
import sys
import time
import statistics
import psycopg2
import requests
from datetime import date, timedelta
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────────────
DAYS       = int(sys.argv[1]) if len(sys.argv) > 1 else 30
NWS_BASE   = "https://api.weather.gov"
STATION_ID = "KBOS"
ALERT_AREA = "MA"
HEADERS    = {
    "User-Agent": "urban-stress-platform/1.0 (immanisri.satyasai2001@gmail.com)",
    "Accept":     "application/geo+json",
}

DB_HOST = os.environ.get("DB_HOST", "urbanstressdemo-db5d02a0a9-k590fdsgpdha.c6x2q6oeq58g.us-east-1.rds.amazonaws.com")
DB_PASS = os.environ.get("DB_PASS", "")
DB_USER = "postgres"
DB_NAME = "postgres"

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=5432, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode="require",
        connect_timeout=10,
    )

def upsert(conn, table, rows, conflict_cols):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values = [[r[c] for c in cols] for r in rows]
    conflict = ", ".join(conflict_cols)
    updates  = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(rows)

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_observations(obs: date) -> list:
    url = f"{NWS_BASE}/stations/{STATION_ID}/observations"
    r = requests.get(url, headers=HEADERS, params={
        "start": f"{obs}T00:00:00Z",
        "end":   f"{obs}T23:59:59Z",
        "limit": 500,
    }, timeout=30)
    r.raise_for_status()
    return r.json().get("features", [])

def fetch_alerts(obs: date) -> list:
    url = f"{NWS_BASE}/alerts"
    r = requests.get(url, headers=HEADERS, params={
        "area":  ALERT_AREA,
        "start": f"{obs}T00:00:00Z",
        "end":   f"{obs}T23:59:59Z",
    }, timeout=30)
    r.raise_for_status()
    return r.json().get("features", [])

# ── Parse ─────────────────────────────────────────────────────────────────────
def parse_weather(features: list, obs: date):
    rows = []
    for f in features:
        p = f.get("properties", {})
        def val(field):
            v = p.get(field)
            return v.get("value") if isinstance(v, dict) else None
        rows.append({
            "temp_c":     val("temperature"),
            "heat_index": val("heatIndex"),
            "precip_6hr": val("precipitationLast6Hours"),
            "wind_gust":  val("windGust"),
            "humidity":   val("relativeHumidity"),
            "desc":       p.get("textDescription"),
        })
    rows = [r for r in rows if r["temp_c"] is not None]
    if not rows:
        return None

    def col(k): return [r[k] for r in rows if r[k] is not None]
    def mode_of(lst):
        if not lst: return None
        freq = {}
        for v in lst: freq[v] = freq.get(v, 0) + 1
        return max(freq, key=freq.get)

    temps = col("temp_c")
    return {
        "station_id":       STATION_ID,
        "obs_date":         str(obs),
        "tmax":             max(temps),
        "tmin":             min(temps),
        "heat_index_max":   max(col("heat_index"))   if col("heat_index") else None,
        "precip_6hr_max":   max(col("precip_6hr"))   if col("precip_6hr") else None,
        "wind_gust_max":    max(col("wind_gust"))     if col("wind_gust")  else None,
        "humidity_avg":     statistics.mean(col("humidity")) if col("humidity") else None,
        "text_description": mode_of(col("desc")),
    }

def parse_alerts(features: list, obs: date) -> list:
    rows = []
    for f in features:
        p = f.get("properties", {})
        alert_id = f.get("id")
        if not alert_id:
            continue
        rows.append({
            "alert_id":      alert_id,
            "city":          "Boston",
            "obs_date":      str(obs),
            "event_type":    p.get("event"),
            "severity":      p.get("severity"),
            "urgency":       p.get("urgency"),
            "certainty":     p.get("certainty"),
            "response_type": p.get("response"),
            "category":      p.get("category"),
            "onset":         p.get("onset"),
            "expires":       p.get("expires"),
            "headline":      p.get("headline"),
            "description":   p.get("description"),
            "instruction":   p.get("instruction"),
            "area_desc":     p.get("areaDesc"),
        })
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== Loading NOAA/NWS — last {DAYS} days ===\n")
    conn = get_conn()
    weather_loaded = 0
    alerts_loaded  = 0

    for i in range(DAYS, 0, -1):
        obs = date.today() - timedelta(days=i)
        print(f"[{obs}] Fetching...")
        try:
            obs_features   = fetch_observations(obs)
            weather_row    = parse_weather(obs_features, obs)
            alert_features = fetch_alerts(obs)
            alert_rows     = parse_alerts(alert_features, obs)

            if weather_row:
                upsert(conn, "weather_daily", [weather_row], ["station_id", "obs_date"])
                weather_loaded += 1
                print(f"[{obs}] Weather: tmax={weather_row['tmax']:.1f}°C  "
                      f"alerts={len(alert_rows)}")
            else:
                print(f"[{obs}] No weather observations available")

            if alert_rows:
                upsert(conn, "nws_alerts", alert_rows, ["alert_id"])
                alerts_loaded += len(alert_rows)

        except Exception as e:
            print(f"[{obs}] Skipped — {e}")

        time.sleep(0.5)   # NWS rate limit is generous but be polite

    conn.close()
    print(f"\n=== Done — {weather_loaded} weather days, {alerts_loaded} alerts loaded ===")

if __name__ == "__main__":
    main()
