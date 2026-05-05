"""
load_noaa_cdo.py — Fetch historical daily weather from NOAA CDO (NCEI) into RDS
Uses GHCND (Global Historical Climatology Network - Daily) dataset.
Goes back years unlike the NWS observations endpoint (~7 days only).

Usage:
  python3 scripts/load_noaa_cdo.py [DAYS]
  Default: 30 days back from yesterday

Requirements:
  pip install psycopg2-binary requests --break-system-packages

IMPORTANT: You need a free NOAA CDO API token.
  1. Visit https://www.ncdc.noaa.gov/cdo-web/token
  2. Enter your email and they send you a token instantly
  3. Set it: export CDO_TOKEN="your_token_here"
  Or pass it on the command line as the 2nd arg:
  python3 scripts/load_noaa_cdo.py 30 YOUR_TOKEN
"""

import os
import sys
import time
import psycopg2
import requests
from datetime import date, timedelta
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────────────
DAYS      = int(sys.argv[1]) if len(sys.argv) > 1 else 30
CDO_TOKEN = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("CDO_TOKEN", "")

CDO_BASE    = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
STATION_ID  = "GHCND:USW00014739"  # Boston Logan International Airport
DATASET_ID  = "GHCND"
STATION_TAG = "KBOS"

DB_HOST = os.environ.get("DB_HOST", "urbanstressdemo-db5d02a0a9-k590fdsgpdha.c6x2q6oeq58g.us-east-1.rds.amazonaws.com")
DB_PASS = os.environ.get("DB_PASS", "")
DB_USER = "postgres"
DB_NAME = "postgres"

# GHCND data types we care about
# TMAX/TMIN in tenths of degrees C → divide by 10
# PRCP in tenths of mm → divide by 10 to get mm
# AWND in tenths of m/s → divide by 10
# WSF5 = peak wind gust speed (m/s * 10)
# RHAV = average relative humidity (not always available in GHCND)
DATA_TYPES = ["TMAX", "TMIN", "PRCP", "AWND", "WSF5"]

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

# ── Fetch from CDO ────────────────────────────────────────────────────────────
def fetch_day(obs: date) -> dict:
    if not CDO_TOKEN:
        raise ValueError("CDO_TOKEN is required. Get one free at https://www.ncdc.noaa.gov/cdo-web/token")

    params = {
        "datasetid":  DATASET_ID,
        "stationid":  STATION_ID,
        "datatypeid": ",".join(DATA_TYPES),
        "startdate":  str(obs),
        "enddate":    str(obs),
        "units":      "metric",
        "limit":      100,
    }
    headers = {"token": CDO_TOKEN}
    r = requests.get(CDO_BASE, headers=headers, params=params, timeout=30)

    if r.status_code == 429:
        print(f"    Rate limited — sleeping 30s...")
        time.sleep(30)
        r = requests.get(CDO_BASE, headers=headers, params=params, timeout=30)

    r.raise_for_status()
    data = r.json().get("results", [])
    return {item["datatype"]: item["value"] for item in data}

def parse_day(values: dict, obs: date) -> dict:
    """Convert CDO GHCND values to weather_daily row."""
    tmax_raw = values.get("TMAX")
    tmin_raw = values.get("TMIN")

    if tmax_raw is None or tmin_raw is None:
        return None

    # CDO returns metric values directly when units=metric
    # TMAX/TMIN are in Celsius, PRCP in mm, AWND/WSF5 in m/s
    tmax = float(tmax_raw)
    tmin = float(tmin_raw)

    prcp    = float(values["PRCP"])   if "PRCP"  in values else None
    awnd    = float(values["AWND"])   if "AWND"  in values else None
    wsf5    = float(values["WSF5"])   if "WSF5"  in values else None

    wind_gust = wsf5 if wsf5 is not None else awnd

    # Rough text description based on temp and precip
    if prcp and prcp > 5:
        desc = "Rain"
    elif tmax > 32:
        desc = "Hot"
    elif tmax < 0:
        desc = "Freezing"
    else:
        desc = "Clear"

    return {
        "station_id":       STATION_TAG,
        "obs_date":         str(obs),
        "tmax":             tmax,
        "tmin":             tmin,
        "heat_index_max":   None,        # not in GHCND
        "precip_6hr_max":   prcp,        # daily total, close enough
        "wind_gust_max":    wind_gust,
        "humidity_avg":     None,        # not reliably in GHCND
        "text_description": desc,
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not CDO_TOKEN:
        print("ERROR: CDO_TOKEN not set.")
        print("  Get a free token at: https://www.ncdc.noaa.gov/cdo-web/token")
        print("  Then run: export CDO_TOKEN=your_token_here")
        sys.exit(1)

    print(f"=== Loading NOAA CDO GHCND — last {DAYS} days ===")
    print(f"    Station: {STATION_ID} ({STATION_TAG})\n")
    conn = get_conn()
    loaded = 0

    for i in range(DAYS, 0, -1):
        obs = date.today() - timedelta(days=i)
        print(f"[{obs}] Fetching from NCEI CDO...")
        try:
            values = fetch_day(obs)
            if not values:
                print(f"[{obs}] No data returned (GHCND may not have this date yet)")
                continue
            row = parse_day(values, obs)
            if row:
                upsert(conn, "weather_daily", [row], ["station_id", "obs_date"])
                loaded += 1
                print(f"[{obs}] tmax={row['tmax']:.1f}°C  tmin={row['tmin']:.1f}°C  "
                      f"prcp={row['precip_6hr_max']}mm  gust={row['wind_gust_max']} m/s")
            else:
                print(f"[{obs}] Incomplete data (missing TMAX/TMIN)")
        except Exception as e:
            print(f"[{obs}] Skipped — {e}")

        time.sleep(0.4)   # CDO rate limit: ~5 req/sec, 1000 req/day

    conn.close()
    print(f"\n=== Done — {loaded} days loaded ===")

if __name__ == "__main__":
    main()
