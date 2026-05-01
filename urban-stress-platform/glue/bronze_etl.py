import sys
import pandas as pd
from datetime import date, timedelta

from glue_db import get_connection  # injected via --extra-py-files

# Optional --TARGET_DATE=YYYY-MM-DD; defaults to yesterday
_argv_map = {}
for _token in sys.argv:
    if _token.startswith("--") and "=" in _token:
        _k, _v = _token[2:].split("=", 1)
        _argv_map[_k] = _v

_target = _argv_map.get("TARGET_DATE")
obs_date = date.fromisoformat(_target) if _target else date.today() - timedelta(days=1)

REQUIRED_311 = ["service_request_id", "status", "requested_datetime"]
REQUIRED_WEATHER = ["station_id", "obs_date", "tmax"]
REQUIRED_GDELT = ["city", "obs_date", "avg_tone"]

def validate(df: pd.DataFrame, required: list, table: str) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=required)
    dropped = before - len(df)
    if dropped:
        print(f"  [{table}] Dropped {dropped} rows missing required fields")
    return df

def run_bronze(conn, obs_date: date):
    print(f"Running bronze ETL for {obs_date}")

    # --- requests_311 ---
    df_311 = pd.read_sql(
        "SELECT * FROM requests_311 WHERE DATE(requested_datetime) = %s",
        conn, params=[str(obs_date)]
    )
    df_311 = validate(df_311, REQUIRED_311, "requests_311")
    df_311["requested_datetime"] = pd.to_datetime(df_311["requested_datetime"], utc=True)
    df_311["updated_datetime"]   = pd.to_datetime(df_311["updated_datetime"],   utc=True, errors="coerce")
    df_311["lat"] = pd.to_numeric(df_311["lat"], errors="coerce")
    df_311["lon"] = pd.to_numeric(df_311["lon"], errors="coerce")
    print(f"  requests_311 : {len(df_311)} valid rows")

    # --- weather_daily ---
    df_weather = pd.read_sql(
        "SELECT * FROM weather_daily WHERE obs_date = %s",
        conn, params=[str(obs_date)]
    )
    df_weather = validate(df_weather, REQUIRED_WEATHER, "weather_daily")
    print(f"  weather_daily: {len(df_weather)} valid rows")

    # --- gdelt_daily ---
    df_gdelt = pd.read_sql(
        "SELECT * FROM gdelt_daily WHERE obs_date = %s",
        conn, params=[str(obs_date)]
    )
    df_gdelt = validate(df_gdelt, REQUIRED_GDELT, "gdelt_daily")
    print(f"  gdelt_daily  : {len(df_gdelt)} valid rows")

    return df_311, df_weather, df_gdelt

if __name__ == "__main__":
    conn = get_connection()
    df_311, df_weather, df_gdelt = run_bronze(conn, obs_date)
    conn.close()
    print("Bronze ETL complete.")