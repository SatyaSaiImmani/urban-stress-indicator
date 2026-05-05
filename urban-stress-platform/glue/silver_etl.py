import sys
import pandas as pd
from datetime import date, timedelta

from glue_db import get_connection, upsert  # injected via --extra-py-files

try:
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["TARGET_DATE"])
    obs_date = date.fromisoformat(_args["TARGET_DATE"])
except Exception:
    obs_date = date.today() - timedelta(days=1)

def run_silver(conn, obs_date: date):
    print(f"Running silver ETL for {obs_date}")

    # --- 1. Deduplicate requests_311 ---
    df_311 = pd.read_sql(
        "SELECT * FROM requests_311 WHERE DATE(requested_datetime) = %s",
        conn, params=[str(obs_date)]
    )
    before = len(df_311)
    df_311 = df_311.drop_duplicates(subset=["service_request_id"], keep="last")
    print(f"  requests_311 : {before} rows → {len(df_311)} after dedup")

    # --- 2. Compute resolution_hrs for closed requests ---
    df_311["requested_datetime"] = pd.to_datetime(df_311["requested_datetime"], utc=True)
    df_311["updated_datetime"]   = pd.to_datetime(df_311["updated_datetime"],   utc=True, errors="coerce")
    mask = df_311["status"] == "closed"
    df_311.loc[mask, "resolution_hrs"] = (
        (df_311.loc[mask, "updated_datetime"] - df_311.loc[mask, "requested_datetime"])
        .dt.total_seconds() / 3600
    ).round(2)
    closed_count = mask.sum()
    print(f"  resolution_hrs computed for {closed_count} closed requests")

    # --- 3. Write resolution_hrs back to RDS ---
    for _, row in df_311[mask].iterrows():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE requests_311 SET resolution_hrs = %s WHERE service_request_id = %s",
                (row["resolution_hrs"], row["service_request_id"])
            )
    conn.commit()
    print(f"  Updated resolution_hrs in requests_311")

    # --- 4. Compute avg_tone_7d rolling average for gdelt_daily ---
    df_gdelt = pd.read_sql(
        "SELECT city, obs_date, avg_tone FROM gdelt_daily ORDER BY obs_date",
        conn
    )
    df_gdelt["obs_date"] = pd.to_datetime(df_gdelt["obs_date"])
    df_gdelt = df_gdelt.sort_values("obs_date")
    df_gdelt["avg_tone_7d"] = (
        df_gdelt.groupby("city")["avg_tone"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
        .round(3)
    )

    # Write avg_tone_7d back
    for _, row in df_gdelt.iterrows():
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE gdelt_daily SET avg_tone_7d = %s WHERE city = %s AND obs_date = %s",
                (row["avg_tone_7d"], row["city"], row["obs_date"].date())
            )
    conn.commit()
    print(f"  Updated avg_tone_7d for {len(df_gdelt)} gdelt_daily rows")

    return df_311, df_gdelt

if __name__ == "__main__":
    conn = get_connection()
    df_311, df_gdelt = run_silver(conn, obs_date)
    conn.close()
    print("Silver ETL complete.")