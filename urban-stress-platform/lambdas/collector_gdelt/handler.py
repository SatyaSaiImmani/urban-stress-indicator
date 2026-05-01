import os
import pandas as pd
from datetime import date, timedelta

from s3 import write_bronze
from db import get_connection, upsert

CITY_FILTER = "Boston, Massachusetts, United States"
CITY_NAME   = "Boston"

def patch_gdelt():
    """Patch gdelt import to avoid GitHub fetch in restricted environments."""
    import gdelt.base as _gb
    codes_path = os.path.join(os.path.dirname(__file__), "cameoCodes.json")
    if os.path.exists(codes_path):
        _gb.codes = pd.read_json(
            codes_path,
            dtype={"cameoCode": "str", "GoldsteinScale": float},
            precise_float=True,
            convert_dates=False,
        )
        _gb.codes.set_index("cameoCode", drop=False, inplace=True)

def fetch_events(obs_date: date) -> pd.DataFrame:
    patch_gdelt()
    import gdelt
    gd = gdelt.gdelt(version=2)
    date_str = obs_date.strftime("%Y %b %d")
    df = gd.Search(date=[date_str], table="events", coverage=True, output=None)
    if df is None or df.empty:
        return pd.DataFrame()
    return df[df["ActionGeo_FullName"].str.contains("Boston, Massachusetts", na=False)]

def aggregate_events(df: pd.DataFrame, obs_date: date) -> dict | None:
    if df.empty:
        return None
    protest = df[df["EventRootCode"] == "14"]
    return {
        "city":                  CITY_NAME,
        "obs_date":              str(obs_date),
        "avg_tone":              float(df["AvgTone"].mean()),
        "avg_tone_7d":           None,   # computed in Glue silver_etl rolling window
        "polarity":              None,   # from gkg table — Glue only
        "protest_event_count":   int(len(protest)),
        "protest_mention_sum":   int(protest["NumMentions"].sum()) if not protest.empty else 0,
        "goldstein_avg":         float(df["GoldsteinScale"].mean()),
        "num_articles":          int(df["NumArticles"].sum()),
        "source_count":          int(df["SOURCEURL"].nunique()),
    }

def handler(event=None, context=None):
    obs_date = date.today() - timedelta(days=1)
    print(f"Fetching GDELT events for {obs_date}")

    df = fetch_events(obs_date)
    print(f"  Fetched {len(df)} Boston events")

    if os.environ.get("SKIP_S3") != "true":
        write_bronze("gdelt", df.to_dict(orient="records"), obs_date)
        print(f"  Written to S3 bronze")

    if os.environ.get("SKIP_DB") != "true":
        row = aggregate_events(df, obs_date)
        if row:
            conn = get_connection()
            upsert(conn, "gdelt_daily", [row], ["city", "obs_date"])
            conn.close()
            print(f"  Upserted 1 row to gdelt_daily")
        else:
            print(f"  No Boston events found for {obs_date}")

    return {"status": "ok", "date": str(obs_date), "boston_events": len(df)}

if __name__ == "__main__":
    result = handler()
    print(result)