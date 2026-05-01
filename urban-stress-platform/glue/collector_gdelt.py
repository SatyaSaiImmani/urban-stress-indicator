"""
GDELT Collector — Glue Python Shell job
========================================
Position in pipeline : Last step of Step Functions (runs after composite_score)
Triggered by         : urban-stress-etl-pipeline state machine (daily at 07:00 UTC)

Two operating modes
-------------------
1. Daily mode  (TARGET_DATE supplied):
   Downloads a single GDELT export for the given date.
   Used by the Step Functions pipeline for regular daily runs.

2. Scan mode   (no TARGET_DATE):
   Walks back SCAN_DAYS days (default 30) and downloads every file
   GDELT actually has — typically the last 14–15 days. Null rows are
   written for dates where GDELT has already rotated the file out.
   Used by backfill.sh (single invocation pulls all available history).

Why Glue, not Lambda?
  The gdelt Python package pulls large CSV exports + uses pandas — incompatible
  with Lambda AL2 glibc. Glue Python Shell has pandas pre-installed and no glibc
  constraint.

Data source:
  GDELT 1.0 daily Events export — one file per day, available by ~01:00 UTC.
  URL: http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip
  GDELT rotates files after ~14 days; 404 is handled gracefully.

Glue args (from etl_stack.py glue_args):
  --DB_SECRET_ARN    Secrets Manager secret name/ARN
  --BRONZE_BUCKET    S3 bucket name

Optional args (pass via --arguments on StartJobRun):
  --TARGET_DATE      YYYY-MM-DD  → daily mode (default: scan mode)
  --SCAN_DAYS        int         → how many days back to scan (default: 30)
  --CITY             city name   (default: Boston)
"""

import io
import json
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta

import boto3
import pandas as pd
import psycopg2

# ── Glue args ────────────────────────────────────────────────────────────────
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["DB_SECRET_ARN", "BRONZE_BUCKET"])
DB_SECRET_ARN = args["DB_SECRET_ARN"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]

_argv_map = {}
for token in sys.argv:
    if token.startswith("--") and "=" in token:
        k, v = token[2:].split("=", 1)
        _argv_map[k] = v

TARGET_DATE = _argv_map.get("TARGET_DATE")   # None → scan mode
SCAN_DAYS   = int(_argv_map.get("SCAN_DAYS", "30"))
CITY        = _argv_map.get("CITY", "Boston")

SCAN_MODE = TARGET_DATE is None
print(f"[collector_gdelt] mode={'scan' if SCAN_MODE else 'daily'}  "
      f"target={TARGET_DATE or f'last {SCAN_DAYS} days'}  city={CITY}")

# ── Column names for GDELT 1.0 (58 cols, tab-separated, no header) ───────────
GDELT_COLS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
s3_client = boto3.client("s3")

def _get_secret():
    sm = boto3.client("secretsmanager")
    return json.loads(sm.get_secret_value(SecretId=DB_SECRET_ARN)["SecretString"])

def _get_conn(secret):
    return psycopg2.connect(
        host=secret["host"],
        port=int(secret.get("port", 5432)),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        connect_timeout=10,
        sslmode="require",
    )

def _mean(s):
    v = s.dropna()
    return round(float(v.mean()), 3) if len(v) else None

def _sum(s):
    v = s.dropna()
    return int(v.sum()) if len(v) else 0

UPSERT_SQL = """
    INSERT INTO gdelt_daily
        (city, obs_date, avg_tone, avg_tone_7d, polarity,
         protest_event_count, protest_mention_sum, goldstein_avg,
         num_articles, source_count)
    VALUES
        (%(city)s, %(obs_date)s, %(avg_tone)s, %(avg_tone_7d)s, %(polarity)s,
         %(protest_event_count)s, %(protest_mention_sum)s, %(goldstein_avg)s,
         %(num_articles)s, %(source_count)s)
    ON CONFLICT (city, obs_date) DO UPDATE SET
        avg_tone            = EXCLUDED.avg_tone,
        protest_event_count = EXCLUDED.protest_event_count,
        protest_mention_sum = EXCLUDED.protest_mention_sum,
        goldstein_avg       = EXCLUDED.goldstein_avg,
        num_articles        = EXCLUDED.num_articles,
        source_count        = EXCLUDED.source_count
"""

NULL_UPSERT_SQL = """
    INSERT INTO gdelt_daily
        (city, obs_date, avg_tone, avg_tone_7d, polarity,
         protest_event_count, protest_mention_sum, goldstein_avg,
         num_articles, source_count)
    VALUES
        (%(city)s, %(obs_date)s, %(avg_tone)s, %(avg_tone_7d)s, %(polarity)s,
         %(protest_event_count)s, %(protest_mention_sum)s, %(goldstein_avg)s,
         %(num_articles)s, %(source_count)s)
    ON CONFLICT (city, obs_date) DO NOTHING
"""

def process_date(target_date_str, conn):
    """
    Download and process one GDELT daily file.
    Returns: 'downloaded' | 'unavailable'
    """
    target     = date.fromisoformat(target_date_str)
    gdelt_date = target.strftime("%Y%m%d")
    date_pfx   = target.strftime("%Y/%m/%d")
    gdelt_url  = f"http://data.gdeltproject.org/events/{gdelt_date}.export.CSV.zip"

    # 1. Attempt download
    zip_bytes = None
    try:
        req = urllib.request.Request(
            gdelt_url, headers={"User-Agent": "urban-stress-platform/1.0"}
        )
        with urllib.request.urlopen(req, timeout=180) as resp:
            zip_bytes = resp.read()
        print(f"  [{target_date_str}] Downloaded {len(zip_bytes)/1_000_000:.1f} MB")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  [{target_date_str}] Not available on GDELT servers (404) — null row")
        else:
            raise

    if zip_bytes is None:
        # Write null sentinel — DO NOTHING if row already exists
        null_row = {
            "city": CITY, "obs_date": target_date_str,
            "avg_tone": None, "avg_tone_7d": None, "polarity": None,
            "protest_event_count": 0, "protest_mention_sum": 0,
            "goldstein_avg": None, "num_articles": 0, "source_count": 0,
        }
        with conn.cursor() as cur:
            cur.execute(NULL_UPSERT_SQL, null_row)
        conn.commit()
        return "unavailable"

    # 2. Parse + filter for city
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            df_raw = pd.read_csv(
                f, sep="\t", header=None, names=GDELT_COLS,
                dtype=str, low_memory=False, on_bad_lines="skip",
            )

    boston_mask = (
        df_raw["ActionGeo_FullName"].str.contains(CITY, na=False, case=False) |
        df_raw["Actor1Geo_FullName"].str.contains(CITY, na=False, case=False) |
        df_raw["Actor2Geo_FullName"].str.contains(CITY, na=False, case=False)
    )
    df = df_raw[boston_mask].copy()
    print(f"  [{target_date_str}] {len(df_raw):,} global events → {len(df)} Boston events")

    for col in ["GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["EventRootCode"] = df["EventRootCode"].fillna("").astype(str).str.strip()

    # 3. Write filtered CSV to S3 bronze
    events_key = f"gdelt/{date_pfx}/events.csv"
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=events_key,
        Body=df.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )

    # 4. Aggregate and upsert to gdelt_daily
    protest_df = df[df["EventRootCode"] == "14"]
    row = {
        "city":                CITY,
        "obs_date":            target_date_str,
        "avg_tone":            _mean(df["AvgTone"]),
        "avg_tone_7d":         None,   # computed by silver_etl rolling window
        "polarity":            None,
        "protest_event_count": int(len(protest_df)),
        "protest_mention_sum": _sum(protest_df["NumMentions"]),
        "goldstein_avg":       _mean(df["GoldsteinScale"]),
        "num_articles":        _sum(df["NumArticles"]),
        "source_count":        _sum(df["NumSources"]),
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
    conn.commit()
    print(f"  [{target_date_str}] protests={row['protest_event_count']}  tone={row['avg_tone']}  upserted ✓")
    return "downloaded"

# ── Build date list ───────────────────────────────────────────────────────────
if SCAN_MODE:
    # Walk back SCAN_DAYS days; today's file isn't published yet so start from yesterday
    dates = [
        str(date.today() - timedelta(days=i))
        for i in range(1, SCAN_DAYS + 1)
    ]
else:
    dates = [TARGET_DATE]

# ── Connect once, process all dates ──────────────────────────────────────────
secret = _get_secret()
conn   = _get_conn(secret)

downloaded  = 0
unavailable = 0

for d in dates:
    result = process_date(d, conn)
    if result == "downloaded":
        downloaded += 1
    else:
        unavailable += 1

conn.close()

print(
    f"[collector_gdelt] Done — "
    f"downloaded={downloaded}  unavailable(null rows)={unavailable}"
)
