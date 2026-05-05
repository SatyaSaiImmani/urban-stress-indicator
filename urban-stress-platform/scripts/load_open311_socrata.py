"""
load_open311_socrata.py — Fetch Boston 311 requests from the Data Hub (Socrata) and load into RDS
Uses data.boston.gov instead of the real-time Open311 API — has years of history available.

Usage:
  python3 scripts/load_open311_socrata.py [DAYS]
  Default: 30 days back from yesterday

Requirements:
  pip install psycopg2-binary requests --break-system-packages

No API key required (but free key from https://dev.socrata.com lets you avoid rate limits).
Set SOCRATA_TOKEN env var if you have one.
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
PAGE_SIZE = 1000   # Socrata supports up to 50000, but 1000 is polite

# Boston 311 dataset on data.boston.gov
# Dataset: https://data.boston.gov/dataset/311-service-requests
SOCRATA_BASE     = "https://data.boston.gov/resource/wc77-mjmu.json"
SOCRATA_TOKEN    = os.environ.get("SOCRATA_TOKEN", "")

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

def upsert(conn, rows: list) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values = [[r[c] for c in cols] for r in rows]
    conflict = "service_request_id"
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict)
    sql = (
        f"INSERT INTO requests_311 ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(rows)

# ── Fetch one day (paginated via Socrata SoQL) ────────────────────────────────
def fetch_day(obs: date) -> list:
    headers = {"Accept": "application/json"}
    if SOCRATA_TOKEN:
        headers["X-App-Token"] = SOCRATA_TOKEN

    start = f"{obs}T00:00:00.000"
    end   = f"{obs}T23:59:59.999"

    all_rows = []
    offset   = 0
    while True:
        params = {
            "$where":  f"open_dt between '{start}' and '{end}'",
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "open_dt ASC",
        }
        try:
            r = requests.get(SOCRATA_BASE, headers=headers, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"    HTTP error at offset {offset}: {e} — stopping")
            break

        if not r.text.strip():
            break
        try:
            batch = r.json()
        except Exception:
            print(f"    Bad JSON at offset {offset}: {r.text[:100]} — stopping")
            break

        if not batch:
            break

        all_rows.extend(batch)
        print(f"    offset={offset}: {len(batch)} records (total: {len(all_rows)})")

        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)

    return all_rows

# ── Parse Socrata fields → requests_311 schema ───────────────────────────────
def parse(raw: list) -> list:
    rows = []
    for r in raw:
        req_id = r.get("case_enquiry_id") or r.get("service_request_id")
        if not req_id:
            continue

        subject    = r.get("subject", "")
        reason     = r.get("reason", "")
        dept       = r.get("department", "")
        case_title = r.get("case_title", "")

        # Reconstruct a service_code similar to Open311 format (dept:reason)
        service_code = f"{dept}:{reason}" if dept and reason else (dept or reason or None)

        rows.append({
            "service_request_id": str(req_id),
            "service_code":       service_code,
            "service_name":       case_title or reason,
            "department":         dept or None,
            "category":           reason or None,
            "status":             r.get("case_status"),
            "requested_datetime": r.get("open_dt"),
            "updated_datetime":   r.get("closed_dt") or r.get("target_dt"),
            "resolution_hrs":     None,
            "lat":                r.get("latitude"),
            "lon":                r.get("longitude"),
            "neighbourhood":      r.get("neighborhood"),
            "description":        r.get("closure_reason") or r.get("case_title"),
        })
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== Loading Boston 311 (Socrata) — last {DAYS} days ===\n")
    conn = get_conn()
    grand_total = 0

    for i in range(DAYS, 0, -1):
        obs = date.today() - timedelta(days=i)
        print(f"[{obs}] Fetching from data.boston.gov...")
        raw  = fetch_day(obs)
        rows = parse(raw)
        n    = upsert(conn, rows)
        print(f"[{obs}] {n} rows upserted\n")
        grand_total += n
        time.sleep(0.5)

    conn.close()
    print(f"=== Done — {grand_total} total rows loaded ===")

if __name__ == "__main__":
    main()
