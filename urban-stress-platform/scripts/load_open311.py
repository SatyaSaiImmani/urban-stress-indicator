"""
load_open311.py — Fetch Boston Open311 requests and load into RDS
Usage: python3 scripts/load_open311.py [DAYS]
Default: 30 days back from yesterday
"""

import os
import sys
import time
import json
import psycopg2
import requests
from datetime import date, timedelta
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────────────
DAYS      = int(sys.argv[1]) if len(sys.argv) > 1 else 30
BASE_URL  = "https://311.boston.gov/open311/v2/requests.json"
PAGE_SIZE = 50   # Boston Open311 caps at 50; requesting 100 returns non-JSON garbage

DB_HOST = os.environ.get("DB_HOST", "urbanstressdemo-db5d02a0a9-k590fdsgpdha.c6x2q6oeq58g.us-east-1.rds.amazonaws.com")
DB_PASS = os.environ.get("DB_PASS", "")
DB_USER = "postgres"
DB_NAME = "postgres"

# ── DB connection ─────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=5432, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode="require",
        connect_timeout=10,
    )

# ── Fetch one day (paginated) ─────────────────────────────────────────────────
def fetch_page(obs: date, page: int, retries: int = 4) -> list | None:
    """Fetch one page, returning None if the API is throttling (bad JSON / non-200)."""
    params = {
        "start_date": f"{obs}T00:00:00Z",
        "end_date":   f"{obs}T23:59:59Z",
        "page_size":  PAGE_SIZE,
        "page":       page,
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            wait = attempt * 5
            print(f"    HTTP error (attempt {attempt}): {e} — retrying in {wait}s")
            time.sleep(wait)
            continue

        # Empty body means no more results
        if not r.text.strip():
            return []

        try:
            return r.json()
        except Exception:
            # Print body so we know what we're dealing with (rate-limit HTML, etc.)
            preview = repr(r.text[:200])
            print(f"    Bad JSON (attempt {attempt}), HTTP {r.status_code}, body: {preview}")
            wait = attempt * 10
            print(f"    Sleeping {wait}s before retry...")
            time.sleep(wait)

    print(f"    Giving up on page {page} after {retries} attempts")
    return None

def fetch_day(obs: date) -> list:
    all_rows = []
    page = 1
    while True:
        batch = fetch_page(obs, page)

        if batch is None:        # all retries exhausted
            break
        if not batch:            # empty body or empty list → done
            break

        all_rows.extend(batch)
        print(f"    Page {page}: {len(batch)} records (running total: {len(all_rows)})")

        if len(batch) < PAGE_SIZE:
            break   # last page

        page += 1
        time.sleep(2)   # Boston Open311 throttles hard — be generous between pages

    return all_rows

# ── Parse into DB rows ────────────────────────────────────────────────────────
def parse(raw: list) -> list:
    rows = []
    for r in raw:
        if not r.get("service_request_id"):
            continue
        parts = (r.get("service_code") or "").split(":")
        rows.append({
            "service_request_id": r["service_request_id"],
            "service_code":       r.get("service_code"),
            "service_name":       r.get("service_name"),
            "department":         parts[0] if len(parts) > 0 else None,
            "category":           parts[1] if len(parts) > 1 else None,
            "status":             r.get("status"),
            "requested_datetime": r.get("requested_datetime"),
            "updated_datetime":   r.get("updated_datetime"),
            "resolution_hrs":     None,
            "lat":                r.get("lat"),
            "lon":                r.get("long"),
            "neighbourhood":      None,
            "description":        r.get("description"),
        })
    return rows

# ── Upsert ────────────────────────────────────────────────────────────────────
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

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== Loading Open311 — last {DAYS} days ===\n")
    conn = get_conn()
    grand_total = 0

    for i in range(DAYS, 0, -1):
        obs = date.today() - timedelta(days=i)
        print(f"[{obs}] Fetching...")
        raw  = fetch_day(obs)
        rows = parse(raw)
        n    = upsert(conn, rows)
        print(f"[{obs}] {n} rows upserted\n")
        grand_total += n
        time.sleep(3)   # pause between days to avoid triggering rate-limit

    conn.close()
    print(f"=== Done — {grand_total} total rows loaded ===")

if __name__ == "__main__":
    main()
