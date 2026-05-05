import os
import requests
from datetime import date, timedelta

from s3 import write_bronze
from db import get_connection, upsert

BASE_URL  = "https://311.boston.gov/open311/v2/requests.json"
PAGE_SIZE = 50   # Boston Open311 caps at 50; requesting 100 returns non-JSON garbage

def fetch_requests(obs_date: date) -> list:
    """Paginate through all Open311 requests for obs_date."""
    all_results = []
    page = 1
    while True:
        params = {
            "start_date": f"{obs_date}T00:00:00Z",
            "end_date":   f"{obs_date}T23:59:59Z",
            "page_size":  PAGE_SIZE,
            "page":       page,
        }
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  Page {page} HTTP error: {e} — stopping")
            break

        # API returns empty body (not []) when no more results
        if not r.text.strip():
            break

        try:
            batch = r.json()
        except Exception:
            print(f"  Bad JSON on page {page} (body: {r.text[:100]!r}) — stopping")
            break

        if not batch:
            break

        all_results.extend(batch)
        print(f"  Page {page}: {len(batch)} records (total so far: {len(all_results)})")

        if len(batch) < PAGE_SIZE:
            break   # last page — no more results
        page += 1

    return all_results

def parse_requests(raw: list) -> list:
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
            "resolution_hrs":     None,  # computed in Glue silver_etl
            "lat":                r.get("lat"),
            "lon":                r.get("long"),
            "neighbourhood":      None,  # derived via spatial join in Glue
            "description":        r.get("description"),
        })
    return rows

def handler(event=None, context=None):
    obs_date = date.today() - timedelta(days=1)

    print(f"Fetching Open311 requests for {obs_date}")
    raw = fetch_requests(obs_date)
    print(f"  Fetched {len(raw)} requests")

    if os.environ.get("SKIP_S3") != "true":
        path = write_bronze("open311", raw, obs_date)
        print(f"  Written to {path}")

    if os.environ.get("SKIP_DB") != "true":
        rows = parse_requests(raw)
        conn = get_connection()
        n = upsert(conn, "requests_311", rows, ["service_request_id"])
        conn.close()
        print(f"  Upserted {n} rows to requests_311")

    return {"status": "ok", "date": str(obs_date), "count": len(raw)}

if __name__ == "__main__":
    result = handler()
    print(result)
