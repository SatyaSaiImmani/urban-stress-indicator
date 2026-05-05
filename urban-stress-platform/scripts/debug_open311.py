"""
debug_open311.py — Diagnose what the Boston Open311 API returns for given dates.
Run this locally to see the raw response status codes and body previews.

Usage:
  python3 scripts/debug_open311.py
"""
import requests

BASE_URL = "https://311.boston.gov/open311/v2/requests.json"

TEST_DATES = [
    "2026-04-04",  # worked before (572 rows)
    "2026-04-05",  # worked before (380 rows)
    "2026-04-06",  # failed — "Bad JSON on page 1"
    "2026-04-10",
    "2026-04-20",
    "2026-05-01",
    "2026-05-03",
]

for test_date in TEST_DATES:
    params = {
        "start_date": f"{test_date}T00:00:00Z",
        "end_date":   f"{test_date}T23:59:59Z",
        "page_size":  10,
        "page":       1,
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=20)
        body_preview = repr(r.text[:300])
        print(f"{test_date}: HTTP {r.status_code}  len={len(r.text)}  body={body_preview}")
    except Exception as e:
        print(f"{test_date}: ERROR — {e}")
