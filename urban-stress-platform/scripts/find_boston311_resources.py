"""
find_boston311_resources.py — Discover all Boston 311 dataset resource IDs from data.boston.gov
Run this once to see which resource IDs to use for different years.

Usage:
  python3 scripts/find_boston311_resources.py
"""
import requests
import json

CKAN_API = "https://data.boston.gov/api/3/action/package_show"

# Try known package slugs for Boston 311
PACKAGES = [
    "311-service-requests",
    "311-service-requests-new-system",
]

for pkg_id in PACKAGES:
    print(f"\n=== Querying package: {pkg_id} ===")
    try:
        r = requests.get(CKAN_API, params={"id": pkg_id}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            print(f"  Not found: {data.get('error', {}).get('message', 'unknown error')}")
            continue

        result = data["result"]
        print(f"  Title: {result.get('title')}")
        print(f"  Resources ({len(result.get('resources', []))}):")
        for res in result.get("resources", []):
            print(f"    ID: {res['id']}  Name: {res.get('name', 'unnamed')}  Format: {res.get('format', '?')}")
            print(f"         URL: {res.get('url', 'N/A')}")

    except Exception as e:
        print(f"  Error: {e}")

print("\n=== Also trying direct Socrata discovery ===")
# Try known and guessed resource IDs directly
KNOWN_IDS = {
    "2025": "9d7c2214-4709-478a-a2e8-fb2020a5bb94",
    "2024": "dff4d804-5031-443a-8409-8344efd0e5c8",
    "2021": "f53ebccd-bc61-49f9-83db-625f209c95f5",
    "2020": "6ff6a6fd-3141-4440-a880-6f60a37fe789",
}

for year, res_id in KNOWN_IDS.items():
    url = f"https://data.boston.gov/resource/{res_id}.json"
    try:
        r = requests.get(url, params={"$limit": 1}, timeout=10)
        status = r.status_code
        if status == 200:
            sample = r.json()
            fields = list(sample[0].keys()) if sample else []
            print(f"  {year} ({res_id}): OK — fields: {fields[:6]}...")
        else:
            print(f"  {year} ({res_id}): HTTP {status}")
    except Exception as e:
        print(f"  {year} ({res_id}): Error — {e}")
