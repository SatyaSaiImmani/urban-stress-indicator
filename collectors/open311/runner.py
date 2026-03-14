from __future__ import annotations

import argparse
from pathlib import Path
import os

from collectors.open311.client import Open311Client
from collectors.open311.config import load_city_config
from collectors.open311.utils import utc_now_iso, make_batch_id, ensure_dir, write_json


def build_output_dir(city: str, ingest_date: str, batch_id: str) -> Path:
    base = Path(__file__).resolve().parent / "data" / "raw" / city / ingest_date / batch_id
    ensure_dir(base)
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description="Open311 raw collector")
    parser.add_argument("--city", required=True)
    parser.add_argument("--updated-after", required=True)
    parser.add_argument("--updated-before", required=True)
    args = parser.parse_args()

    city_cfg = load_city_config(args.city)
    api_key = os.getenv("OPEN311_API_KEY")

    client = Open311Client(
        base_url=city_cfg["base_url"],
        api_key=api_key,
    )

    batch_id = make_batch_id()
    ingested_at = utc_now_iso()
    ingest_date = ingested_at[:10]

    params = {
        "updated_after": args.updated_after,
        "updated_before": args.updated_before,
    }

    status_code, data, headers = client.get(city_cfg["requests_path"], params=params)

    record_count = len(data) if isinstance(data, list) else 1
    output_dir = build_output_dir(args.city, ingest_date, batch_id)

    response_path = output_dir / "response.json"
    manifest_path = output_dir / "manifest.json"

    manifest = {
        "source": "open311",
        "city": args.city,
        "base_url": city_cfg["base_url"],
        "endpoint": city_cfg["requests_path"],
        "request_params": params,
        "ingested_at_utc": ingested_at,
        "batch_id": batch_id,
        "http_status": status_code,
        "record_count": record_count,
        "response_file": str(response_path),
        "response_headers": headers,
    }

    write_json(response_path, data)
    write_json(manifest_path, manifest)

    print(f"Saved raw response to: {response_path}")
    print(f"Saved manifest to: {manifest_path}")
    print(f"Record count: {record_count}")


if __name__ == "__main__":
    main()
