from __future__ import annotations

import argparse
import os
from pathlib import Path

from client import Open311Client
from config import load_city_config
from s3_io import S3Writer
from utils import ensure_dir, make_batch_id, utc_now_iso, write_json
from dotenv import load_dotenv
load_dotenv()


def build_output_dir(city: str, ingest_date: str, batch_id: str) -> Path:
    base = Path(__file__).resolve().parent / "data" / "raw" / city / ingest_date / batch_id
    ensure_dir(base)
    return base

def main() -> None:
    parser = argparse.ArgumentParser(description="Open311 raw collector")
    parser.add_argument("--city", required=True)
    parser.add_argument("--updated-after", required=True)
    parser.add_argument("--updated-before", required=True)
    parser.add_argument("--bucket", required=False, help="Optional S3 bucket", default=os.getenv("S3_BUCKET_NAME"))
    parser.add_argument(
        "--batch-id",
        required=False,
        help="Optional externally supplied batch id (useful for Airflow)",
    )
    args = parser.parse_args()

    city_cfg = load_city_config(args.city)
    api_key = os.getenv("OPEN311_API_KEY")

    client = Open311Client(
        base_url=city_cfg["base_url"],
        api_key=api_key,
    )

    batch_id = args.batch_id if args.batch_id else make_batch_id()
    ingested_at = utc_now_iso()
    ingest_date = ingested_at[:10]

    params = {
        "updated_after": args.updated_after,
        "updated_before": args.updated_before,
    }

    status_code, data, headers = client.get(city_cfg["requests_path"], params=params)

    if not data:
        raise RuntimeError("Open311 response returned zero records")

    if isinstance(data, list) and len(data) < 5:
        print("WARNING: Very small response size, verify API window")

    if isinstance(data, list) and len(data) > 0:
        sample = data[0]
        required = ["service_request_id", "requested_datetime"]
        for field in required:
            if field not in sample:
                print(f"WARNING: Expected field missing: {field}")

    record_count = len(data) if isinstance(data, list) else 1
    output_dir = build_output_dir(args.city, ingest_date, batch_id)

    response_path = output_dir / "response.json"
    manifest_path = output_dir / "manifest.json"

    write_json(response_path, data)

    manifest = {
        "source": "open311",
        "city": args.city,
        "batch_id": batch_id,
        "ingested_at_utc": ingested_at,
        "record_count": record_count,
        "updated_after": args.updated_after,
        "updated_before": args.updated_before,
        "status_code": status_code,
    }

    if args.bucket:
        s3 = S3Writer(args.bucket)
        s3_prefix = (
            f"raw/open311/city={args.city}/ingest_date={ingest_date}/batch_id={batch_id}"
        )

        manifest["bucket"] = args.bucket
        manifest["s3_prefix"] = s3_prefix

        s3.upload_file(response_path, f"{s3_prefix}/response.json")
        print(f"Uploaded to S3: s3://{args.bucket}/{s3_prefix}/response.json")

    write_json(manifest_path, manifest)

    if args.bucket:
        s3.upload_file(manifest_path, f"{manifest['s3_prefix']}/manifest.json")
        print(f"Uploaded to S3: s3://{args.bucket}/{manifest['s3_prefix']}/manifest.json")

    print(f"Saved raw response to: {response_path}")
    print(f"Saved manifest to: {manifest_path}")
    print(f"Record count: {record_count}")

    print("------ Open311 Ingestion Summary ------")
    print("City:", args.city)
    print("Batch ID:", batch_id)
    print("Ingest Time:", ingested_at)
    print("Records:", record_count)
    print("Output:", response_path)
    if args.bucket:
        print("S3 Prefix:", manifest["s3_prefix"])
    print("---------------------------------------")


if __name__ == "__main__":
    main()
