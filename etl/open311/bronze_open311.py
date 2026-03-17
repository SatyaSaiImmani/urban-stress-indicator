from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
import boto3

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {uri}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not bucket:
        raise ValueError(f"Missing bucket in S3 URI: {uri}")
    if not key:
        raise ValueError(f"Missing key/prefix in S3 URI: {uri}")

    return bucket, key


def normalize_prefix(prefix: str) -> str:
    return prefix.rstrip("/")


def read_json_from_s3(s3_client, bucket: str, key: str) -> Any:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def write_text_to_s3(s3_client, bucket: str, key: str, text: str) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="application/jsonl",
    )


def validate_manifest(manifest: Dict[str, Any]) -> None:
    required_fields = ["city", "batch_id", "ingested_at_utc"]

    missing = [field for field in required_fields if field not in manifest]
    if missing:
        raise ValueError(
            f"manifest.json is missing required fields: {', '.join(missing)}"
        )


def validate_response_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        raise ValueError("response.json must contain a JSON list")

    for i, record in enumerate(payload):
        if not isinstance(record, dict):
            raise ValueError(f"response.json record at index {i} is not a JSON object")

    return payload


def safe_get(record: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record:
            return record.get(key)
    return None


def flatten_open311_record(
    record: Dict[str, Any],
    city: str,
    batch_id: str,
    ingested_at_utc: str,
) -> Dict[str, Any]:
    return {
        "source": "open311",
        "city": city,
        "batch_id": batch_id,
        "ingested_at_utc": ingested_at_utc,
        "service_request_id": safe_get(record, "service_request_id"),
        "status": safe_get(record, "status"),
        "service_name": safe_get(record, "service_name"),
        "service_code": safe_get(record, "service_code"),
        "description": safe_get(record, "description"),
        "requested_datetime": safe_get(record, "requested_datetime"),
        "updated_datetime": safe_get(record, "updated_datetime"),
        "closed_datetime": safe_get(record, "closed_datetime"),
        "address": safe_get(record, "address"),
        "lat": safe_get(record, "lat", "latitude"),
        "lon": safe_get(record, "long", "lon", "longitude"),
        "media_url": safe_get(record, "media_url"),
        "raw_payload": record,
    }


def build_output_key(output_prefix: str, city: str, ingested_at_utc: str, batch_id: str) -> str:
    dt = datetime.fromisoformat(ingested_at_utc.replace("Z", "+00:00"))
    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"

    output_prefix = normalize_prefix(output_prefix)

    return (
        f"{output_prefix}/year={year}/month={month}/day={day}/"
        f"city={city}/batch_id={batch_id}/part-00001.jsonl"
    )


def build_jsonl(rows: List[Dict[str, Any]]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)


def run(raw_s3_dir: str, output_s3_root: str) -> str:
    s3_client = boto3.client("s3")

    raw_bucket, raw_prefix = parse_s3_uri(raw_s3_dir)
    output_bucket, output_prefix = parse_s3_uri(output_s3_root)

    raw_prefix = normalize_prefix(raw_prefix)
    output_prefix = normalize_prefix(output_prefix)

    response_key = f"{raw_prefix}/response.json"
    manifest_key = f"{raw_prefix}/manifest.json"

    payload = read_json_from_s3(s3_client, raw_bucket, response_key)
    manifest = read_json_from_s3(s3_client, raw_bucket, manifest_key)

    validate_manifest(manifest)
    records = validate_response_payload(payload)

    city = manifest["city"]
    batch_id = manifest["batch_id"]
    ingested_at_utc = manifest["ingested_at_utc"]

    bronze_rows = [
        flatten_open311_record(
            record=record,
            city=city,
            batch_id=batch_id,
            ingested_at_utc=ingested_at_utc,
        )
        for record in records
    ]

    output_key = build_output_key(
        output_prefix=output_prefix,
        city=city,
        ingested_at_utc=ingested_at_utc,
        batch_id=batch_id,
    )

    jsonl_text = build_jsonl(bronze_rows)
    write_text_to_s3(s3_client, output_bucket, output_key, jsonl_text)

    output_uri = f"s3://{output_bucket}/{output_key}"

    print("Bronze Open311 extraction complete")
    print(f"Raw input S3 dir   : s3://{raw_bucket}/{raw_prefix}")
    print(f"Output JSONL S3    : {output_uri}")
    print(f"Row count          : {len(bronze_rows)}")
    print(f"Generated at UTC   : {utc_now_iso()}")

    return output_uri


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten a raw Open311 batch from S3 into Bronze JSONL on S3"
    )
    parser.add_argument(
        "--raw-s3-dir",
        required=True,
        help="S3 prefix containing response.json and manifest.json, e.g. s3://bucket/raw/open311/city=boston/ingest_date=2026-03-14/batch_id=...",
    )
    parser.add_argument(
        "--output-s3-root",
        required=True,
        help="S3 root for Bronze output, e.g. s3://bucket/bronze/open311",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(raw_s3_dir=args.raw_s3_dir, output_s3_root=args.output_s3_root)


if __name__ == "__main__":
    main()
