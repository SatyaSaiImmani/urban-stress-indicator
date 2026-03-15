from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
    bronze_record = {
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
    return bronze_record


def build_output_path(output_root: Path, city: str, ingested_at_utc: str, batch_id: str) -> Path:
    dt = datetime.fromisoformat(ingested_at_utc.replace("Z", "+00:00"))
    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"

    output_dir = (
        output_root
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
        / f"city={city}"
        / f"batch_id={batch_id}"
    )
    ensure_dir(output_dir)
    return output_dir / "part-00001.jsonl"


def run(raw_dir: Path, output_root: Path) -> Path:
    response_path = raw_dir / "response.json"
    manifest_path = raw_dir / "manifest.json"

    if not response_path.exists():
        raise FileNotFoundError(f"Missing response.json at: {response_path}")

    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest.json at: {manifest_path}")

    payload = read_json(response_path)
    manifest = read_json(manifest_path)

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

    output_path = build_output_path(
        output_root=output_root,
        city=city,
        ingested_at_utc=ingested_at_utc,
        batch_id=batch_id,
    )

    write_jsonl(output_path, bronze_rows)

    print("Bronze Open311 extraction complete")
    print(f"Raw input directory : {raw_dir}")
    print(f"Output JSONL        : {output_path}")
    print(f"Row count           : {len(bronze_rows)}")
    print(f"Generated at UTC    : {utc_now_iso()}")

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten a raw Open311 batch into bronze JSONL records"
    )
    parser.add_argument(
        "--raw-dir",
        required=True,
        help="Path to raw batch directory containing response.json and manifest.json",
    )
    parser.add_argument(
        "--output-root",
        default="etl/open311/data/bronze",
        help="Root directory for bronze output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir).resolve()
    output_root = Path(args.output_root).resolve()

    run(raw_dir=raw_dir, output_root=output_root)


if __name__ == "__main__":
    main()
