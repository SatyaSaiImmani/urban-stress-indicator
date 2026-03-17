from __future__ import annotations

import argparse
import json
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Any, List, Tuple
from urllib.parse import urlparse

import boto3
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize Bronze Open311 JSONL from S3 into Silver Parquet on S3"
    )
    parser.add_argument(
        "--bronze-s3-file",
        required=True,
        help="S3 URI to Bronze JSONL file, e.g. s3://bucket/bronze/open311/.../part-00001.jsonl",
    )
    parser.add_argument(
        "--mapping-file",
        required=True,
        help="Local path to category mapping CSV",
    )
    parser.add_argument(
        "--output-s3-root",
        required=True,
        help="S3 root for Silver output, e.g. s3://bucket/silver/open311",
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URI, got: {uri}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not bucket:
        raise ValueError(f"Missing bucket in S3 URI: {uri}")
    if not key:
        raise ValueError(f"Missing key in S3 URI: {uri}")

    return bucket, key


def normalize_prefix(prefix: str) -> str:
    return prefix.rstrip("/")


def parse_timestamp(ts: Any):
    if ts is None or ts == "":
        return None
    try:
        return pd.to_datetime(ts, utc=True)
    except Exception:
        return None


def read_text_from_s3(s3_client, bucket: str, key: str) -> str:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def load_jsonl_from_s3(s3_client, bucket: str, key: str) -> List[dict]:
    text = read_text_from_s3(s3_client, bucket, key)
    rows = []

    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSONL in s3://{bucket}/{key} at line {line_number}: {exc}"
            ) from exc

    return rows


def build_s3_output_key(
    prefix_root: str,
    city: str,
    ingested_at_utc: str,
    batch_id: str,
) -> str:
    dt = datetime.fromisoformat(ingested_at_utc.replace("Z", "+00:00"))
    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"

    prefix_root = normalize_prefix(prefix_root)

    return (
        f"{prefix_root}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"city={city}/"
        f"batch_id={batch_id}/"
        f"part-00000.parquet"
    )


def write_parquet_to_s3(df: pd.DataFrame, s3_client, bucket: str, key: str) -> None:
    buffer = BytesIO()
    df.to_parquet(
    buffer,
    index=False,
    engine="pyarrow",
    coerce_timestamps="us",
    allow_truncated_timestamps=True,
    )
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def main() -> None:
    args = parse_args()
    s3_client = boto3.client("s3")

    bronze_bucket, bronze_key = parse_s3_uri(args.bronze_s3_file)
    output_bucket, output_prefix = parse_s3_uri(args.output_s3_root)

    mapping_file = Path(args.mapping_file)
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

    print("Loading bronze records from S3...")
    rows = load_jsonl_from_s3(s3_client, bronze_bucket, bronze_key)

    if not rows:
        raise RuntimeError("Bronze input file is empty")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Bronze dataframe is empty after load")
    batch_id = df["batch_id"].iloc[0]
    required_columns = {
        "city",
        "batch_id",
        "ingested_at_utc",
        "service_name",
        "requested_datetime",
        "updated_datetime",
        "closed_datetime",
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(
            f"Bronze input is missing required columns: {', '.join(sorted(missing))}"
        )

    print("Parsing timestamps...")
    df["requested_at_utc"] = df["requested_datetime"].apply(parse_timestamp)
    df["updated_at_utc"] = df["updated_datetime"].apply(parse_timestamp)
    df["closed_at_utc"] = df["closed_datetime"].apply(parse_timestamp)

    print("Loading category mapping from local file...")
    mapping = pd.read_csv(mapping_file)

    required_mapping_cols = {
        "raw_category",
        "normalized_category",
        "is_infrastructure",
        "is_emergency_type",
    }
    missing_mapping = required_mapping_cols - set(mapping.columns)
    if missing_mapping:
        raise ValueError(
            f"Mapping file is missing required columns: {', '.join(sorted(missing_mapping))}"
        )

    df = df.merge(
        mapping,
        left_on="service_name",
        right_on="raw_category",
        how="left",
    )

    df["normalized_category"] = df["normalized_category"].fillna("Other")
    df["is_infrastructure"] = df["is_infrastructure"].fillna(0).astype(int)
    df["is_emergency_type"] = df["is_emergency_type"].fillna(0).astype(int)

    city = df["city"].iloc[0]
    ingested_at_utc = df["ingested_at_utc"].iloc[0]

    output_key = build_s3_output_key(
        prefix_root=output_prefix,
        city=city,
        ingested_at_utc=ingested_at_utc,
        batch_id=batch_id,
    )

    print("Writing Silver parquet to S3...")
    write_parquet_to_s3(
        df=df,
        s3_client=s3_client,
        bucket=output_bucket,
        key=output_key,
    )

    output_location = f"s3://{output_bucket}/{output_key}"

    print("Silver normalization complete")
    print("Rows:", len(df))
    print("Output:", output_location)
    print("Mapping file:", mapping_file)


if __name__ == "__main__":
    main()
