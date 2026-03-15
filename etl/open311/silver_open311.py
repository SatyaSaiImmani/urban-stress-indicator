from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime
import pandas as pd


def load_jsonl(path: Path):
    rows = []
    with path.open() as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def parse_timestamp(ts):
    if not ts:
        return None
    try:
        return pd.to_datetime(ts, utc=True)
    except Exception:
        return None


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-file", required=True)
    parser.add_argument("--mapping-file", required=True)
    parser.add_argument("--output-root", default="etl/open311/data/silver")
    args = parser.parse_args()

    bronze_file = Path(args.bronze_file)
    mapping_file = Path(args.mapping_file)

    print("Loading bronze records...")
    rows = load_jsonl(bronze_file)

    df = pd.DataFrame(rows)

    print("Parsing timestamps...")
    df["requested_at_utc"] = df["requested_datetime"].apply(parse_timestamp)
    df["updated_at_utc"] = df["updated_datetime"].apply(parse_timestamp)
    df["closed_at_utc"] = df["closed_datetime"].apply(parse_timestamp)

    print("Loading category mapping...")
    mapping = pd.read_csv(mapping_file)

    df = df.merge(
        mapping,
        left_on="service_name",
        right_on="raw_category",
        how="left"
    )

    df["normalized_category"] = df["normalized_category"].fillna("Other")
    df["is_infrastructure"] = df["is_infrastructure"].fillna(0)
    df["is_emergency_type"] = df["is_emergency_type"].fillna(0)

    city = df["city"].iloc[0]
    ts = df["ingested_at_utc"].iloc[0]

    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"

    output_dir = (
        Path(args.output_root)
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
        / f"city={city}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "part-00000.parquet"

    print("Writing Silver parquet...")
    df.to_parquet(output_file, index=False)

    print("Silver normalization complete")
    print("Rows:", len(df))
    print("Output:", output_file)


if __name__ == "__main__":
    main()
