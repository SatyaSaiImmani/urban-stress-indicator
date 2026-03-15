from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_silver(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def build_partition_path(root: Path, city: str, dt: pd.Timestamp) -> Path:
    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    outdir = root / f"year={year}" / f"month={month}" / f"day={day}" / f"city={city}"
    ensure_dir(outdir)
    return outdir / "part-00000.parquet"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-file", required=True)
    parser.add_argument("--hourly-output-root", required=True)
    parser.add_argument("--daily-output-root", required=True)
    parser.add_argument("--category-output-root", required=True)
    args = parser.parse_args()

    silver_file = Path(args.silver_file)
    df = load_silver(silver_file)

    if df.empty:
        raise RuntimeError("Silver input is empty")

    if "requested_at_utc" not in df.columns:
        raise RuntimeError("Missing required column: requested_at_utc")

    df = df.copy()
    df["requested_at_utc"] = pd.to_datetime(df["requested_at_utc"], utc=True, errors="coerce")
    df = df[df["requested_at_utc"].notna()].copy()

    if df.empty:
        raise RuntimeError("No valid requested_at_utc values available for aggregation")

    city = df["city"].iloc[0]

    df["hour_window"] = df["requested_at_utc"].dt.floor("h")

    df["day_window"] = df["requested_at_utc"].dt.floor("D")

    df["is_open"] = (df["status"].fillna("").str.lower() == "open").astype(int)
    df["is_closed"] = (df["status"].fillna("").str.lower() == "closed").astype(int)
    df["is_infrastructure"] = df["is_infrastructure"].fillna(0).astype(int)
    df["is_emergency_type"] = df["is_emergency_type"].fillna(0).astype(int)

    hourly = (
        df.groupby(["city", "hour_window"], as_index=False)
        .agg(
            total_requests=("service_request_id", "count"),
            open_requests=("is_open", "sum"),
            closed_requests=("is_closed", "sum"),
            infrastructure_requests=("is_infrastructure", "sum"),
            emergency_requests=("is_emergency_type", "sum"),
            distinct_categories=("normalized_category", "nunique"),
        )
    )
    hourly["emergency_ratio"] = hourly["emergency_requests"] / hourly["total_requests"]

    daily = (
        df.groupby(["city", "day_window"], as_index=False)
        .agg(
            total_requests=("service_request_id", "count"),
            open_requests=("is_open", "sum"),
            closed_requests=("is_closed", "sum"),
            infrastructure_requests=("is_infrastructure", "sum"),
            emergency_requests=("is_emergency_type", "sum"),
            distinct_categories=("normalized_category", "nunique"),
        )
    )
    daily["emergency_ratio"] = daily["emergency_requests"] / daily["total_requests"]

    category_hourly = (
        df.groupby(["city", "hour_window", "normalized_category"], as_index=False)
        .agg(request_count=("service_request_id", "count"))
    )

    sample_dt = pd.to_datetime(df["ingested_at_utc"].iloc[0], utc=True)


    hourly_path = build_partition_path(Path(args.hourly_output_root), city, sample_dt)
    daily_path = build_partition_path(Path(args.daily_output_root), city, sample_dt)
    category_path = build_partition_path(Path(args.category_output_root), city, sample_dt)

    hourly.to_parquet(hourly_path, index=False)
    daily.to_parquet(daily_path, index=False)
    category_hourly.to_parquet(category_path, index=False)

    print("Aggregation complete")
    print("Hourly rows:", len(hourly))
    print("Daily rows:", len(daily))
    print("Category-hourly rows:", len(category_hourly))
    print("Hourly output:", hourly_path)
    print("Daily output:", daily_path)
    print("Category output:", category_path)


if __name__ == "__main__":
    main()
