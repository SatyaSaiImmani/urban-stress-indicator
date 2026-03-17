from __future__ import annotations

import argparse
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate Open311 Silver Parquet into hourly, daily, and category-hourly metrics using Spark"
    )
    parser.add_argument(
        "--silver-file",
        required=True,
        help="Path to Silver Parquet file or directory",
    )
    parser.add_argument(
        "--hourly-output-root",
        required=True,
        help="Root output path for hourly aggregates",
    )
    parser.add_argument(
        "--daily-output-root",
        required=True,
        help="Root output path for daily aggregates",
    )
    parser.add_argument(
        "--category-output-root",
        required=True,
        help="Root output path for category-hourly aggregates",
    )
    parser.add_argument(
        "--app-name",
        default="Open311AggregationSpark",
        help="Spark application name",
    )
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    df = spark.read.parquet(silver_path)

    required_columns = {
        "city",
        "status",
        "service_request_id",
        "normalized_category",
        "is_infrastructure",
        "is_emergency_type",
        "requested_at_utc",
        "ingested_at_utc",
        "batch_id",
    }

    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Silver input is missing required columns: {', '.join(missing)}")

    return df


def prepare_base_dataframe(df: DataFrame) -> DataFrame:
    prepared = (
        df.withColumn("requested_at_utc_ts", F.to_timestamp("requested_at_utc"))
        .withColumn("ingested_at_utc_ts", F.to_timestamp("ingested_at_utc"))
        .filter(F.col("requested_at_utc_ts").isNotNull())
        .filter(F.col("ingested_at_utc_ts").isNotNull())
        .withColumn("status_lower", F.lower(F.coalesce(F.col("status"), F.lit(""))))
        .withColumn(
            "is_open",
            F.when(F.col("status_lower") == F.lit("open"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_closed",
            F.when(F.col("status_lower") == F.lit("closed"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_infrastructure_int",
            F.coalesce(F.col("is_infrastructure").cast("int"), F.lit(0)),
        )
        .withColumn(
            "is_emergency_type_int",
            F.coalesce(F.col("is_emergency_type").cast("int"), F.lit(0)),
        )
        .withColumn(
            "normalized_category_clean",
            F.coalesce(F.col("normalized_category"), F.lit("Other")),
        )
        .withColumn("hour_window", F.date_trunc("hour", F.col("requested_at_utc_ts")))
        .withColumn("day_window", F.to_date(F.col("requested_at_utc_ts")))
        .withColumn("ingest_year", F.date_format(F.col("ingested_at_utc_ts"), "yyyy"))
        .withColumn("ingest_month", F.date_format(F.col("ingested_at_utc_ts"), "MM"))
        .withColumn("ingest_day", F.date_format(F.col("ingested_at_utc_ts"), "dd"))
    )

    if prepared.rdd.isEmpty():
        raise RuntimeError("No valid rows available after parsing requested_at_utc and ingested_at_utc")

    return prepared


def compute_hourly(df: DataFrame) -> DataFrame:
    hourly = (
        df.groupBy("city", "ingest_year", "ingest_month", "ingest_day", "hour_window")
        .agg(
            F.count("service_request_id").alias("total_requests"),
            F.sum("is_open").alias("open_requests"),
            F.sum("is_closed").alias("closed_requests"),
            F.sum("is_infrastructure_int").alias("infrastructure_requests"),
            F.sum("is_emergency_type_int").alias("emergency_requests"),
            F.countDistinct("normalized_category_clean").alias("distinct_categories"),
        )
        .withColumn(
            "emergency_ratio",
            F.when(
                F.col("total_requests") > 0,
                F.col("emergency_requests") / F.col("total_requests"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumnRenamed("hour_window", "window_start")
        .select(
            "city",
            "ingest_year",
            "ingest_month",
            "ingest_day",
            "window_start",
            "total_requests",
            "open_requests",
            "closed_requests",
            "infrastructure_requests",
            "emergency_requests",
            "emergency_ratio",
            "distinct_categories",
        )
        .orderBy("city", "window_start")
    )
    return hourly


def compute_daily(df: DataFrame) -> DataFrame:
    daily = (
        df.groupBy("city", "ingest_year", "ingest_month", "ingest_day", "day_window")
        .agg(
            F.count("service_request_id").alias("total_requests"),
            F.sum("is_open").alias("open_requests"),
            F.sum("is_closed").alias("closed_requests"),
            F.sum("is_infrastructure_int").alias("infrastructure_requests"),
            F.sum("is_emergency_type_int").alias("emergency_requests"),
            F.countDistinct("normalized_category_clean").alias("distinct_categories"),
        )
        .withColumn(
            "emergency_ratio",
            F.when(
                F.col("total_requests") > 0,
                F.col("emergency_requests") / F.col("total_requests"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumnRenamed("day_window", "window_start")
        .select(
            "city",
            "ingest_year",
            "ingest_month",
            "ingest_day",
            "window_start",
            "total_requests",
            "open_requests",
            "closed_requests",
            "infrastructure_requests",
            "emergency_requests",
            "emergency_ratio",
            "distinct_categories",
        )
        .orderBy("city", "window_start")
    )
    return daily


def compute_category_hourly(df: DataFrame) -> DataFrame:
    category_hourly = (
        df.groupBy(
            "city",
            "ingest_year",
            "ingest_month",
            "ingest_day",
            "hour_window",
            "normalized_category_clean",
        )
        .agg(F.count("service_request_id").alias("request_count"))
        .withColumnRenamed("hour_window", "window_start")
        .withColumnRenamed("normalized_category_clean", "normalized_category")
        .select(
            "city",
            "ingest_year",
            "ingest_month",
            "ingest_day",
            "window_start",
            "normalized_category",
            "request_count",
        )
        .orderBy("city", "window_start", "normalized_category")
    )
    return category_hourly


def write_partitioned(df: DataFrame, output_root: str) -> None:
    (
        df.write.mode("overwrite")
        .partitionBy("ingest_year", "ingest_month", "ingest_day", "city")
        .parquet(output_root)
    )


def summarize_counts(hourly: DataFrame, daily: DataFrame, category_hourly: DataFrame) -> Tuple[int, int, int]:
    return hourly.count(), daily.count(), category_hourly.count()


def main() -> None:
    args = parse_args()

    spark = build_spark(args.app_name)

    try:
        silver_df = load_silver(spark, args.silver_file)
        base_df = prepare_base_dataframe(silver_df)

        hourly_df = compute_hourly(base_df)
        daily_df = compute_daily(base_df)
        category_hourly_df = compute_category_hourly(base_df)

        hourly_rows, daily_rows, category_hourly_rows = summarize_counts(
            hourly_df, daily_df, category_hourly_df
        )

        write_partitioned(hourly_df, args.hourly_output_root)
        write_partitioned(daily_df, args.daily_output_root)
        write_partitioned(category_hourly_df, args.category_output_root)

        print("Open311 Spark aggregation complete")
        print(f"Silver input           : {args.silver_file}")
        print(f"Hourly rows            : {hourly_rows}")
        print(f"Daily rows             : {daily_rows}")
        print(f"Category-hourly rows   : {category_hourly_rows}")
        print(f"Hourly output root     : {args.hourly_output_root}")
        print(f"Daily output root      : {args.daily_output_root}")
        print(f"Category output root   : {args.category_output_root}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()