from __future__ import annotations

import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DEFAULT_BUCKET = "usi-blob-storage"
DEFAULT_METRICS = [
    "total_requests",
    "infrastructure_requests",
    "emergency_ratio",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute Open311 hourly rolling baselines from aggregate Parquet using Spark"
    )
    parser.add_argument(
        "--input-root",
        default=f"s3a://{DEFAULT_BUCKET}/aggregates/open311/hourly/",
        help="S3A path to hourly aggregate input root",
    )
    parser.add_argument(
        "--output-root",
        default=f"s3a://{DEFAULT_BUCKET}/baselines/open311/hourly/",
        help="S3A path to hourly baseline output root",
    )
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master URL, e.g. local[*] or spark://<ip>:7077",
    )
    parser.add_argument(
        "--app-name",
        default="Open311HourlyBaselinesSpark",
        help="Spark application name",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=64,
        help="Spark SQL shuffle partitions",
    )
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=0,
        help="Optional number of output partitions before write; 0 keeps Spark default",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24 * 7,
        help="Trailing lookback window in hours for baseline computation",
    )
    parser.add_argument(
        "--min-history-points",
        type=int,
        default=24,
        help="Minimum prior hourly points required before a baseline is considered valid",
    )
    return parser.parse_args()


def validate_s3a_path(path: str, arg_name: str) -> None:
    if not path.startswith("s3a://"):
        raise ValueError(f"{arg_name} must be an s3a:// path. Got: {path}")


def build_spark(app_name: str, master: str, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_hourly_aggregates(spark: SparkSession, input_root: str) -> DataFrame:
    df = spark.read.parquet(input_root)

    required_columns = {
        "city",
        "window_start",
        "total_requests",
        "infrastructure_requests",
        "emergency_ratio",
    }

    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(
            f"Hourly aggregate input is missing required columns: {', '.join(missing)}"
        )

    return df


def prepare_hourly_dataframe(df: DataFrame) -> DataFrame:
    prepared = (
        df.withColumn("window_start_ts", F.to_timestamp("window_start"))
        .filter(F.col("window_start_ts").isNotNull())
        .withColumn("window_start_epoch", F.col("window_start_ts").cast("long"))
        .withColumn("total_requests", F.coalesce(F.col("total_requests").cast("double"), F.lit(0.0)))
        .withColumn(
            "infrastructure_requests",
            F.coalesce(F.col("infrastructure_requests").cast("double"), F.lit(0.0)),
        )
        .withColumn("emergency_ratio", F.coalesce(F.col("emergency_ratio").cast("double"), F.lit(0.0)))
    )

    if prepared.limit(1).count() == 0:
        raise RuntimeError("No valid aggregate rows found after parsing window_start")

    return prepared


def add_metric_baselines(
    df: DataFrame,
    metric_col: str,
    base_window: Window,
    min_history_points: int,
) -> DataFrame:
    mean_col = f"{metric_col}_mean"
    std_col = f"{metric_col}_std"
    count_col = f"{metric_col}_history_points"
    z_col = f"{metric_col}_zscore"
    valid_col = f"{metric_col}_baseline_valid"

    return (
        df.withColumn(mean_col, F.avg(F.col(metric_col)).over(base_window))
        .withColumn(std_col, F.stddev_samp(F.col(metric_col)).over(base_window))
        .withColumn(count_col, F.count(F.col(metric_col)).over(base_window))
        .withColumn(
            valid_col,
            (F.col(count_col) >= F.lit(min_history_points))
            & F.col(std_col).isNotNull()
            & (F.col(std_col) > 0),
        )
        .withColumn(
            z_col,
            F.when(
                F.col(valid_col),
                (F.col(metric_col) - F.col(mean_col)) / F.col(std_col),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def compute_hourly_baselines(
    df: DataFrame,
    lookback_hours: int,
    min_history_points: int,
) -> DataFrame:
    lookback_seconds = lookback_hours * 3600

    trailing_window = (
        Window.partitionBy("city")
        .orderBy("window_start_epoch")
        .rangeBetween(-lookback_seconds, -1)
    )

    baseline_df = df
    for metric in DEFAULT_METRICS:
        baseline_df = add_metric_baselines(
            baseline_df,
            metric_col=metric,
            base_window=trailing_window,
            min_history_points=min_history_points,
        )

    baseline_df = (
        baseline_df.withColumn(
            "baseline_history_points",
            F.least(*[F.col(f"{metric}_history_points") for metric in DEFAULT_METRICS]),
        )
        .withColumn(
            "baseline_is_valid",
            F.col("baseline_history_points") >= F.lit(min_history_points),
        )
        .withColumn("baseline_window_hours", F.lit(lookback_hours))
        .withColumn("baseline_min_history_points", F.lit(min_history_points))
        .withColumn("source", F.lit("open311"))
        .withColumn("baseline_method", F.lit("trailing_time_window"))
        .withColumn("baseline_version", F.lit("v1"))
        .withColumn("baseline_computed_at_utc", F.current_timestamp())
    )

    return baseline_df


def select_output_columns(df: DataFrame) -> DataFrame:
    out = df

    # Preserve ingest partitions from upstream if they exist.
    # Fallback to deriving them from window_start only for local/dev cases.
    if "ingest_year" not in out.columns:
        out = out.withColumn("ingest_year", F.date_format("window_start_ts", "yyyy"))
    if "ingest_month" not in out.columns:
        out = out.withColumn("ingest_month", F.date_format("window_start_ts", "MM"))
    if "ingest_day" not in out.columns:
        out = out.withColumn("ingest_day", F.date_format("window_start_ts", "dd"))

    # Avoid duplicate/ambiguous window_start columns.
    out = out.drop("window_start").withColumn("window_start", F.col("window_start_ts")).drop("window_start_ts")

    final_cols = [
        "source",
        "city",
        "ingest_year",
        "ingest_month",
        "ingest_day",
        "window_start",
        "total_requests",
        "total_requests_mean",
        "total_requests_std",
        "total_requests_history_points",
        "total_requests_baseline_valid",
        "total_requests_zscore",
        "infrastructure_requests",
        "infrastructure_requests_mean",
        "infrastructure_requests_std",
        "infrastructure_requests_history_points",
        "infrastructure_requests_baseline_valid",
        "infrastructure_requests_zscore",
        "emergency_ratio",
        "emergency_ratio_mean",
        "emergency_ratio_std",
        "emergency_ratio_history_points",
        "emergency_ratio_baseline_valid",
        "emergency_ratio_zscore",
        "baseline_history_points",
        "baseline_is_valid",
        "baseline_window_hours",
        "baseline_min_history_points",
        "baseline_method",
        "baseline_version",
        "baseline_computed_at_utc",
    ]

    return out.select(*final_cols)


def validate_no_duplicate_city_windows(df: DataFrame) -> None:
    dupes = (
        df.groupBy("city", "window_start")
        .count()
        .filter(F.col("count") > 1)
    )

    if dupes.limit(1).count() > 0:
        sample = dupes.limit(10).collect()
        raise RuntimeError(
            f"Duplicate baseline rows found for (city, window_start). Sample: {sample}"
        )


def maybe_repartition_for_output(df: DataFrame, output_partitions: int) -> DataFrame:
    if output_partitions and output_partitions > 0:
        return df.coalesce(output_partitions)
    return df


def write_partitioned(df: DataFrame, output_root: str, output_partitions: int) -> None:
    out_df = maybe_repartition_for_output(df, output_partitions)
    (
        out_df.write.mode("overwrite")
        .partitionBy("ingest_year", "ingest_month", "ingest_day", "city")
        .parquet(output_root)
    )


def main() -> None:
    args = parse_args()

    validate_s3a_path(args.input_root, "--input-root")
    validate_s3a_path(args.output_root, "--output-root")

    spark = build_spark(
        app_name=args.app_name,
        master=args.spark_master,
        shuffle_partitions=args.shuffle_partitions,
    )

    prepared_df = None
    output_df = None

    try:
        agg_df = load_hourly_aggregates(spark, args.input_root)

        prepared_df = prepare_hourly_dataframe(agg_df).persist(StorageLevel.MEMORY_AND_DISK)

        baseline_df = compute_hourly_baselines(
            prepared_df,
            lookback_hours=args.lookback_hours,
            min_history_points=args.min_history_points,
        )

        output_df = select_output_columns(baseline_df).persist(StorageLevel.MEMORY_AND_DISK)

        validate_no_duplicate_city_windows(output_df)

        row_count = output_df.count()
        valid_count = output_df.filter(F.col("baseline_is_valid") == True).count()

        output_df.select(
            "city",
            "window_start",
            "total_requests_history_points",
            "infrastructure_requests_history_points",
            "emergency_ratio_history_points",
            "baseline_history_points",
            "baseline_is_valid"
        ).orderBy("window_start").show(50, truncate=False)

        write_partitioned(output_df, args.output_root, args.output_partitions)

        print("Open311 hourly baselines complete")
        print(f"Spark master                    : {args.spark_master}")
        print(f"Input root                      : {args.input_root}")
        print(f"Output root                     : {args.output_root}")
        print(f"Lookback hours                  : {args.lookback_hours}")
        print(f"Minimum history points          : {args.min_history_points}")
        print(f"Output row count                : {row_count}")
        print(f"Rows with valid baseline        : {valid_count}")

    finally:
        if output_df is not None:
            output_df.unpersist()
        if prepared_df is not None:
            prepared_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()