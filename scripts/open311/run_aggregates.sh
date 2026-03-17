#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [run_aggregate] $*"
}

usage() {
  cat <<EOF
Usage:
  bash scripts/open311/run_aggregate.sh \
    --bucket <s3_bucket> \
    --city <city> \
    --ingest-date <YYYY-MM-DD> \
    --batch-id <batch_id> \
    [--silver-file <s3a_uri>] \
    [--hourly-output-root <s3a_uri>] \
    [--daily-output-root <s3a_uri>] \
    [--category-output-root <s3a_uri>] \
    [--repo-root <path>] \
    [--env-file <path>] \
    [--spark-submit-bin <path>] \
    [--spark-script <path>] \
    [--app-name <name>]

Required:
  --bucket             S3 bucket name
  --city               City name
  --ingest-date        Ingest date used in Silver partition
  --batch-id           Unique batch identifier

Optional:
  --silver-file        Full Silver parquet S3A URI. If omitted, derived automatically.
  --hourly-output-root Hourly aggregate root. Default: s3a://<bucket>/aggregates/open311/hourly
  --daily-output-root  Daily aggregate root. Default: s3a://<bucket>/aggregates/open311/daily
  --category-output-root Category-hourly root. Default: s3a://<bucket>/aggregates/open311/category_hourly
  --repo-root          Project root path. Defaults to auto-detected repo root.
  --env-file           Shell env file to source before running.
  --spark-submit-bin   Spark submit executable. Default: /opt/spark/bin/spark-submit
  --spark-script       Aggregation script path relative to repo root.
                       Default: etl/open311/aggregate_open311_spark.py
  --app-name           Spark app name. Default: Open311AggregationSpark
EOF
}

BUCKET=""
CITY=""
INGEST_DATE=""
BATCH_ID=""
SILVER_FILE=""
HOURLY_OUTPUT_ROOT=""
DAILY_OUTPUT_ROOT=""
CATEGORY_OUTPUT_ROOT=""
REPO_ROOT=""
ENV_FILE=""
SPARK_SUBMIT_BIN="/opt/spark/bin/spark-submit"
SPARK_SCRIPT="etl/open311/aggregate_open311_spark.py"
APP_NAME="Open311AggregationSpark"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --city)
      CITY="${2:-}"
      shift 2
      ;;
    --ingest-date)
      INGEST_DATE="${2:-}"
      shift 2
      ;;
    --batch-id)
      BATCH_ID="${2:-}"
      shift 2
      ;;
    --silver-file)
      SILVER_FILE="${2:-}"
      shift 2
      ;;
    --hourly-output-root)
      HOURLY_OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --daily-output-root)
      DAILY_OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --category-output-root)
      CATEGORY_OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --repo-root)
      REPO_ROOT="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --spark-submit-bin)
      SPARK_SUBMIT_BIN="${2:-}"
      shift 2
      ;;
    --spark-script)
      SPARK_SCRIPT="${2:-}"
      shift 2
      ;;
    --app-name)
      APP_NAME="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$BUCKET" || -z "$CITY" || -z "$INGEST_DATE" || -z "$BATCH_ID" ]]; then
  echo "Error: missing required arguments." >&2
  usage
  exit 1
fi

if [[ -z "$REPO_ROOT" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
fi

if [[ ! -d "$REPO_ROOT" ]]; then
  echo "Error: repo root does not exist: $REPO_ROOT" >&2
  exit 1
fi

cd "$REPO_ROOT"

if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: env file not found: $ENV_FILE" >&2
    exit 1
  fi
  log "Sourcing env file: $ENV_FILE"
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [[ ! -x "$SPARK_SUBMIT_BIN" ]]; then
  echo "Error: spark-submit not found or not executable: $SPARK_SUBMIT_BIN" >&2
  exit 1
fi

if [[ ! -f "$SPARK_SCRIPT" ]]; then
  echo "Error: Spark script not found: $SPARK_SCRIPT" >&2
  exit 1
fi

YEAR="${INGEST_DATE:0:4}"
MONTH="${INGEST_DATE:5:2}"
DAY="${INGEST_DATE:8:2}"

if [[ -z "$SILVER_FILE" ]]; then
  SILVER_FILE="s3a://${BUCKET}/silver/open311/year=${YEAR}/month=${MONTH}/day=${DAY}/city=${CITY}/batch_id=${BATCH_ID}/part-00000.parquet"
fi

if [[ -z "$HOURLY_OUTPUT_ROOT" ]]; then
  HOURLY_OUTPUT_ROOT="s3a://${BUCKET}/aggregates/open311/hourly"
fi

if [[ -z "$DAILY_OUTPUT_ROOT" ]]; then
  DAILY_OUTPUT_ROOT="s3a://${BUCKET}/aggregates/open311/daily"
fi

if [[ -z "$CATEGORY_OUTPUT_ROOT" ]]; then
  CATEGORY_OUTPUT_ROOT="s3a://${BUCKET}/aggregates/open311/category_hourly"
fi

log "Starting Open311 aggregate Spark job"
log "Repo root: $REPO_ROOT"
log "Bucket: $BUCKET"
log "City: $CITY"
log "Ingest date: $INGEST_DATE"
log "Batch ID: $BATCH_ID"
log "Spark submit: $SPARK_SUBMIT_BIN"
log "Spark script: $SPARK_SCRIPT"
log "Silver file: $SILVER_FILE"
log "Hourly output root: $HOURLY_OUTPUT_ROOT"
log "Daily output root: $DAILY_OUTPUT_ROOT"
log "Category output root: $CATEGORY_OUTPUT_ROOT"
log "App name: $APP_NAME"
log "Hostname: $(hostname)"

log "Executing Spark aggregation job"

"$SPARK_SUBMIT_BIN" \
  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  "$SPARK_SCRIPT" \
  --silver-file "$SILVER_FILE" \
  --hourly-output-root "$HOURLY_OUTPUT_ROOT" \
  --daily-output-root "$DAILY_OUTPUT_ROOT" \
  --category-output-root "$CATEGORY_OUTPUT_ROOT" \
  --app-name "$APP_NAME"

log "Open311 aggregate Spark job completed successfully"