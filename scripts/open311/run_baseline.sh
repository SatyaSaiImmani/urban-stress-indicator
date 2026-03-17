#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [run_baseline] $*"
}

usage() {
  cat <<EOF
Usage:
  bash scripts/open311/run_baseline.sh \
    --bucket <s3_bucket> \
    [--input-root <s3a_uri>] \
    [--output-root <s3a_uri>] \
    [--lookback-hours <int>] \
    [--min-history-points <int>] \
    [--repo-root <path>] \
    [--env-file <path>] \
    [--spark-submit-bin <path>] \
    [--spark-script <path>] \
    [--app-name <name>]

Required:
  --bucket               S3 bucket name

Optional:
  --input-root           Default: s3a://<bucket>/aggregates/open311/hourly/
  --output-root          Default: s3a://<bucket>/baselines/open311/hourly/
  --lookback-hours       Default: 168
  --min-history-points   Default: 24
  --repo-root            Project root path
  --env-file             Shell env file
  --spark-submit-bin     Default: /opt/spark/bin/spark-submit
  --spark-script         Default: etl/open311/baselines_spark.py
  --app-name             Default: Open311HourlyBaselinesSpark
EOF
}

BUCKET=""
INPUT_ROOT=""
OUTPUT_ROOT=""
LOOKBACK_HOURS="168"
MIN_HISTORY_POINTS="24"
REPO_ROOT=""
ENV_FILE=""
SPARK_SUBMIT_BIN="/opt/spark/bin/spark-submit"
SPARK_SCRIPT="etl/open311/baselines_spark.py"
APP_NAME="Open311HourlyBaselinesSpark"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --input-root)
      INPUT_ROOT="${2:-}"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --lookback-hours)
      LOOKBACK_HOURS="${2:-}"
      shift 2
      ;;
    --min-history-points)
      MIN_HISTORY_POINTS="${2:-}"
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

if [[ -z "$BUCKET" ]]; then
  echo "Error: --bucket is required" >&2
  usage
  exit 1
fi

if [[ -z "$REPO_ROOT" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
fi

cd "$REPO_ROOT"

if [[ -n "$ENV_FILE" ]]; then
  log "Sourcing env file: $ENV_FILE"
  source "$ENV_FILE"
fi

if [[ -z "$INPUT_ROOT" ]]; then
  INPUT_ROOT="s3a://${BUCKET}/aggregates/open311/hourly/"
fi

if [[ -z "$OUTPUT_ROOT" ]]; then
  OUTPUT_ROOT="s3a://${BUCKET}/baselines/open311/hourly/"
fi

log "Starting Open311 baseline Spark job"
log "Bucket: $BUCKET"
log "Input root: $INPUT_ROOT"
log "Output root: $OUTPUT_ROOT"
log "Lookback hours: $LOOKBACK_HOURS"
log "Min history points: $MIN_HISTORY_POINTS"
log "Spark script: $SPARK_SCRIPT"
log "Hostname: $(hostname)"

"$SPARK_SUBMIT_BIN" \
  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  "$SPARK_SCRIPT" \
  --input-root "$INPUT_ROOT" \
  --output-root "$OUTPUT_ROOT" \
  --lookback-hours "$LOOKBACK_HOURS" \
  --min-history-points "$MIN_HISTORY_POINTS" \
  --app-name "$APP_NAME"

log "Open311 baseline job completed successfully"