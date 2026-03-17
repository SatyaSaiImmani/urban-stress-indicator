#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [run_bronze] $*"
}

usage() {
  cat <<EOF
Usage:
  bash scripts/open311/run_bronze.sh \
    --bucket <s3_bucket> \
    --city <city> \
    --ingest-date <YYYY-MM-DD> \
    --batch-id <batch_id> \
    [--raw-s3-dir <s3_uri>] \
    [--output-s3-root <s3_uri>] \
    [--repo-root <path>] \
    [--env-file <path>] \
    [--python-bin <python_bin>]

Required:
  --bucket           S3 bucket name
  --city             City name
  --ingest-date      Ingest date used in raw path partition
  --batch-id         Unique batch identifier

Optional:
  --raw-s3-dir       Full raw S3 prefix. If omitted, it is derived automatically.
  --output-s3-root   Bronze S3 root. Default: s3://<bucket>/bronze/open311
  --repo-root        Project root path. Defaults to auto-detected repo root.
  --env-file         Shell env file to source before running.
  --python-bin       Python executable to use. Default: python3
EOF
}

BUCKET=""
CITY=""
INGEST_DATE=""
BATCH_ID=""
RAW_S3_DIR=""
OUTPUT_S3_ROOT=""
REPO_ROOT=""
ENV_FILE=""
PYTHON_BIN="python3"

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
    --raw-s3-dir)
      RAW_S3_DIR="${2:-}"
      shift 2
      ;;
    --output-s3-root)
      OUTPUT_S3_ROOT="${2:-}"
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
    --python-bin)
      PYTHON_BIN="${2:-}"
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

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: python executable not found: $PYTHON_BIN" >&2
  exit 1
fi

if [[ -z "$RAW_S3_DIR" ]]; then
  RAW_S3_DIR="s3://${BUCKET}/raw/open311/city=${CITY}/ingest_date=${INGEST_DATE}/batch_id=${BATCH_ID}"
fi

if [[ -z "$OUTPUT_S3_ROOT" ]]; then
  OUTPUT_S3_ROOT="s3://${BUCKET}/bronze/open311"
fi

log "Starting Open311 Bronze ETL"
log "Repo root: $REPO_ROOT"
log "Python bin: $PYTHON_BIN"
log "Bucket: $BUCKET"
log "City: $CITY"
log "Ingest date: $INGEST_DATE"
log "Batch ID: $BATCH_ID"
log "Raw S3 dir: $RAW_S3_DIR"
log "Output S3 root: $OUTPUT_S3_ROOT"
log "Hostname: $(hostname)"

log "Executing: $PYTHON_BIN etl/open311/bronze_open311.py --raw-s3-dir $RAW_S3_DIR --output-s3-root $OUTPUT_S3_ROOT"

"$PYTHON_BIN" etl/open311/bronze_open311.py \
  --raw-s3-dir "$RAW_S3_DIR" \
  --output-s3-root "$OUTPUT_S3_ROOT"

log "Open311 Bronze ETL completed successfully"