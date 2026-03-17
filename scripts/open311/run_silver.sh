#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [run_silver] $*"
}

usage() {
  cat <<EOF
Usage:
  bash scripts/open311/run_silver.sh \
    --bucket <s3_bucket> \
    --city <city> \
    --ingest-date <YYYY-MM-DD> \
    --batch-id <batch_id> \
    --mapping-file <local_csv_path> \
    [--bronze-s3-file <s3_uri>] \
    [--output-s3-root <s3_uri>] \
    [--repo-root <path>] \
    [--env-file <path>] \
    [--python-bin <python_bin>]

Required:
  --bucket           S3 bucket name
  --city             City name
  --ingest-date      Ingest date used in Bronze partition
  --batch-id         Unique batch identifier
  --mapping-file     Local path to category mapping CSV

Optional:
  --bronze-s3-file   Full Bronze JSONL file S3 URI. If omitted, derived automatically.
  --output-s3-root   Silver S3 root. Default: s3://<bucket>/silver/open311
  --repo-root        Project root path. Defaults to auto-detected repo root.
  --env-file         Shell env file to source before running.
  --python-bin       Python executable to use. Default: python3
EOF
}

BUCKET=""
CITY=""
INGEST_DATE=""
BATCH_ID=""
MAPPING_FILE=""
BRONZE_S3_FILE=""
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
    --mapping-file)
      MAPPING_FILE="${2:-}"
      shift 2
      ;;
    --bronze-s3-file)
      BRONZE_S3_FILE="${2:-}"
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

if [[ -z "$BUCKET" || -z "$CITY" || -z "$INGEST_DATE" || -z "$BATCH_ID" || -z "$MAPPING_FILE" ]]; then
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

if [[ ! -f "$MAPPING_FILE" ]]; then
  echo "Error: mapping file not found: $MAPPING_FILE" >&2
  exit 1
fi

if [[ -z "$BRONZE_S3_FILE" ]]; then
  YEAR="${INGEST_DATE:0:4}"
  MONTH="${INGEST_DATE:5:2}"
  DAY="${INGEST_DATE:8:2}"

  BRONZE_S3_FILE="s3://${BUCKET}/bronze/open311/year=${YEAR}/month=${MONTH}/day=${DAY}/city=${CITY}/batch_id=${BATCH_ID}/part-00001.jsonl"
fi

if [[ -z "$OUTPUT_S3_ROOT" ]]; then
  OUTPUT_S3_ROOT="s3://${BUCKET}/silver/open311"
fi

log "Starting Open311 Silver ETL"
log "Repo root: $REPO_ROOT"
log "Python bin: $PYTHON_BIN"
log "Bucket: $BUCKET"
log "City: $CITY"
log "Ingest date: $INGEST_DATE"
log "Batch ID: $BATCH_ID"
log "Mapping file: $MAPPING_FILE"
log "Bronze S3 file: $BRONZE_S3_FILE"
log "Output S3 root: $OUTPUT_S3_ROOT"
log "Hostname: $(hostname)"

log "Executing: $PYTHON_BIN etl/open311/silver_open311.py --bronze-s3-file $BRONZE_S3_FILE --mapping-file $MAPPING_FILE --output-s3-root $OUTPUT_S3_ROOT"

"$PYTHON_BIN" etl/open311/silver_open311.py \
  --bronze-s3-file "$BRONZE_S3_FILE" \
  --mapping-file "$MAPPING_FILE" \
  --output-s3-root "$OUTPUT_S3_ROOT"

log "Open311 Silver ETL completed successfully"