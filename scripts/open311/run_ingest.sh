#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [run_ingest] $*"
}

usage() {
  cat <<EOF
Usage:
  bash scripts/open311/run_ingest.sh \
    --city <city> \
    --updated-after <iso_timestamp> \
    --updated-before <iso_timestamp> \
    --bucket <s3_bucket> \
    --batch-id <batch_id> \
    [--repo-root <path>] \
    [--env-file <path>] \
    [--python-bin <python_bin>]

Required:
  --city             City name configured in collectors/open311/cities.yaml
  --updated-after    ISO timestamp lower bound
  --updated-before   ISO timestamp upper bound
  --bucket           Target S3 bucket
  --batch-id         Unique batch identifier

Optional:
  --repo-root        Project root path. Defaults to auto-detected repo root.
  --env-file         Shell env file to source before running.
  --python-bin       Python executable to use. Default: python3
EOF
}

CITY=""
UPDATED_AFTER=""
UPDATED_BEFORE=""
BUCKET=""
BATCH_ID=""
REPO_ROOT=""
ENV_FILE=""
PYTHON_BIN="python3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --city)
      CITY="${2:-}"
      shift 2
      ;;
    --updated-after)
      UPDATED_AFTER="${2:-}"
      shift 2
      ;;
    --updated-before)
      UPDATED_BEFORE="${2:-}"
      shift 2
      ;;
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --batch-id)
      BATCH_ID="${2:-}"
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

if [[ -z "$CITY" || -z "$UPDATED_AFTER" || -z "$UPDATED_BEFORE" || -z "$BUCKET" || -z "$BATCH_ID" ]]; then
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

log "Starting Open311 ingestion"
log "Repo root: $REPO_ROOT"
log "Python bin: $PYTHON_BIN"
log "City: $CITY"
log "Updated after: $UPDATED_AFTER"
log "Updated before: $UPDATED_BEFORE"
log "Bucket: $BUCKET"
log "Batch ID: $BATCH_ID"
log "Hostname: $(hostname)"

log "Executing: $PYTHON_BIN collectors/open311/runner.py --city $CITY --updated-after $UPDATED_AFTER --updated-before $UPDATED_BEFORE --bucket $BUCKET --batch-id $BATCH_ID"

"$PYTHON_BIN" collectors/open311/runner.py \
  --city "$CITY" \
  --updated-after "$UPDATED_AFTER" \
  --updated-before "$UPDATED_BEFORE" \
  --bucket "$BUCKET" \
  --batch-id "$BATCH_ID"

log "Open311 ingestion completed successfully"