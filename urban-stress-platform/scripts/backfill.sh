#!/usr/bin/env bash
# backfill.sh — Run the ETL pipeline for each of the past N days
# Usage:  bash scripts/backfill.sh [DAYS]
# Default: 30 days back from yesterday.
#
# ETL jobs (bronze → silver → scoring → composite) run per-date against
# data already in RDS from the Lambda collectors.
#
# GdeltCollector runs ONCE at the end in scan mode — it walks back DAYS days
# and downloads every file GDELT currently has (~last 14 days). No per-date
# 404 failures. Null sentinel rows are written for dates outside GDELT's window.
set -euo pipefail

REGION="us-east-1"
DAYS="${1:-30}"

ETL_JOBS=(
  "urban-stress-bronzeetl"
  "urban-stress-silveretl"
  "urban-stress-signalscore"
  "urban-stress-compositescore"
)

wait_for_job() {
  local JOB_NAME="$1"
  local RUN_ID="$2"
  echo "    Waiting for ${JOB_NAME} (${RUN_ID})..."
  while true; do
    STATE=$(aws glue get-job-run \
      --job-name "$JOB_NAME" \
      --run-id "$RUN_ID" \
      --region "$REGION" \
      --query 'JobRun.JobRunState' \
      --output text)
    case "$STATE" in
      SUCCEEDED)
        echo "    ✅ ${JOB_NAME} SUCCEEDED"
        return 0
        ;;
      FAILED|ERROR|TIMEOUT)
        echo "    ❌ ${JOB_NAME} ${STATE}"
        aws glue get-job-run \
          --job-name "$JOB_NAME" --run-id "$RUN_ID" \
          --region "$REGION" \
          --query 'JobRun.ErrorMessage' --output text
        return 1
        ;;
      RUNNING|STARTING|STOPPING)
        sleep 15
        ;;
      *)
        echo "    ⚠️  Unknown state: ${STATE} — waiting..."
        sleep 15
        ;;
    esac
  done
}

echo "=== Urban Stress Backfill — ${DAYS} days ==="
echo ""

# ── Phase 1: ETL per date ────────────────────────────────────────────────────
for (( i=DAYS; i>=1; i-- )); do
  TARGET=$(date -v-${i}d +%Y-%m-%d 2>/dev/null \
           || date -d "${i} days ago" +%Y-%m-%d)

  echo "--- Date: ${TARGET} ---"

  for JOB in "${ETL_JOBS[@]}"; do
    echo "  Starting ${JOB} for ${TARGET}..."
    RUN_ID=$(aws glue start-job-run \
      --job-name "$JOB" \
      --region "$REGION" \
      --arguments "{\"--TARGET_DATE\":\"${TARGET}\"}" \
      --query 'JobRunId' \
      --output text)
    wait_for_job "$JOB" "$RUN_ID"
  done

  echo ""
done

# ── Phase 2: GDELT scan — one run pulls all available history ────────────────
echo "--- GDELT scan mode (last ${DAYS} days, pulls all available) ---"
RUN_ID=$(aws glue start-job-run \
  --job-name "urban-stress-gdeltcollector" \
  --region "$REGION" \
  --arguments "{\"--SCAN_DAYS\":\"${DAYS}\"}" \
  --query 'JobRunId' \
  --output text)
wait_for_job "urban-stress-gdeltcollector" "$RUN_ID"

echo ""
echo "=== Backfill complete ==="
