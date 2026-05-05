#!/usr/bin/env bash
# build_and_upload_lambdas.sh
# Packages each Lambda inside a linux/amd64 Docker container matching the
# Lambda runtime — guarantees correct binary wheels (numpy, psycopg2, etc.)
# Run from the project root: bash scripts/build_and_upload_lambdas.sh
set -euo pipefail

ACCOUNT="836734770581"
REGION="us-east-1"
ARTIFACTS_BUCKET="urban-stress-data-${ACCOUNT}"   # single bucket in new stack
BUILD_DIR="/tmp/urban-stress-lambda-build"

echo "=== Creating artifacts bucket (if not exists) ==="
aws s3api create-bucket \
  --bucket "$ARTIFACTS_BUCKET" \
  --region "$REGION" 2>/dev/null || echo "Bucket already exists"

build_lambda() {
  local NAME=$1                        # e.g. collector_open311
  local S3_NAME="${2:-${NAME}}"        # optional override for S3 key, e.g. collector_open311_v2
  local SRC="lambdas/${NAME}"
  local DEST="${BUILD_DIR}/${NAME}"

  echo "--- Building ${NAME} → s3 key: ${S3_NAME} (linux/amd64 via Docker) ---"
  rm -rf "$DEST" && mkdir -p "$DEST"

  # Copy handler + shared helpers into build dir
  cp "${SRC}/handler.py" "$DEST/"
  cp lambdas/shared/*.py "$DEST/"

  # Copy requirements if present
  [ -f "${SRC}/requirements.txt" ] && cp "${SRC}/requirements.txt" "$DEST/"

  # Install deps inside the official Lambda python3.11 image (linux/amd64)
  if [ -f "${DEST}/requirements.txt" ]; then
    # Use the exact Lambda runtime image with entrypoint cleared.
    # Install gcc first so packages like numpy that need a C compiler can build.
    docker run --rm --platform linux/amd64 \
      --entrypoint "" \
      -v "${DEST}:/pkg" \
      public.ecr.aws/lambda/python:3.11 \
      /bin/bash -c "pip install -r /pkg/requirements.txt --target /pkg --quiet --only-binary=:all:"
  fi

  # Zip the package
  local ZIP="${BUILD_DIR}/${S3_NAME}.zip"
  (cd "$DEST" && zip -r "$ZIP" . -x "*.pyc" -x "__pycache__/*" > /dev/null)

  # Upload to S3
  aws s3 cp "$ZIP" "s3://${ARTIFACTS_BUCKET}/lambdas/${S3_NAME}.zip"
  echo "    Uploaded s3://${ARTIFACTS_BUCKET}/lambdas/${S3_NAME}.zip"
}

# _v2 keys force CloudFormation to detect the code change and call UpdateFunctionCode
# via LabRole (voclabs is denied lambda:UpdateFunctionCode directly)
build_lambda "collector_open311" "collector_open311_v2"
build_lambda "collector_noaa"    "collector_noaa_v2"
build_lambda "collector_gdelt"
build_lambda "verdict_gen"
build_lambda "log_run"

# ── Glue scripts ────────────────────────────────────────────────────────────
# Plain .py files — no Docker build needed. Uploaded to the bronze bucket
# (not artifacts) under glue-scripts/ because etl_stack.py ScriptLocation
# points there: s3://{bronze_bucket}/glue-scripts/{script}.py
# Must be re-uploaded after any edit; CDK deploy only updates the job
# definition, not the script content in S3.
echo ""
echo "=== Uploading Glue scripts to s3://${ARTIFACTS_BUCKET}/glue-scripts/ ==="
for script in glue/*.py; do
  aws s3 cp "$script" "s3://${ARTIFACTS_BUCKET}/glue-scripts/" --region "$REGION"
  echo "    Uploaded $script"
done

echo ""
echo "=== Done. ==="
echo "  Lambda zips : s3://${ARTIFACTS_BUCKET}/lambdas/"
echo "  Glue scripts: s3://${ARTIFACTS_BUCKET}/glue-scripts/"
