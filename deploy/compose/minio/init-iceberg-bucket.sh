#!/bin/sh
set -eu

WAREHOUSE="${ICEBERG_CATALOG_WAREHOUSE:-s3://telecom-mart/}"
ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"

case "$WAREHOUSE" in
  s3://*)
    ;;
  *)
    echo "ERROR: ICEBERG_CATALOG_WAREHOUSE must start with s3://, got: $WAREHOUSE" >&2
    exit 1
    ;;
esac

BUCKET="${WAREHOUSE#s3://}"
BUCKET="${BUCKET%%/*}"

if [ -z "$BUCKET" ]; then
  echo "ERROR: Unable to parse bucket from ICEBERG_CATALOG_WAREHOUSE=$WAREHOUSE" >&2
  exit 1
fi

mc alias set local "$ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
mc mb --ignore-existing "local/$BUCKET"
mc ls local
