#!/bin/sh
set -eu
export PGPASSWORD="${POSTGRES_PASSWORD}"
exec psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1 -f /trino-iceberg-catalog-init.sql
