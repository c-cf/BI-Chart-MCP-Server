#!/bin/sh
# Create the read-only role bi-chart-mcp connects as (demo password; change it for real use).
set -eu
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v role=bi_reader -v password="${BI_READER_PASSWORD:-bi_reader_demo}" \
  -v database="$POSTGRES_DB" -v schema=sales \
  -f /scripts/create_readonly_role.sql
