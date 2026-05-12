#!/usr/bin/env bash
# Container entry script: refresh raw.* from WGE, then rebuild dbt models.
# Run by ofelia on the 1st of each month (or manually via `docker run`).

set -euo pipefail

echo "================================================================="
echo "  Streetlight ingest + dbt run"
echo "  Started:    $(date -u '+%Y-%m-%dT%H:%M:%SZ') UTC"
echo "  WGE source: ${WGE_SERVER:-<unset>} / ${WGE_DATABASE:-WGE}"
echo "  Warehouse:  ${WAREHOUSE_SERVER:-<unset>} / ${WAREHOUSE_DATABASE:-<unset>}"
echo "================================================================="
echo

echo "==> Step 1/4: WGE -> raw.* via ingest/sync_wge.py"
python /app/ingest/sync_wge.py
echo

cd /app/dbt

echo "==> Step 2/4: dbt seed (loads rate_wattage + hours_of_darkness_daily)"
dbt seed
echo

echo "==> Step 3/4: dbt run (staging -> intermediate -> marts)"
dbt run
echo

echo "==> Step 4/4: dbt test"
dbt test
echo

echo "================================================================="
echo "  Completed:  $(date -u '+%Y-%m-%dT%H:%M:%SZ') UTC"
echo "================================================================="
