#!/usr/bin/env bash
# End-to-end ingest of the 30 daily CSVs (plus the products dimension).
#
# Strategy:
#   1. products.csv  → raw/products/   (wait for SFN to finish)
#   2. 15 orders CSVs → raw/orders/    (uploaded back-to-back; wait for all)
#   3. 15 order_items CSVs → raw/order_items/  (depends on (2) being durable)
#
# The wait between (2) and (3) is critical: order_items joins against the
# orders Delta table for referential integrity, so orders MUST be persisted
# in the silver zone before order_items runs.
#
# Re-runnable: every Glue job ends in MERGE INTO, so a second run is a no-op
# on existing keys.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
BUCKET="$(cd "$REPO_ROOT/terraform" && terraform output -raw bucket_name)"
SM_ARN="$(cd "$REPO_ROOT/terraform" && terraform output -raw state_machine_arn)"
CSV_DIR="$REPO_ROOT/Data/daily_csvs"

DATES=(
  2025-04-01 2025-04-02 2025-04-03 2025-04-04 2025-04-05
  2025-04-06 2025-04-07 2025-04-08 2025-04-09 2025-04-10
  2025-04-11 2025-04-12 2025-04-13 2025-04-14 2025-04-15
)

upload_one() {
  local localfile="$1"
  local key="$2"
  echo "  → s3://$BUCKET/$key"
  aws s3 cp "$localfile" "s3://$BUCKET/$key" --no-progress >/dev/null
}

wait_for_no_running() {
  local label="$1"
  local timeout_s="${2:-900}"
  local sleep_s=15
  local elapsed=0
  echo "Waiting for $label executions to finish (timeout ${timeout_s}s)..."
  while true; do
    local running
    running=$(aws stepfunctions list-executions \
      --state-machine-arn "$SM_ARN" \
      --status-filter RUNNING \
      --max-results 50 \
      --query "length(executions)" \
      --output text)
    if [ "$running" = "0" ]; then
      echo "  ✓ $label done (waited ${elapsed}s)"
      return 0
    fi
    if [ "$elapsed" -ge "$timeout_s" ]; then
      echo "  ✗ timeout — $running still RUNNING after ${timeout_s}s"
      return 1
    fi
    echo "  ($elapsed s) $running still RUNNING..."
    sleep "$sleep_s"
    elapsed=$((elapsed + sleep_s))
  done
}

upload_dataset_split() {
  # Upload day 1 by itself, wait for it to land in the Delta table. The first
  # write CREATES the table; doing it alone avoids two concurrent jobs each
  # trying to bootstrap the same path. Then days 2-15 in parallel — by then
  # the table exists, partition_filter narrows the MERGE scope per day.
  local dataset="$1"
  local first="${DATES[0]}"

  echo "  day 1 alone (creates the Delta table if needed)…"
  upload_one "$CSV_DIR/${dataset}_${first}.csv" "raw/${dataset}/${dataset}_${first}.csv"
  wait_for_no_running "${dataset} day 1"

  echo "  days 2-15 in parallel…"
  for d in "${DATES[@]:1}"; do
    upload_one "$CSV_DIR/${dataset}_${d}.csv" "raw/${dataset}/${dataset}_${d}.csv"
    sleep 2
  done
  wait_for_no_running "${dataset} days 2-15"
}

echo "=== 1. products ==="
upload_one "$CSV_DIR/products.csv" "raw/products/products.csv"
wait_for_no_running "products"

echo
echo "=== 2. orders (15 days) ==="
upload_dataset_split "orders"

echo
echo "=== 3. order_items (15 days) ==="
upload_dataset_split "order_items"

echo
echo "Refreshing Glue catalog…"
aws glue start-crawler --name lakehouse-ecommerce-crawler >/dev/null 2>&1 || true
echo "All uploads complete."
