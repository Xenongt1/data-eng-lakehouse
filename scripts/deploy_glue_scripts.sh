#!/usr/bin/env bash
# Sync the PySpark Glue job scripts to s3://<bucket>/scripts/.
# The Glue job definitions in terraform/glue.tf reference
# s3://<bucket>/scripts/<script>.py as their script_location, so this is what
# wires the code to the running job.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
BUCKET="$(cd "$REPO_ROOT/terraform" && terraform output -raw bucket_name)"

echo "Syncing $REPO_ROOT/glue_jobs/ → s3://$BUCKET/scripts/"
aws s3 sync \
  "$REPO_ROOT/glue_jobs/" \
  "s3://$BUCKET/scripts/" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc" \
  --delete

echo "Done."
