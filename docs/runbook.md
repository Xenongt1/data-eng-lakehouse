# Lakehouse runbook

Operator-facing guide to deploy, ingest, query, debug, and tear down the
e-commerce lakehouse on AWS.

---

## 0. Prerequisites

- AWS CLI v2 with a profile that has admin in the target account
- Terraform >= 1.5
- Python 3.11 + `pandas`, `openpyxl` (for the local xlsx → CSV split)

```bash
export AWS_PROFILE=lakehouse          # or whatever profile name you use
aws sts get-caller-identity           # sanity-check the account
```

The Terraform default `region = "us-east-1"`. Override via
`terraform apply -var region=eu-west-1` if needed.

---

## 1. Deploy from scratch

```bash
# 1a. Provision infra (S3, IAM, Glue jobs, Glue DB, crawler,
#     Step Functions, EventBridge, Athena workgroup).
cd terraform
terraform init
terraform plan -out=tfplan
terraform apply -auto-approve tfplan
cd ..

# 1b. Sync the PySpark scripts to s3://<bucket>/scripts/. The Glue jobs
#     read their entry-point from there.
bash scripts/deploy_glue_scripts.sh
```

After both steps, capture the bucket name for convenience:

```bash
export BUCKET=$(cd terraform && terraform output -raw bucket_name)
```

---

## 2. Redeploy a PySpark script

Just re-sync — no `terraform apply` needed unless `default_arguments`
or worker config changed.

```bash
bash scripts/deploy_glue_scripts.sh
```

The next Glue job run will pick up the new script automatically.

---

## 3. Ingest a new day's data

The pipeline is fully event-driven: drop a file under `raw/<dataset>/` and
EventBridge fires the Step Functions state machine. Order matters because
`process_order_items` joins against the orders and products Delta tables
that already live in `processed/`.

For a new day `YYYY-MM-DD`:

```bash
# 1. (Once) products dimension — only needed if you've never run before
aws s3 cp Data/daily_csvs/products.csv "s3://$BUCKET/raw/products/products.csv"

# 2. Orders for the day — must finish before order_items
aws s3 cp "Data/daily_csvs/orders_YYYY-MM-DD.csv" \
          "s3://$BUCKET/raw/orders/orders_YYYY-MM-DD.csv"

# 3. Wait until the orders execution succeeds, THEN upload order_items
aws s3 cp "Data/daily_csvs/order_items_YYYY-MM-DD.csv" \
          "s3://$BUCKET/raw/order_items/order_items_YYYY-MM-DD.csv"
```

`scripts/upload_raw_data.sh` automates this for all 15 sample days.

---

## 4. Watch a pipeline run

```bash
SM_ARN=$(cd terraform && terraform output -raw state_machine_arn)

# List recent executions
aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --max-results 10

# Pick an execution arn and watch it
aws stepfunctions describe-execution --execution-arn <exec-arn>

# See the per-state history (which state failed, when)
aws stepfunctions get-execution-history --execution-arn <exec-arn> \
    --query 'events[?type==`TaskFailed` || type==`ExecutionFailed`]'
```

In the AWS console, **Step Functions → State machines →
lakehouse-ecommerce-ingest → Executions** shows the full visual graph.

---

## 5. Query in Athena

In the console: **Athena → Query editor → Workgroup: `lakehouse_wg` →
Database: `lakehouse_dwh`**. The default workgroup will NOT work — its result
location isn't configured.

Sample queries live in `athena/queries.sql`.

From the CLI:

```bash
aws athena start-query-execution \
    --query-string "SELECT count(*) FROM lakehouse_dwh.orders" \
    --work-group lakehouse_wg \
    --query-execution-context Database=lakehouse_dwh

# Then with the returned QueryExecutionId:
aws athena get-query-results --query-execution-id <qid>
```

Athena engine v3 reads native Delta tables directly — no manifest refresh
between writes and reads.

---

## 6. Debug a failed Glue run

1. **Find the run.** `aws glue get-job-runs --job-name lakehouse-ecommerce-process_orders`
   — look for `JobRunState != SUCCEEDED`.
2. **Get the CloudWatch link.** Run-level stderr is at
   `/aws-glue/jobs/error/<JobRunId>`; stdout (where our `print()` lines land) is at
   `/aws-glue/jobs/output/<JobRunId>`.
   ```bash
   aws logs tail /aws-glue/jobs/error --follow \
       --log-stream-names <JobRunId>
   ```
3. **If the row failed validation, look in quarantine.**
   ```bash
   aws s3 ls "s3://$BUCKET/quarantine/<dataset>/" --recursive
   ```
   Each run writes Parquet with `rejection_reason` set to the first failing
   rule (see `docs/schema_design.md` §6.4 for the rule names).
4. **If the file processing itself blew up,** Step Functions writes a JSON
   marker at `quarantine/_failed/<original-key>.json` describing the error.

---

## 7. Backfill / re-run a single file

Because every write is a `MERGE INTO`, re-running a file is idempotent — the
existing rows get matched on primary key and updated in place. To force a
re-run after fixing a bug:

```bash
aws s3 cp s3://$BUCKET/archive/orders/orders_2025-04-03.csv \
          s3://$BUCKET/raw/orders/orders_2025-04-03.csv
```

That object-created event triggers Step Functions again.

---

## 8. Refresh the catalog manually

The Step Functions state machine runs the crawler after each Glue job
succeeds. If you need to run it manually (e.g., after a schema evolution):

```bash
aws glue start-crawler --name lakehouse-ecommerce-crawler
```

---

## 9. Tear down

**Destructive.** This wipes the bucket and removes every AWS resource. Do
NOT run unless you mean it.

```bash
# 1. Empty the bucket (Terraform won't delete a non-empty bucket without
#    force_destroy=true, which we've intentionally left off).
aws s3 rm "s3://$BUCKET" --recursive

# 2. Destroy everything else
cd terraform
terraform destroy
```

If `terraform destroy` complains about the bucket, the `aws s3 rm` step
missed something — check for delete markers from versioning:

```bash
aws s3api list-object-versions --bucket "$BUCKET" \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
    > /tmp/versions.json
aws s3api delete-objects --bucket "$BUCKET" --delete file:///tmp/versions.json
```
