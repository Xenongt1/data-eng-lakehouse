# Lakehouse Architecture for E-Commerce Transactions

Production-grade Lakehouse on AWS. Raw e-commerce transaction CSVs land in S3,
are validated and deduplicated by AWS Glue PySpark jobs, and written into Delta
Lake tables in a curated zone. The Glue Data Catalog exposes the tables to
Amazon Athena for downstream analytics. AWS Step Functions orchestrates the
flow; GitHub Actions runs CI on every PR.

## Architecture

```
S3 raw/  ─▶ EventBridge ─▶ Step Functions ─▶ Glue PySpark + Delta
                                    │
                                    ├─▶ MERGE INTO  s3://.../processed/
                                    ├─▶ Glue Crawler ─▶ Glue Data Catalog ─▶ Athena
                                    └─▶ Archive raw to s3://.../archive/
```

## Datasets

| Dataset | Source | Partition | Dedup key |
|---|---|---|---|
| `products` | `Data/products.csv` (1,000 rows) | none | `product_id` |
| `orders` | `Data/orders_apr_2025.xlsx` (15 daily sheets) | `order_date` | `order_id` |
| `order_items` | `Data/order_items_apr_2025.xlsx` (15 daily sheets) | `order_date` | `id` |

Validation enforces non-null primary keys, parseable timestamps, non-negative
amounts, and referential integrity between `order_items → orders` and
`order_items → products`.

## Repo layout

| Path | Purpose |
|---|---|
| `terraform/` | All AWS infrastructure (S3, IAM, Glue, Step Functions, EventBridge, Athena) |
| `glue_jobs/` | PySpark jobs uploaded to S3 and run by Glue (`process_products`, `process_orders`, `process_order_items`) |
| `step_functions/` | State machine definition (ASL JSON) |
| `athena/` | Sample analytical queries |
| `scripts/` | Helper scripts (data prep, upload, deploy, local testing) |
| `tests/` | Pytest unit tests run by CI |
| `docs/` | Architecture diagram, schema design, runbook |
| `.github/workflows/` | GitHub Actions CI |

## Quick start

```bash
# 1. AWS auth (one time)
aws configure --profile lakehouse
export AWS_PROFILE=lakehouse

# 2. Provision
cd terraform
terraform init
terraform apply
cd ..

# 3. Preprocess data
python scripts/split_xlsx_to_daily_csv.py

# 4. Upload Glue scripts and trigger the pipeline
bash scripts/deploy_glue_scripts.sh
bash scripts/upload_raw_data.sh

# 5. Query
aws athena start-query-execution \
  --query-string "SELECT count(*) FROM lakehouse_dwh.orders" \
  --work-group lakehouse_wg \
  --result-configuration OutputLocation=s3://$(cd terraform && terraform output -raw bucket_name)/athena-results/
```

## CI

GitHub Actions runs `pytest` over the Glue transformation logic and validates
the Terraform on every PR to `main`. See `.github/workflows/ci.yml`.

## License

MIT.
