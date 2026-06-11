# Schema, partitioning, and validation design

This document captures the data contract for the lakehouse: Delta table
schemas, partitioning choices, deduplication keys, validation rules, and the
MERGE pattern used by all three Glue jobs. These are the rules the PySpark
jobs in `glue_jobs/` implement and the tests in `tests/` enforce.

---

## 1. Delta table schemas (explicit, not inferred)

Schemas are declared explicitly in PySpark rather than inferred from CSV so
that bad types fail loudly at ingest instead of silently flowing downstream.

### 1.1 `products` (small dimension, no partition)

| col            | type      | nullable | notes              |
|----------------|-----------|----------|--------------------|
| product_id     | INT       | no       | primary key        |
| department_id  | INT       | yes      |                    |
| department     | STRING    | yes      |                    |
| product_name   | STRING    | yes      |                    |
| ingested_at    | TIMESTAMP | no       | set by Glue job    |

### 1.2 `orders` (partitioned by `order_date`)

| col             | type           | nullable | notes              |
|-----------------|----------------|----------|--------------------|
| order_id        | BIGINT         | no       | primary key        |
| order_num       | INT            | yes      |                    |
| user_id         | BIGINT         | no       |                    |
| order_timestamp | TIMESTAMP      | no       |                    |
| total_amount    | DECIMAL(10,2)  | yes      |                    |
| order_date      | DATE           | no       | partition key      |
| ingested_at     | TIMESTAMP      | no       | set by Glue job    |

### 1.3 `order_items` (partitioned by `order_date`)

| col                    | type      | nullable | notes                              |
|------------------------|-----------|----------|------------------------------------|
| id                     | BIGINT    | no       | primary key                        |
| order_id               | BIGINT    | no       | FK → `orders.order_id`             |
| user_id                | BIGINT    | no       |                                    |
| product_id             | INT       | no       | FK → `products.product_id`         |
| days_since_prior_order | INT       | yes      |                                    |
| add_to_cart_order      | INT       | yes      |                                    |
| reordered              | BOOLEAN   | yes      |                                    |
| order_timestamp        | TIMESTAMP | no       |                                    |
| order_date             | DATE      | no       | partition key                      |
| ingested_at            | TIMESTAMP | no       | set by Glue job                    |

The source CSV column `date` is renamed to `order_date` on ingest — `date` is
a reserved-ish word in some SQL contexts and the new name is clearer.

---

## 2. Partitioning

- `orders` and `order_items`: partitioned by `order_date` (one Hive-style
  partition per day). 15 partitions for the April 2025 sample; scales
  linearly with new days.
- `products`: no partition. It's a small, low-churn dimension; partitioning
  would only add small-file overhead.

Partitioning by date is the obvious choice here because both fact tables
have a natural time axis and analytical queries (daily revenue, top
products per day, etc.) filter on it.

---

## 3. Dedup keys (used in the MERGE predicate)

| table       | dedup key   |
|-------------|-------------|
| products    | product_id  |
| orders      | order_id    |
| order_items | id          |

Within a single batch we dedupe with a row-number window:

```sql
row_number() OVER (PARTITION BY <dedup_key> ORDER BY ingested_at DESC)
```

and keep only rows where `rn = 1` before the MERGE. This guards against the
same primary key appearing twice in one file (e.g. an upstream retry).

---

## 4. Validation rules

Validation runs on the raw DataFrame after schema casting and before the
MERGE. Rules **fail the row, not the job**: rejected rows are written to
`s3://<bucket>/quarantine/<dataset>/<run_id>/` as Parquet with an added
`rejection_reason` column. Good rows continue down the pipeline.

This is the right shape because the alternative — failing the whole job on
one bad row — would block 99% of a day's data from being available for
queries because of a single malformed record.

### 4.1 `products`

- `product_id IS NOT NULL`
- `department_id IS NOT NULL`

### 4.2 `orders`

- `order_id IS NOT NULL`
- `user_id IS NOT NULL`
- `order_timestamp` parseable as `TIMESTAMP`
- `total_amount >= 0` (negative totals are nonsense)

### 4.3 `order_items`

- `id IS NOT NULL`
- `order_id IS NOT NULL` **AND** exists in the `orders` Delta table
  → rejection reason: `unknown_order_id`
- `product_id IS NOT NULL` **AND** exists in the `products` Delta table
  → rejection reason: `unknown_product_id`
- `order_timestamp` parseable as `TIMESTAMP`

Referential integrity is checked against the **Delta tables in the
processed zone**, not against the same-batch raw files. This has an
implication: orders for day D must be processed before order_items for day
D, otherwise every order_items row will be quarantined as `unknown_order_id`.
The Step Functions definition enforces this ordering (orders job runs first
per-day; order_items job runs after).

---

## 5. MERGE pattern (idempotent upsert)

Every Glue job, after validation + dedup, performs a Delta `MERGE`:

```sql
MERGE INTO target USING source
ON target.<dedup_key> = source.<dedup_key>
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

The first time a table is written (no `_delta_log` yet), the job creates the
Delta table with the documented schema and partitioning instead of running
`MERGE`. Every subsequent run is a MERGE.

Re-running the same file is therefore a no-op for already-present rows and a
correction for any rows whose values changed — this is what makes the
pipeline idempotent and safe to replay.

---

## 6. Layout in S3

```
s3://<bucket>/
  raw/                    ← CSVs land here (bronze)
    products/
    orders/
    order_items/
  processed/              ← Delta tables (silver — "lakehouse-dwh")
    products/
    orders/
    order_items/
  archive/                ← raw files after successful load
  quarantine/             ← rejected rows + bad files
  scripts/                ← Glue job .py files
  athena-results/         ← Athena query output
```

Gold-level business aggregates are out of scope for this project; Athena
queries the silver Delta tables directly via the Glue Data Catalog.
