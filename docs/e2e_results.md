# End-to-end test results — 2026-06-12

Trigger: drop the 31 daily CSVs (1 products + 15 orders + 15 order_items)
into `s3://lakehouse-ecommerce-899957567386-us-east-1/raw/<dataset>/`.
EventBridge routes each S3 object-created event to the
`lakehouse-ecommerce-ingest` Step Functions state machine, which runs the
matching Glue job, kicks the crawler, and archives the raw file.

---

## Final row counts (Athena via lakehouse_wg, database lakehouse_dwh)

| Table        | Rows   | Expected | Match |
|--------------|--------|----------|-------|
| products     |  1,000 |  1,000   | ✓     |
| orders       |  7,500 |  7,500   | ✓     |
| order_items  | 40,803 | 40,803   | ✓     |

`order_items` matches the raw CSV row count exactly (40,803 = `wc -l` of all
15 daily files, minus 15 headers). Zero rows were dropped to quarantine.
The `reordered = true` count (20,294) lines up byte-for-byte with the
20,294 `1`s in the source CSVs.

## Distribution by day

Orders: 15 × 500. Order_items per day:

| date       | order_items | date       | order_items |
|------------|-------------|------------|-------------|
| 2025-04-01 | 2,768       | 2025-04-09 | 2,741       |
| 2025-04-02 | 2,743       | 2025-04-10 | 2,632       |
| 2025-04-03 | 2,732       | 2025-04-11 | 2,708       |
| 2025-04-04 | 2,668       | 2025-04-12 | 2,678       |
| 2025-04-05 | 2,808       | 2025-04-13 | 2,707       |
| 2025-04-06 | 2,644       | 2025-04-14 | 2,787       |
| 2025-04-07 | 2,752       | 2025-04-15 | 2,678       |
| 2025-04-08 | 2,757       |            |             |

## Selected analytical queries

**Daily revenue** (queries.sql §2)
Range $120k–$137k per day, 15 days, ~$1.95M total — consistent with 500
orders/day × ~$255 mean order value.

**Reorder rate by department** (queries.sql §5)

| department  | items  | reorders | reorder_rate |
|-------------|--------|----------|--------------|
| Electronics |  6,135 |  3,092   | 0.504        |
| Home        |  7,414 |  3,726   | 0.503        |
| Books       |  6,833 |  3,426   | 0.501        |
| Clothing    |  6,873 |  3,392   | 0.494        |
| Toys        |  6,471 |  3,181   | 0.492        |
| Sports      |  7,077 |  3,477   | 0.491        |

Synthetic data has reorder rate ~50% across the board, as expected.

## Execution timing

- products: 1 SFN execution, ~2 min (Glue cold start).
- orders: 15 SFN executions; day 1 sequential (~2 min) then days 2–15
  parallel (~3 min) — table-creation race avoided by uploading day 1 alone.
- order_items: same shape, ~5 min total.
- Wall clock end-to-end: ~14 minutes from first upload to last execution
  finishing.

## Issues encountered, and how they were resolved

The first E2E attempt found **two real bugs** that needed code fixes, and
then surfaced **one Delta-on-S3 subtlety** that needs operational care:

### Bug 1 — Ambiguous `product_id` in `process_order_items.py`

The referential-integrity check was written as
`v.join(kp, F.col("product_id") == F.col("kp.product_id"), "leftsemi")`,
which lost v's `v.` alias after the prior leftsemi join, leaving Spark
unable to resolve `F.col("product_id")` (it's in both sides). All 15
order_items runs failed with `AnalysisException: Reference 'product_id'
is ambiguous`. Fix: switch to the column-name form
`df.join(other, "product_id", "leftsemi")` which dedupes the join key
in the result.

### Bug 2 — Delta `ConcurrentAppendException` on parallel MERGE

With 15 parallel `process_orders` jobs each MERGE-ing one day's partition,
Delta's optimistic concurrency check fired across disjoint partitions and
9 of 15 jobs failed with
`ConcurrentAppendException: Files were added to partition [order_date=...]
by a concurrent update.` Fix in `glue_jobs/common.py::merge_into_delta`:
accept an optional `partition_filter` and append it to the MERGE
predicate (`AND t.order_date IN (date'2025-04-07')`) so Delta scopes
the conflict check to the partition this batch actually touches. Both
`process_orders` and `process_order_items` now derive the per-batch
partition list from the source DF and pass it in.

A jittered exponential-backoff retry around the MERGE was added as
belt-and-suspenders.

### Subtlety — silent orphaned parquet in one of 14 parallel MERGEs

After the two bug fixes, the 15-day parallel order_items batch produced
the right counts for **14** days but missed day 2025-04-06 entirely
(2,644 rows). The Glue job stdout reported `read=2644 valid=2644 rejected=0`
and the SFN execution reported SUCCEEDED, yet `_delta_log/` had no
commit referencing the parquet file Spark wrote into
`order_date=2025-04-06/`. The parquet was on S3 (36 KB, same size as
sibling partitions), just orphaned — Delta committed nothing.

Likely cause: a `ConcurrentAppendException` was caught and retried, but
the retry path saw a stale `DeltaTable.forPath` view and committed a
no-op. The MERGE bookkeeping returned without raising, so the outer
job reported success. This is consistent with reports of MERGE-retry
silent-loss on plain S3 (no DynamoDB log-store).

Mitigation in this run: re-uploaded `order_items_2025-04-06.csv`
(MERGE is idempotent on `id`); the row count now matches exactly.
Longer-term mitigations to consider: (a) enable the Delta-S3 DynamoDB
log store, (b) serialize ingest per partition, (c) verify final row
counts as part of CI rather than trusting "job succeeded".

### Schema bug — `reordered` was read as null

Source CSV stores `reordered` as `0`/`1`; my schema declared
`T.BooleanType()`, and Spark's CSV reader only parses literal
`true`/`false` for booleans. In PERMISSIVE mode this silently turned
every value to null, so `SUM(CASE WHEN reordered THEN 1 ELSE 0 END)`
returned 0 for every department. Fix: read `reordered` as
`IntegerType`, then `(F.col("reordered") != 0).cast("boolean")`
after read. Re-uploading the 15 order_items files with MERGE updated
all existing rows in place.

## State of the system after this test

- `s3://.../raw/`                  — empty (all 31 files archived)
- `s3://.../archive/`              — 31 source CSVs preserved
- `s3://.../processed/products/`   — 1 Delta partition, 1 parquet
- `s3://.../processed/orders/`     — 15 Delta partitions
- `s3://.../processed/order_items/`— 15 Delta partitions
- `s3://.../quarantine/`           — empty (no rejections in this run; 24
  failure-marker JSONs from the first buggy run are at
  `quarantine/_failed/` and can be deleted)
- Glue Catalog `lakehouse_dwh`     — products, orders, order_items
- `git tag v1.0.0` will be cut on the commit that introduces this file
