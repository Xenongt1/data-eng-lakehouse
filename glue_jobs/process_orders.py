"""Glue job: validate + dedupe + MERGE an orders CSV into the orders Delta
table (partitioned by order_date).

Args:
    --JOB_NAME    Glue-supplied
    --source_key  S3 key of the incoming CSV (e.g. raw/orders/orders_2025-04-01.csv)
    --bucket      Lakehouse S3 bucket name (no scheme)
"""
import sys
import uuid
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (
    build_spark,
    dedupe,
    merge_into_delta,
    read_csv,
    validate,
    write_quarantine,
)


# Source columns. `date` is renamed to `order_date` post-read (Section 6 schema).
ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_num", T.IntegerType(), True),
        T.StructField("order_id", T.LongType(), True),
        T.StructField("user_id", T.LongType(), True),
        T.StructField("order_timestamp", T.TimestampType(), True),
        T.StructField("total_amount", T.DecimalType(10, 2), True),
        T.StructField("date", T.DateType(), True),
    ]
)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_key", "bucket"])
    bucket = args["bucket"]
    source_key = args["source_key"]
    run_id = (
        datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    )

    spark = build_spark(args["JOB_NAME"])
    bucket_root = f"s3://{bucket}"
    source_path = f"{bucket_root}/{source_key}"

    raw = (
        read_csv(spark, source_path, ORDERS_SCHEMA)
        .withColumnRenamed("date", "order_date")
        .withColumn("ingested_at", F.current_timestamp())
    )
    n_read = raw.count()

    rules = [
        ("order_id_null", F.col("order_id").isNotNull()),
        ("user_id_null", F.col("user_id").isNotNull()),
        # PERMISSIVE CSV → unparseable timestamps land as null. Same rule covers both.
        ("order_timestamp_unparseable", F.col("order_timestamp").isNotNull()),
        # total_amount is nullable per schema; only reject explicit negatives.
        (
            "total_amount_negative",
            F.col("total_amount").isNull() | (F.col("total_amount") >= 0),
        ),
    ]
    valid, rejected = validate(raw, rules)
    n_rejected = rejected.count()
    write_quarantine(rejected, bucket_root, "orders", run_id)

    deduped = dedupe(valid, key="order_id", order_by="ingested_at")
    n_valid = deduped.count()

    # Constrain the MERGE to the partitions actually present in this batch so
    # concurrent MERGEs to other days don't trigger ConcurrentAppendException.
    dates = [r.order_date for r in deduped.select("order_date").distinct().collect()]
    partition_filter = None
    if dates:
        date_lits = ", ".join(f"date'{d}'" for d in dates)
        partition_filter = f"t.order_date IN ({date_lits})"

    merge_into_delta(
        spark,
        deduped,
        table_path=f"{bucket_root}/processed/orders/",
        dedup_key="order_id",
        partition_by=["order_date"],
        partition_filter=partition_filter,
    )

    print(
        f"[process_orders] run_id={run_id} source={source_key} "
        f"read={n_read} valid={n_valid} rejected={n_rejected}"
    )


if __name__ == "__main__":
    main()
