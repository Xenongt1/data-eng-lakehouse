"""Glue job: validate + dedupe + MERGE an order_items CSV into the order_items
Delta table (partitioned by order_date).

Referential integrity:
    - order_items.order_id MUST exist in the orders Delta table
    - order_items.product_id MUST exist in the products Delta table
    Rows that violate either are quarantined with reason
    `unknown_order_id` / `unknown_product_id`, not failed at job level. This is
    why orders MUST be processed before order_items for a given day (the
    Step Functions definition enforces the ordering operationally).

Args:
    --JOB_NAME    Glue-supplied
    --source_key  S3 key of the incoming CSV
    --bucket      Lakehouse S3 bucket name (no scheme)
"""
import sys
import uuid
from datetime import datetime

from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
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


ORDER_ITEMS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("order_id", T.LongType(), True),
        T.StructField("user_id", T.LongType(), True),
        T.StructField("days_since_prior_order", T.IntegerType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("add_to_cart_order", T.IntegerType(), True),
        T.StructField("reordered", T.BooleanType(), True),
        T.StructField("order_timestamp", T.TimestampType(), True),
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
        read_csv(spark, source_path, ORDER_ITEMS_SCHEMA)
        .withColumnRenamed("date", "order_date")
        .withColumn("ingested_at", F.current_timestamp())
    )
    n_read = raw.count()

    # Stage 1: structural validation.
    structural_rules = [
        ("id_null", F.col("id").isNotNull()),
        ("order_id_null", F.col("order_id").isNotNull()),
        ("product_id_null", F.col("product_id").isNotNull()),
        ("order_timestamp_unparseable", F.col("order_timestamp").isNotNull()),
    ]
    structural_valid, structural_rejected = validate(raw, structural_rules)

    # Stage 2: referential integrity against the Delta tables in processed/.
    orders_path = f"{bucket_root}/processed/orders/"
    products_path = f"{bucket_root}/processed/products/"

    orders_exists = DeltaTable.isDeltaTable(spark, orders_path)
    products_exists = DeltaTable.isDeltaTable(spark, products_path)

    if not orders_exists or not products_exists:
        missing = []
        if not orders_exists:
            missing.append("orders")
        if not products_exists:
            missing.append("products")
        raise RuntimeError(
            "Referential targets missing in processed zone: "
            + ", ".join(missing)
            + ". Run process_orders and process_products before order_items."
        )

    known_orders = (
        spark.read.format("delta").load(orders_path).select("order_id").distinct()
    )
    known_products = (
        spark.read.format("delta").load(products_path).select("product_id").distinct()
    )

    v = structural_valid.alias("v")
    ko = known_orders.alias("ko")
    kp = known_products.alias("kp")

    bad_order = v.join(
        ko, F.col("v.order_id") == F.col("ko.order_id"), "leftanti"
    ).withColumn("rejection_reason", F.lit("unknown_order_id"))

    survives_order = v.join(
        ko, F.col("v.order_id") == F.col("ko.order_id"), "leftsemi"
    )

    go = survives_order.alias("go")
    bad_product = go.join(
        kp, F.col("go.product_id") == F.col("kp.product_id"), "leftanti"
    ).withColumn("rejection_reason", F.lit("unknown_product_id"))

    valid_final = survives_order.join(
        kp, F.col("product_id") == F.col("kp.product_id"), "leftsemi"
    )

    ref_rejected = bad_order.unionByName(bad_product)
    all_rejected = structural_rejected.unionByName(
        ref_rejected, allowMissingColumns=True
    )
    n_rejected = all_rejected.count()
    write_quarantine(all_rejected, bucket_root, "order_items", run_id)

    deduped = dedupe(valid_final, key="id", order_by="ingested_at")
    n_valid = deduped.count()

    merge_into_delta(
        spark,
        deduped,
        table_path=f"{bucket_root}/processed/order_items/",
        dedup_key="id",
        partition_by=["order_date"],
    )

    print(
        f"[process_order_items] run_id={run_id} source={source_key} "
        f"read={n_read} valid={n_valid} rejected={n_rejected}"
    )


if __name__ == "__main__":
    main()
