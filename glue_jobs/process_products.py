"""Glue job: validate + dedupe + MERGE the products CSV into the products
Delta table.

Args:
    --JOB_NAME    Glue-supplied
    --source_key  S3 key of the incoming CSV (e.g. raw/products/products.csv)
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


PRODUCTS_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("department_id", T.IntegerType(), True),
        T.StructField("department", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
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

    raw = read_csv(spark, source_path, PRODUCTS_SCHEMA).withColumn(
        "ingested_at", F.current_timestamp()
    )
    n_read = raw.count()

    rules = [
        ("product_id_null", F.col("product_id").isNotNull()),
        ("department_id_null", F.col("department_id").isNotNull()),
    ]
    valid, rejected = validate(raw, rules)
    n_rejected = rejected.count()
    write_quarantine(rejected, bucket_root, "products", run_id)

    deduped = dedupe(valid, key="product_id", order_by="ingested_at")
    n_valid = deduped.count()

    merge_into_delta(
        spark,
        deduped,
        table_path=f"{bucket_root}/processed/products/",
        dedup_key="product_id",
        partition_by=None,
    )

    print(
        f"[process_products] run_id={run_id} source={source_key} "
        f"read={n_read} valid={n_valid} rejected={n_rejected}"
    )


if __name__ == "__main__":
    main()
