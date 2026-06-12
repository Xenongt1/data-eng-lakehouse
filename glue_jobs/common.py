"""Shared helpers for the lakehouse PySpark Glue jobs.

Public API:
    build_spark(app_name)         -> SparkSession with Delta extensions
    read_csv(spark, path, schema) -> DataFrame
    validate(df, rules)           -> (valid_df, rejected_df)
    dedupe(df, key, order_by)     -> DataFrame
    merge_into_delta(spark, df, table_path, dedup_key, partition_by) -> None
    write_quarantine(df, bucket_root, dataset, run_id) -> None
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover — only for static analysis envs
    DeltaTable = None  # type: ignore


Rule = Tuple[str, Column]


def build_spark(app_name: str) -> SparkSession:
    """Return a Delta-aware SparkSession.

    On Glue 4.0 with --datalake-formats=delta the jars + configs are already
    applied; the builder configs below are harmless because getOrCreate()
    returns the existing session. Locally these configs register the Delta
    extensions on a fresh session.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
    """Explicit-schema CSV read. PERMISSIVE so unparseable values become null
    rather than failing the whole job — the validation pass filters them out."""
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(path)
    )


def validate(df: DataFrame, rules: List[Rule]) -> Tuple[DataFrame, DataFrame]:
    """Split df into (valid, rejected). A row is rejected if any rule's
    predicate is False; rejected rows get a `rejection_reason` column carrying
    the FIRST failing rule's name (rules are evaluated top-to-bottom)."""
    if not rules:
        empty_rejected = df.withColumn(
            "rejection_reason", F.lit(None).cast("string")
        ).filter(F.lit(False))
        return df, empty_rejected

    reason_expr = F.lit(None).cast("string")
    for name, predicate in reversed(rules):
        reason_expr = F.when(~predicate, F.lit(name)).otherwise(reason_expr)

    marked = df.withColumn("rejection_reason", reason_expr)
    valid = marked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = marked.filter(F.col("rejection_reason").isNotNull())
    return valid, rejected


def dedupe(df: DataFrame, key: str, order_by: str = "ingested_at") -> DataFrame:
    """Keep only the newest row per `key`, breaking ties arbitrarily. The
    ordering column must already exist on df."""
    w = Window.partitionBy(key).orderBy(F.col(order_by).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def merge_into_delta(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    dedup_key: str,
    partition_by: Optional[List[str]] = None,
) -> None:
    """Upsert df into the Delta table at table_path. Creates the table on
    first call with the given partitioning."""
    if DeltaTable is not None and DeltaTable.isDeltaTable(spark, table_path):
        target = DeltaTable.forPath(spark, table_path)
        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{dedup_key} = s.{dedup_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(table_path)


def write_quarantine(
    rejected_df: DataFrame,
    bucket_root: str,
    dataset: str,
    run_id: str,
) -> None:
    """Land rejected rows at <bucket_root>/quarantine/<dataset>/<run_id>/ as
    Parquet. No-op if there are no rejected rows (avoids creating empty
    quarantine partitions)."""
    if rejected_df.head(1):
        path = f"{bucket_root}/quarantine/{dataset}/{run_id}/"
        rejected_df.write.mode("overwrite").parquet(path)
