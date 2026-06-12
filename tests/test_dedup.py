"""Verifies dedupe() keeps exactly one row per key — the one with the newest
`order_by` value."""
from __future__ import annotations

from pyspark.sql import functions as F

from common import dedupe


def test_keeps_newest_per_key(spark):
    df = spark.createDataFrame(
        [
            (1, "2025-01-01 09:00:00", "old"),
            (1, "2025-01-01 11:00:00", "new"),  # winner for key 1
            (1, "2025-01-01 10:00:00", "mid"),
            (2, "2025-01-01 12:00:00", "only"),
        ],
        "key int, ts string, val string",
    ).withColumn("ingested_at", F.to_timestamp("ts"))

    result = dedupe(df, key="key", order_by="ingested_at").collect()
    by_key = {r.key: r.val for r in result}
    assert by_key == {1: "new", 2: "only"}


def test_no_duplicates_passes_through(spark):
    df = spark.createDataFrame(
        [(1, "2025-01-01 10:00:00"), (2, "2025-01-01 11:00:00")],
        "key int, ts string",
    ).withColumn("ingested_at", F.to_timestamp("ts"))

    result = dedupe(df, key="key", order_by="ingested_at")
    assert result.count() == 2


def test_helper_column_is_dropped(spark):
    """The internal _rn column added by the row_number window must not leak
    into the returned DataFrame."""
    df = spark.createDataFrame(
        [(1, "2025-01-01 10:00:00")], "key int, ts string"
    ).withColumn("ingested_at", F.to_timestamp("ts"))
    result = dedupe(df, key="key", order_by="ingested_at")
    assert "_rn" not in result.columns


def test_single_key_with_duplicates_collapses_to_one(spark):
    df = spark.createDataFrame(
        [
            (1, "2025-01-01 10:00:00"),
            (1, "2025-01-01 10:00:01"),
            (1, "2025-01-01 10:00:02"),
        ],
        "key int, ts string",
    ).withColumn("ingested_at", F.to_timestamp("ts"))

    result = dedupe(df, key="key", order_by="ingested_at")
    assert result.count() == 1
