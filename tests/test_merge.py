"""End-to-end test of merge_into_delta(): create + upsert behavior on a
local file:// Delta table."""
from __future__ import annotations

from common import merge_into_delta


def _read(spark, path):
    return (
        spark.read.format("delta")
        .load(path)
        .orderBy("id")
        .collect()
    )


def test_first_call_creates_the_table(spark, tmp_path):
    path = str(tmp_path / "products_initial")
    df = spark.createDataFrame(
        [(1, "a"), (2, "b")], "id int, val string"
    )
    merge_into_delta(spark, df, path, dedup_key="id")
    rows = _read(spark, path)
    assert [(r.id, r.val) for r in rows] == [(1, "a"), (2, "b")]


def test_second_call_upserts(spark, tmp_path):
    path = str(tmp_path / "products_upsert")
    initial = spark.createDataFrame(
        [(1, "a"), (2, "b")], "id int, val string"
    )
    merge_into_delta(spark, initial, path, dedup_key="id")

    # batch 2: update id=2, insert id=3, leave id=1 alone
    batch2 = spark.createDataFrame(
        [(2, "B"), (3, "c")], "id int, val string"
    )
    merge_into_delta(spark, batch2, path, dedup_key="id")

    rows = _read(spark, path)
    assert [(r.id, r.val) for r in rows] == [(1, "a"), (2, "B"), (3, "c")]


def test_partitioning_applied_on_initial_write(spark, tmp_path):
    path = str(tmp_path / "orders_partitioned")
    df = spark.createDataFrame(
        [
            (1, "2025-04-01", 10),
            (2, "2025-04-01", 20),
            (3, "2025-04-02", 30),
        ],
        "id int, order_date string, amount int",
    )
    merge_into_delta(
        spark, df, path, dedup_key="id", partition_by=["order_date"]
    )

    # Two date partitions should exist on disk
    from pathlib import Path as _P

    partitions = sorted(p.name for p in _P(path).iterdir() if p.name.startswith("order_date="))
    assert partitions == ["order_date=2025-04-01", "order_date=2025-04-02"]


def test_rerunning_same_batch_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "products_idempotent")
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")], "id int, val string"
    )
    merge_into_delta(spark, df, path, dedup_key="id")
    merge_into_delta(spark, df, path, dedup_key="id")
    rows = _read(spark, path)
    assert len(rows) == 3
    assert [(r.id, r.val) for r in rows] == [(1, "a"), (2, "b"), (3, "c")]
