"""Verifies the Section 6.4 validation rules behave as described: bad rows
get rejected with the first failing rule's name; good rows pass through."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from common import validate


@pytest.fixture
def products_rules():
    from pyspark.sql import functions as F

    return [
        ("product_id_null", F.col("product_id").isNotNull()),
        ("department_id_null", F.col("department_id").isNotNull()),
    ]


@pytest.fixture
def orders_rules():
    from pyspark.sql import functions as F

    return [
        ("order_id_null", F.col("order_id").isNotNull()),
        ("user_id_null", F.col("user_id").isNotNull()),
        ("order_timestamp_unparseable", F.col("order_timestamp").isNotNull()),
        (
            "total_amount_negative",
            F.col("total_amount").isNull() | (F.col("total_amount") >= 0),
        ),
    ]


def test_all_rows_valid_when_no_rule_fails(spark, products_rules):
    df = spark.createDataFrame(
        [(1, 10, "dept", "name"), (2, 20, "dept", "name")],
        "product_id int, department_id int, department string, product_name string",
    )
    valid, rejected = validate(df, products_rules)
    assert valid.count() == 2
    assert rejected.count() == 0


def test_null_pk_is_rejected(spark, products_rules):
    df = spark.createDataFrame(
        [(None, 10), (2, 20), (3, 30)],
        "product_id int, department_id int",
    )
    valid, rejected = validate(df, products_rules)
    assert valid.count() == 2
    rejected_rows = rejected.collect()
    assert len(rejected_rows) == 1
    assert rejected_rows[0].rejection_reason == "product_id_null"


def test_first_failing_rule_is_reported(spark, products_rules):
    """Row with both PK and FK null should report `product_id_null` (the
    first rule in the list), not `department_id_null`."""
    df = spark.createDataFrame(
        [(None, None)], "product_id int, department_id int"
    )
    _, rejected = validate(df, products_rules)
    assert rejected.collect()[0].rejection_reason == "product_id_null"


def test_negative_total_amount_is_rejected(spark, orders_rules):
    schema = (
        "order_id long, user_id long, order_timestamp timestamp, "
        "total_amount decimal(10,2)"
    )
    df = spark.createDataFrame(
        [
            (1, 100, datetime(2025, 4, 1, 10, 0, 0), Decimal("19.99")),
            (2, 101, datetime(2025, 4, 1, 11, 0, 0), Decimal("-5.00")),
            (3, 102, datetime(2025, 4, 1, 12, 0, 0), Decimal("0.00")),
        ],
        schema,
    )

    valid, rejected = validate(df, orders_rules)
    assert valid.count() == 2
    bad = rejected.collect()
    assert len(bad) == 1
    assert bad[0].rejection_reason == "total_amount_negative"
    assert bad[0].order_id == 2


def test_null_total_amount_is_accepted(spark, orders_rules):
    """The schema marks total_amount nullable; only EXPLICIT negatives fail."""
    schema = (
        "order_id long, user_id long, order_timestamp timestamp, "
        "total_amount decimal(10,2)"
    )
    df = spark.createDataFrame(
        [(1, 100, datetime(2025, 4, 1, 10, 0, 0), None)], schema
    )

    valid, rejected = validate(df, orders_rules)
    assert valid.count() == 1
    assert rejected.count() == 0


def test_unparseable_timestamp_is_rejected(spark, orders_rules):
    """PERMISSIVE CSV read leaves bad timestamps as null. The rule catches that."""
    schema = (
        "order_id long, user_id long, order_timestamp timestamp, "
        "total_amount decimal(10,2)"
    )
    df = spark.createDataFrame(
        [(1, 100, None, Decimal("10.00"))], schema
    )
    valid, rejected = validate(df, orders_rules)
    assert valid.count() == 0
    assert rejected.collect()[0].rejection_reason == "order_timestamp_unparseable"


def test_empty_rule_list_returns_all_rows_valid(spark):
    df = spark.createDataFrame([(1,), (2,)], "x int")
    valid, rejected = validate(df, [])
    assert valid.count() == 2
    assert rejected.count() == 0


def test_rejection_reason_column_only_on_rejected(spark, products_rules):
    df = spark.createDataFrame([(None, 10), (1, 20)], "product_id int, department_id int")
    valid, rejected = validate(df, products_rules)
    assert "rejection_reason" not in valid.columns
    assert "rejection_reason" in rejected.columns
