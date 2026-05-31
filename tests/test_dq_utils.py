"""Tests for the add_dq and to_quarantine helper functions in dq_utils."""

import pyspark.sql.functions as F
from pyspark.sql import Row
from dq_utils import add_dq, to_quarantine, QUARANTINE_COLS


def test_add_dq_clean_record(spark):
    df = spark.createDataFrame([Row(id="CLM-001", amount=500.0)])
    checks = [
        (F.col("amount") <= 0, "non_positive_amount"),
    ]
    result = add_dq(df, checks).collect()
    assert result[0]["_dq"] is None


def test_add_dq_single_violation(spark):
    df = spark.createDataFrame([Row(id="CLM-001", amount=-10.0)])
    checks = [
        (F.col("amount") <= 0, "non_positive_amount"),
    ]
    result = add_dq(df, checks).collect()
    assert result[0]["_dq"] == "non_positive_amount"


def test_add_dq_multiple_violations_pipe_separated(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])
    df = spark.createDataFrame([(None, -10.0)], schema=schema)
    checks = [
        (F.col("id").isNull(), "missing_id"),
        (F.col("amount") <= 0, "non_positive_amount"),
    ]
    result = add_dq(df, checks).collect()
    issues = set(result[0]["_dq"].split("|"))
    assert issues == {"missing_id", "non_positive_amount"}


def test_add_dq_only_failing_check_appears(spark):
    df = spark.createDataFrame([Row(id="CLM-001", amount=-10.0)])
    checks = [
        (F.col("id").isNull(), "missing_id"),
        (F.col("amount") <= 0, "non_positive_amount"),
    ]
    result = add_dq(df, checks).collect()
    assert result[0]["_dq"] == "non_positive_amount"


def test_add_dq_preserves_existing_columns(spark):
    df = spark.createDataFrame([Row(id="CLM-001", amount=100.0, region="Yorkshire")])
    result = add_dq(df, []).collect()[0]
    assert result["id"] == "CLM-001"
    assert result["amount"] == 100.0
    assert result["region"] == "Yorkshire"


def test_to_quarantine_schema(spark):
    df = spark.createDataFrame([Row(id="CLM-BAD", amount=-5.0)])
    checks = [(F.col("amount") <= 0, "non_positive_amount")]
    dq_df = add_dq(df, checks)
    result = to_quarantine(dq_df, "claims", "id")
    assert result.columns == QUARANTINE_COLS


def test_to_quarantine_excludes_clean_records(spark):
    rows = [Row(id="CLM-001", amount=500.0), Row(id="CLM-BAD", amount=-5.0)]
    df = spark.createDataFrame(rows)
    checks = [(F.col("amount") <= 0, "non_positive_amount")]
    dq_df = add_dq(df, checks)
    result = to_quarantine(dq_df, "claims", "id").collect()
    assert len(result) == 1
    assert result[0]["record_id"] == "CLM-BAD"


def test_to_quarantine_record_id_and_source_table(spark):
    df = spark.createDataFrame([Row(id="CLM-BAD", amount=-5.0)])
    checks = [(F.col("amount") <= 0, "non_positive_amount")]
    dq_df = add_dq(df, checks)
    row = to_quarantine(dq_df, "claims", "id").collect()[0]
    assert row["source_table"] == "claims"
    assert row["record_id"] == "CLM-BAD"
    assert row["dq_rule_violated"] == "non_positive_amount"


def test_to_quarantine_raw_record_is_json(spark):
    df = spark.createDataFrame([Row(id="CLM-BAD", amount=-5.0)])
    checks = [(F.col("amount") <= 0, "non_positive_amount")]
    dq_df = add_dq(df, checks)
    row = to_quarantine(dq_df, "claims", "id").collect()[0]
    import json
    parsed = json.loads(row["raw_record"])
    assert parsed["id"] == "CLM-BAD"
    assert parsed["amount"] == -5.0


def test_to_quarantine_empty_when_no_failures(spark):
    df = spark.createDataFrame([Row(id="CLM-001", amount=500.0)])
    checks = [(F.col("amount") <= 0, "non_positive_amount")]
    dq_df = add_dq(df, checks)
    result = to_quarantine(dq_df, "claims", "id").collect()
    assert len(result) == 0
