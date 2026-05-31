"""
Tests for the DQ check expressions used in bronze_to_silver.py.
Each test builds a small DataFrame that mirrors the silver schema for that entity,
applies the same checks used in production, and asserts the expected _dq values.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dq_utils import add_dq


# ── schemas ────────────────────────────────────────────────────────────────────

CLAIMS_SCHEMA = StructType([
    StructField("claim_id",          StringType(), True),
    StructField("policy_id",         StringType(), True),
    StructField("claim_amount_gbp",  DoubleType(), True),
    StructField("severity_label",    StringType(), True),
    StructField("fraud_risk_score",  DoubleType(), True),
])

POLICYHOLDERS_SCHEMA = StructType([
    StructField("policyholder_id", StringType(),  True),
    StructField("age",             IntegerType(), True),
    StructField("email",           StringType(),  True),
])

VEHICLES_SCHEMA = StructType([
    StructField("vehicle_id",       StringType(),  True),
    StructField("policyholder_id",  StringType(),  True),
    StructField("year",             IntegerType(), True),
    StructField("estimated_value",  DoubleType(),  True),
])

POLICIES_SCHEMA = StructType([
    StructField("policy_id",        StringType(), True),
    StructField("policyholder_id",  StringType(), True),
    StructField("vehicle_id",       StringType(), True),
    StructField("premium_monthly",  DoubleType(), True),
])


# ── helpers ────────────────────────────────────────────────────────────────────

def claims_checks():
    return [
        (F.col("claim_id").isNull() | ~F.col("claim_id").like("CLM-%"),  "invalid_claim_id"),
        (F.col("policy_id").isNull(),                                      "missing_policy_fk"),
        (F.col("claim_amount_gbp").isNotNull() & (F.col("claim_amount_gbp") <= 0), "non_positive_claim_amount"),
        (~F.col("severity_label").isin("minor", "moderate", "severe", "total_loss"), "invalid_severity_label"),
        (F.col("fraud_risk_score").isNotNull() & (~F.col("fraud_risk_score").between(0.0, 1.0)), "fraud_score_out_of_range"),
    ]

def policyholders_checks():
    return [
        (F.col("policyholder_id").isNull() | ~F.col("policyholder_id").like("PH-%"), "invalid_policyholder_id"),
        (F.col("age").isNotNull() & (~F.col("age").between(16, 100)),                 "age_out_of_range"),
        (~F.col("email").like("%@%"),                                                  "invalid_email_format"),
    ]

def vehicles_checks():
    return [
        (F.col("vehicle_id").isNull() | ~F.col("vehicle_id").like("VH-%"),           "invalid_vehicle_id"),
        (F.col("policyholder_id").isNull(),                                            "missing_policyholder_fk"),
        (F.col("year").isNotNull() & (~F.col("year").between(1990, 2030)),             "year_out_of_range"),
        (F.col("estimated_value").isNotNull() & (F.col("estimated_value") <= 0),       "non_positive_vehicle_value"),
    ]

def policies_checks():
    return [
        (F.col("policy_id").isNull() | ~F.col("policy_id").like("POL-%"),  "invalid_policy_id"),
        (F.col("policyholder_id").isNull(),                                  "missing_policyholder_fk"),
        (F.col("vehicle_id").isNull(),                                       "missing_vehicle_fk"),
        (F.col("premium_monthly").isNotNull() & (F.col("premium_monthly") <= 0), "non_positive_premium"),
    ]


def _dq(spark, row_overrides, defaults, schema, checks_fn):
    defaults.update(row_overrides)
    df = spark.createDataFrame([tuple(defaults.values())], schema=schema)
    return add_dq(df, checks_fn()).collect()[0]


# ── claims ─────────────────────────────────────────────────────────────────────

def _claim(spark, **kw):
    return _dq(spark, kw, dict(
        claim_id="CLM-001", policy_id="POL-001",
        claim_amount_gbp=1000.0, severity_label="moderate",
        fraud_risk_score=0.3,
    ), CLAIMS_SCHEMA, claims_checks)


def test_claims_clean_record_passes(spark):
    assert _claim(spark)["_dq"] is None

def test_claims_invalid_id_format(spark):
    assert "invalid_claim_id" in _claim(spark, claim_id="BAD-001")["_dq"]

def test_claims_null_id_flagged(spark):
    assert "invalid_claim_id" in _claim(spark, claim_id=None)["_dq"]

def test_claims_missing_policy_fk(spark):
    assert "missing_policy_fk" in _claim(spark, policy_id=None)["_dq"]

def test_claims_non_positive_amount(spark):
    assert "non_positive_claim_amount" in _claim(spark, claim_amount_gbp=0.0)["_dq"]

def test_claims_negative_amount(spark):
    assert "non_positive_claim_amount" in _claim(spark, claim_amount_gbp=-50.0)["_dq"]

def test_claims_invalid_severity(spark):
    assert "invalid_severity_label" in _claim(spark, severity_label="catastrophic")["_dq"]

def test_claims_fraud_score_above_range(spark):
    assert "fraud_score_out_of_range" in _claim(spark, fraud_risk_score=1.5)["_dq"]

def test_claims_fraud_score_below_range(spark):
    assert "fraud_score_out_of_range" in _claim(spark, fraud_risk_score=-0.1)["_dq"]

def test_claims_null_fraud_score_passes(spark):
    row = _claim(spark, fraud_risk_score=None)
    assert row["_dq"] is None or "fraud_score_out_of_range" not in (row["_dq"] or "")

def test_claims_multiple_violations_combined(spark):
    row = _claim(spark, claim_id="BAD", policy_id=None)
    issues = set(row["_dq"].split("|"))
    assert "invalid_claim_id" in issues
    assert "missing_policy_fk" in issues


# ── policyholders ──────────────────────────────────────────────────────────────

def _ph(spark, **kw):
    return _dq(spark, kw, dict(
        policyholder_id="PH-001", age=35, email="user@example.com",
    ), POLICYHOLDERS_SCHEMA, policyholders_checks)


def test_policyholders_clean_record_passes(spark):
    assert _ph(spark)["_dq"] is None

def test_policyholders_invalid_id(spark):
    assert "invalid_policyholder_id" in _ph(spark, policyholder_id="CUST-001")["_dq"]

def test_policyholders_age_too_young(spark):
    assert "age_out_of_range" in _ph(spark, age=15)["_dq"]

def test_policyholders_age_too_old(spark):
    assert "age_out_of_range" in _ph(spark, age=101)["_dq"]

def test_policyholders_boundary_ages_pass(spark):
    for age in [16, 100]:
        row = _ph(spark, age=age)
        assert row["_dq"] is None or "age_out_of_range" not in (row["_dq"] or "")

def test_policyholders_invalid_email(spark):
    assert "invalid_email_format" in _ph(spark, email="notanemail")["_dq"]


# ── vehicles ───────────────────────────────────────────────────────────────────

def _vehicle(spark, **kw):
    return _dq(spark, kw, dict(
        vehicle_id="VH-001", policyholder_id="PH-001", year=2018, estimated_value=12000.0,
    ), VEHICLES_SCHEMA, vehicles_checks)


def test_vehicles_clean_record_passes(spark):
    assert _vehicle(spark)["_dq"] is None

def test_vehicles_invalid_id(spark):
    assert "invalid_vehicle_id" in _vehicle(spark, vehicle_id="CAR-001")["_dq"]

def test_vehicles_missing_policyholder_fk(spark):
    assert "missing_policyholder_fk" in _vehicle(spark, policyholder_id=None)["_dq"]

def test_vehicles_year_too_old(spark):
    assert "year_out_of_range" in _vehicle(spark, year=1985)["_dq"]

def test_vehicles_year_too_new(spark):
    assert "year_out_of_range" in _vehicle(spark, year=2031)["_dq"]

def test_vehicles_non_positive_value(spark):
    assert "non_positive_vehicle_value" in _vehicle(spark, estimated_value=0.0)["_dq"]


# ── policies ───────────────────────────────────────────────────────────────────

def _policy(spark, **kw):
    return _dq(spark, kw, dict(
        policy_id="POL-001", policyholder_id="PH-001", vehicle_id="VH-001", premium_monthly=85.0,
    ), POLICIES_SCHEMA, policies_checks)


def test_policies_clean_record_passes(spark):
    assert _policy(spark)["_dq"] is None

def test_policies_invalid_id(spark):
    assert "invalid_policy_id" in _policy(spark, policy_id="P-001")["_dq"]

def test_policies_missing_policyholder_fk(spark):
    assert "missing_policyholder_fk" in _policy(spark, policyholder_id=None)["_dq"]

def test_policies_missing_vehicle_fk(spark):
    assert "missing_vehicle_fk" in _policy(spark, vehicle_id=None)["_dq"]

def test_policies_non_positive_premium(spark):
    assert "non_positive_premium" in _policy(spark, premium_monthly=-10.0)["_dq"]
