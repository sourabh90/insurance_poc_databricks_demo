# Databricks notebook source
# Pipeline: Bronze → Silver (DQ + Refinement)
#
# Pattern per entity:
#   _<entity>_dq  view  — casts types + appends _dq column (NULL=clean, pipe-sep issues=bad)
#   <entity>      table — clean records only (_dq IS NULL)
#   _<entity>_failed view — bad records projected to common quarantine schema
#
# All bad records from every entity land in a single `quarantine` table:
#   silver_dev.refined_claims.quarantine
#   Columns: source_table | record_id | dq_rule_violated | raw_record (JSON) | quarantined_at
#
# Query examples:
#   SELECT source_table, dq_rule_violated, count(*) FROM quarantine GROUP BY 1,2 ORDER BY 3 DESC
#   SELECT * FROM quarantine WHERE source_table = 'claims' AND dq_rule_violated LIKE '%invalid_severity%'

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from dq_utils import add_dq as _add_dq, to_quarantine as _to_quarantine

bronze_catalog = spark.conf.get("bronze_catalog", "bronze_dev")
bronze_schema  = spark.conf.get("bronze_schema",  "raw_claims")

def bronze(table: str):
    return spark.table(f"{bronze_catalog}.{bronze_schema}.{table}")

# ── policyholders ─────────────────────────────────────────────────────────────

@dlt.view(name="_policyholders_dq")
def _policyholders_dq():
    df = (
        bronze("policyholders")
        .withColumn("age",            F.floor(F.datediff(F.current_date(), F.to_date("date_of_birth")) / 365).cast(IntegerType()))
        .withColumn("driving_points", F.col("driving_points").cast(IntegerType()))
        .withColumn("driving_years",  F.col("driving_years").cast(IntegerType()))
        .withColumn("created_date",   F.to_date("created_date"))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("policyholder_id").isNull() | ~F.col("policyholder_id").like("PH-%"),
         "invalid_policyholder_id"),
        (F.col("age").isNotNull() & (~F.col("age").between(16, 100)),
         "age_out_of_range"),
        (~F.col("email").like("%@%"),
         "invalid_email_format"),
    ])

@dlt.table(name="policyholders", comment="Clean policyholders — all DQ checks passed")
def policyholders():
    return dlt.read("_policyholders_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_policyholders_failed")
def _policyholders_failed():
    return _to_quarantine(dlt.read("_policyholders_dq"), "policyholders", "policyholder_id")

# ── vehicles ──────────────────────────────────────────────────────────────────

@dlt.view(name="_vehicles_dq")
def _vehicles_dq():
    df = (
        bronze("vehicles")
        .withColumn("year",            F.col("year").cast(IntegerType()))
        .withColumn("estimated_value", F.col("estimated_value").cast(DoubleType()))
        .withColumn("mileage",         F.col("mileage").cast(IntegerType()))
        .withColumn("created_date",    F.to_date("created_date"))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("vehicle_id").isNull() | ~F.col("vehicle_id").like("VH-%"),
         "invalid_vehicle_id"),
        (F.col("policyholder_id").isNull(),
         "missing_policyholder_fk"),
        (F.col("year").isNotNull() & (~F.col("year").between(1990, 2030)),
         "year_out_of_range"),
        (F.col("estimated_value").isNotNull() & (F.col("estimated_value") <= 0),
         "non_positive_vehicle_value"),
    ])

@dlt.table(name="vehicles", comment="Clean vehicles — all DQ checks passed")
def vehicles():
    return dlt.read("_vehicles_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_vehicles_failed")
def _vehicles_failed():
    return _to_quarantine(dlt.read("_vehicles_dq"), "vehicles", "vehicle_id")

# ── policies ──────────────────────────────────────────────────────────────────

@dlt.view(name="_policies_dq")
def _policies_dq():
    df = (
        bronze("policies")
        .withColumn("premium_monthly", F.col("premium_monthly").cast(DoubleType()))
        .withColumn("excess",          F.col("excess").cast(DoubleType()))
        .withColumn("coverage_limit",  F.col("coverage_limit").cast(DoubleType()))
        .withColumn("start_date",      F.to_date("start_date"))
        .withColumn("end_date",        F.to_date("end_date"))
        .withColumn("created_date",    F.to_date("created_date"))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("policy_id").isNull() | ~F.col("policy_id").like("POL-%"),
         "invalid_policy_id"),
        (F.col("policyholder_id").isNull(),
         "missing_policyholder_fk"),
        (F.col("vehicle_id").isNull(),
         "missing_vehicle_fk"),
        (F.col("end_date").isNotNull() & F.col("start_date").isNotNull() &
         (F.col("end_date") <= F.col("start_date")),
         "end_date_before_start_date"),
        (F.col("premium_monthly").isNotNull() & (F.col("premium_monthly") <= 0),
         "non_positive_premium"),
    ])

@dlt.table(name="policies", comment="Clean policies — all DQ checks passed")
def policies():
    return dlt.read("_policies_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_policies_failed")
def _policies_failed():
    return _to_quarantine(dlt.read("_policies_dq"), "policies", "policy_id")

# ── claims ────────────────────────────────────────────────────────────────────

@dlt.view(name="_claims_dq")
def _claims_dq():
    df = (
        bronze("claims")
        .withColumn("claim_amount_gbp", F.col("claim_amount_gbp").cast(DoubleType()))
        .withColumn("fraud_risk_score", F.col("fraud_risk_score").cast(DoubleType()))
        .withColumn("claim_date",       F.to_date("claim_date"))
        .withColumn("created_date",     F.to_date("created_date"))
        .withColumn("is_storm_related",
            F.when(F.upper(F.col("is_storm_related")).isin("Y", "YES", "TRUE", "1"), F.lit(True))
             .otherwise(F.lit(False)))
        .withColumn("fraud_flag",
            F.when(F.col("fraud_risk_score") >= 0.6, F.lit(True)).otherwise(F.lit(False)))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("claim_id").isNull() | ~F.col("claim_id").like("CLM-%"),
         "invalid_claim_id"),
        (F.col("policy_id").isNull(),
         "missing_policy_fk"),
        (F.col("claim_amount_gbp").isNotNull() & (F.col("claim_amount_gbp") <= 0),
         "non_positive_claim_amount"),
        (~F.col("severity_label").isin("minor", "moderate", "severe", "total_loss"),
         "invalid_severity_label"),
        (F.col("fraud_risk_score").isNotNull() &
         (~F.col("fraud_risk_score").between(0.0, 1.0)),
         "fraud_score_out_of_range"),
    ])

@dlt.table(name="claims", comment="Clean claims — all DQ checks passed")
def claims():
    return dlt.read("_claims_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_claims_failed")
def _claims_failed():
    return _to_quarantine(dlt.read("_claims_dq"), "claims", "claim_id")

# ── incidents ─────────────────────────────────────────────────────────────────

@dlt.view(name="_incidents_dq")
def _incidents_dq():
    df = (
        bronze("incidents")
        .withColumn("incident_date",         F.to_date("incident_date"))
        .withColumn("num_vehicles_involved",  F.col("num_vehicles_involved").cast(IntegerType()))
        .withColumn("witness_count",          F.col("witness_count").cast(IntegerType()))
        .withColumn("created_date",           F.to_date("created_date"))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("incident_id").isNull() | ~F.col("incident_id").like("INC-%"),
         "invalid_incident_id"),
        (F.col("claim_id").isNull(),
         "missing_claim_fk"),
    ])

@dlt.table(name="incidents", comment="Clean incidents — all DQ checks passed")
def incidents():
    return dlt.read("_incidents_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_incidents_failed")
def _incidents_failed():
    return _to_quarantine(dlt.read("_incidents_dq"), "incidents", "incident_id")

# ── claim_assessments ─────────────────────────────────────────────────────────

@dlt.view(name="_claim_assessments_dq")
def _claim_assessments_dq():
    df = (
        bronze("claim_assessments")
        .withColumn("estimated_repair_cost_gbp", F.col("estimated_repair_cost_gbp").cast(DoubleType()))
        .withColumn("labour_hours",              F.col("labour_hours").cast(DoubleType()))
        .withColumn("parts_cost_gbp",            F.col("parts_cost_gbp").cast(DoubleType()))
        .withColumn("assessment_date",           F.to_date("assessment_date"))
        .withColumn("created_date",              F.to_date("created_date"))
        .withColumn("total_assessment_cost_gbp",
            F.coalesce(F.col("estimated_repair_cost_gbp"), F.lit(0.0)))
        .drop("_ingested_at", "_source_file", "_rescued_data")
    )
    return _add_dq(df, [
        (F.col("assessment_id").isNull() | ~F.col("assessment_id").like("ASS-%"),
         "invalid_assessment_id"),
        (F.col("claim_id").isNull(),
         "missing_claim_fk"),
        (F.col("estimated_repair_cost_gbp").isNotNull() & (F.col("estimated_repair_cost_gbp") <= 0),
         "non_positive_repair_cost"),
        (F.col("labour_hours").isNotNull() & (F.col("labour_hours") <= 0),
         "non_positive_labor_hours"),
    ])

@dlt.table(name="claim_assessments", comment="Clean assessments — all DQ checks passed")
def claim_assessments():
    return dlt.read("_claim_assessments_dq").filter(F.col("_dq").isNull()).drop("_dq")

@dlt.view(name="_claim_assessments_failed")
def _claim_assessments_failed():
    return _to_quarantine(dlt.read("_claim_assessments_dq"), "claim_assessments", "assessment_id")

# ── Common quarantine table ───────────────────────────────────────────────────

@dlt.table(
    name="quarantine",
    comment="""
    Single quarantine table for all DQ failures across every source entity.
    Schema: source_table | record_id | dq_rule_violated | raw_record (JSON) | quarantined_at

    Useful queries:
      -- Summary by table and rule
      SELECT source_table, dq_rule_violated, count(*) AS failed_records
      FROM quarantine GROUP BY 1, 2 ORDER BY 3 DESC

      -- Inspect specific bad records
      SELECT record_id, dq_rule_violated, raw_record
      FROM quarantine WHERE source_table = 'claims'

      -- Records with multiple issues
      SELECT * FROM quarantine WHERE dq_rule_violated LIKE '%|%'
    """
)
def quarantine():
    return (
        dlt.read("_policyholders_failed")
        .unionByName(dlt.read("_vehicles_failed"))
        .unionByName(dlt.read("_policies_failed"))
        .unionByName(dlt.read("_claims_failed"))
        .unionByName(dlt.read("_incidents_failed"))
        .unionByName(dlt.read("_claim_assessments_failed"))
    )
