# Databricks notebook source
# Pipeline: Silver → Gold Facts

import dlt
import pyspark.sql.functions as F

silver_catalog = spark.conf.get("silver_catalog", "silver_dev")
silver_schema  = spark.conf.get("silver_schema",  "refined_claims")

def silver(table: str):
    return spark.table(f"{silver_catalog}.{silver_schema}.{table}")

# ── fact_claim ────────────────────────────────────────────────────────────────

@dlt.table(name="fact_claim", comment="Grain: one row per claim. Central fact table for UK claims analytics.")
def fact_claim():
    claims = silver("claims")
    pols   = silver("policies").select(
        "policy_id", "policy_type", "coverage_type", "premium_monthly",
        "excess", "coverage_limit", "status"
    )
    ph     = silver("policyholders").select(
        "policyholder_id", "age", "gender", "region", "city",
        "driving_years", "driving_points"
    )
    veh    = silver("vehicles").select(
        "vehicle_id", "make", "vehicle_type", "year", "estimated_value"
    )

    return (
        claims
        .join(pols, on="policy_id",       how="left")
        .join(ph,   on="policyholder_id", how="left")
        .join(veh,  on="vehicle_id",      how="left")
        .select(
            # Keys
            "claim_id", "policy_id", "policyholder_id", "vehicle_id",
            # Dates
            "claim_date",
            F.date_format("claim_date", "yyyyMMdd").cast("int").alias("claim_date_key"),
            # Claim attributes
            "claim_type", "incident_region", "incident_city",
            "claim_status", "severity_label",
            "is_storm_related", "fraud_flag", "fraud_risk_score",
            # Amounts (GBP)
            F.col("claim_amount_gbp").alias("claimed_amount_gbp"),
            "excess",
            F.greatest(F.col("claim_amount_gbp") - F.col("excess"), F.lit(0.0)).alias("net_payout_gbp"),
            # Policy context
            "policy_type", "coverage_type", "premium_monthly",
            (F.col("premium_monthly") * 12).alias("annual_premium_gbp"),
            "coverage_limit",
            # Policyholder context
            "age", "gender",
            F.col("region").alias("ph_region"),
            "driving_points",
            # Vehicle context
            "make", "vehicle_type",
            F.col("year").alias("vehicle_year"),
            F.col("estimated_value").alias("vehicle_value_gbp"),
        )
    )

# ── fact_assessment ───────────────────────────────────────────────────────────

@dlt.table(name="fact_assessment", comment="Grain: one row per claim assessment.")
def fact_assessment():
    ass    = silver("claim_assessments")
    claims = silver("claims").select(
        "claim_id", "claim_type", "severity_label", "is_storm_related",
        "incident_region", "claim_date"
    )

    return (
        ass
        .join(claims, on="claim_id", how="left")
        .select(
            "assessment_id", "claim_id", "assessor_id",
            "assessor_tier", "assessment_date",
            F.date_format("assessment_date", "yyyyMMdd").cast("int").alias("assessment_date_key"),
            "damage_category", "recommended_action",
            "estimated_repair_cost_gbp", "labour_hours", "parts_cost_gbp",
            "total_assessment_cost_gbp",
            # Claim context
            "claim_type", "severity_label",
            "is_storm_related", "incident_region", "claim_date",
            F.datediff(F.col("assessment_date"), F.col("claim_date")).alias("days_to_assessment"),
        )
    )
