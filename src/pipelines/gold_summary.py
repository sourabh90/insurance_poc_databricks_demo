# Databricks notebook source
# Pipeline: Silver → Gold Summary (BI KPIs)

import dlt
import pyspark.sql.functions as F

silver_catalog = spark.conf.get("silver_catalog", "silver_dev")
silver_schema  = spark.conf.get("silver_schema",  "refined_claims")

def silver(table: str):
    return spark.table(f"{silver_catalog}.{silver_schema}.{table}")

# ── summary_claims_daily ──────────────────────────────────────────────────────

@dlt.table(name="summary_claims_daily", comment="Daily claim volume and GBP amounts by UK region and claim type.")
def summary_claims_daily():
    return (
        silver("claims")
        .groupBy("claim_date", "incident_region", "claim_type", "severity_label", "is_storm_related")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount_gbp").alias("total_claimed_gbp"),
            F.avg("claim_amount_gbp").alias("avg_claimed_gbp"),
            F.max("claim_amount_gbp").alias("max_claimed_gbp"),
            F.sum(F.when(F.col("claim_status") == "approved", F.col("claim_amount_gbp")).otherwise(0)).alias("approved_amount_gbp"),
            F.sum(F.when(F.col("claim_status") == "denied",   F.lit(1)).otherwise(0)).alias("denied_count"),
            F.sum(F.when(F.col("claim_status") == "settled",  F.col("claim_amount_gbp")).otherwise(0)).alias("settled_amount_gbp"),
            F.sum(F.when(F.col("claim_status") == "open",     F.lit(1)).otherwise(0)).alias("open_count"),
            F.sum(F.when(F.col("severity_label") == "total_loss", F.lit(1)).otherwise(0)).alias("total_loss_count"),
        )
        .withColumn("month",      F.date_format("claim_date", "yyyy-MM"))
        .withColumn("week_start", F.date_trunc("week", F.col("claim_date")))
    )

# ── summary_financial_kpis ────────────────────────────────────────────────────

@dlt.table(name="summary_financial_kpis", comment="Monthly financial performance — premiums vs payouts, loss ratio (GBP).")
def summary_financial_kpis():
    claims = silver("claims")
    pols   = silver("policies").select("policy_id", "premium_monthly", "excess", "coverage_limit", "policy_type", "status")
    joined = claims.join(pols, on="policy_id", how="left")

    return (
        joined
        .withColumn("month", F.date_format("claim_date", "yyyy-MM"))
        .groupBy("month", "policy_type", "incident_region")
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.sum("claim_amount_gbp").alias("total_claims_gbp"),
            F.sum(F.greatest(F.col("claim_amount_gbp") - F.col("excess"), F.lit(0.0))).alias("total_net_payout_gbp"),
            F.avg("claim_amount_gbp").alias("avg_claim_gbp"),
            F.sum("premium_monthly").alias("total_monthly_premium_gbp"),
            F.sum(F.when(F.col("severity_label") == "total_loss", F.col("claim_amount_gbp")).otherwise(0)).alias("total_loss_gbp"),
            F.sum(F.when(F.col("is_storm_related"), F.col("claim_amount_gbp")).otherwise(0)).alias("storm_claims_gbp"),
            F.countDistinct("policyholder_id").alias("unique_claimants"),
        )
        .withColumn("loss_ratio",
            F.round(F.col("total_net_payout_gbp") / F.greatest(F.col("total_monthly_premium_gbp"), F.lit(0.01)), 4))
    )

# ── summary_fraud_intelligence ────────────────────────────────────────────────

@dlt.table(name="summary_fraud_intelligence", comment="Fraud risk aggregations by UK region, claim type, and time.")
def summary_fraud_intelligence():
    return (
        silver("claims")
        .withColumn("month", F.date_format("claim_date", "yyyy-MM"))
        .groupBy("month", "incident_region", "claim_type", "is_storm_related")
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.sum(F.when(F.col("fraud_flag"), 1).otherwise(0)).alias("flagged_claims"),
            F.avg("fraud_risk_score").alias("avg_fraud_score"),
            F.max("fraud_risk_score").alias("max_fraud_score"),
            F.sum(F.when(F.col("fraud_flag"), F.col("claim_amount_gbp")).otherwise(0)).alias("flagged_claims_gbp"),
            F.sum("claim_amount_gbp").alias("total_claims_gbp"),
        )
        .withColumn("fraud_rate",
            F.round(F.col("flagged_claims") / F.greatest(F.col("total_claims"), F.lit(1)), 4))
        .withColumn("fraud_amount_ratio",
            F.round(F.col("flagged_claims_gbp") / F.greatest(F.col("total_claims_gbp"), F.lit(0.01)), 4))
    )

# ── summary_triage_performance ────────────────────────────────────────────────

@dlt.table(name="summary_triage_performance", comment="Assessment turnaround and assessor metrics.")
def summary_triage_performance():
    ass    = silver("claim_assessments")
    claims = silver("claims").select(
        "claim_id", "claim_date", "severity_label", "claim_type",
        "is_storm_related", "incident_region", "claim_amount_gbp"
    )

    return (
        ass
        .join(claims, on="claim_id", how="left")
        .withColumn("days_to_assess", F.datediff(F.col("assessment_date"), F.col("claim_date")))
        .withColumn("month",          F.date_format("claim_date", "yyyy-MM"))
        .groupBy("month", "assessor_tier", "damage_category", "recommended_action",
                 "severity_label", "is_storm_related")
        .agg(
            F.count("assessment_id").alias("total_assessments"),
            F.avg("days_to_assess").alias("avg_days_to_assess"),
            F.max("days_to_assess").alias("max_days_to_assess"),
            F.avg("estimated_repair_cost_gbp").alias("avg_repair_cost_gbp"),
            F.sum("total_assessment_cost_gbp").alias("total_repair_cost_gbp"),
            F.avg("labour_hours").alias("avg_labour_hours"),
            F.countDistinct("claim_id").alias("unique_claims_assessed"),
            F.avg(F.when(
                (F.col("severity_label") == "total_loss") & (F.col("recommended_action") == "write_off"), 1.0
            ).when(
                (F.col("severity_label") == "severe") & (F.col("recommended_action").isin("repair", "replace")), 1.0
            ).when(
                (F.col("severity_label") == "minor") & (F.col("recommended_action") == "repair"), 1.0
            ).otherwise(0.0)).alias("severity_action_alignment_rate"),
        )
    )
