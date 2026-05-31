# Databricks notebook source
# Pipeline: Silver → Gold Features (ML)

import dlt
import pyspark.sql.functions as F

silver_catalog = spark.conf.get("silver_catalog", "silver_dev")
silver_schema  = spark.conf.get("silver_schema",  "refined_claims")

def silver(table: str):
    return spark.table(f"{silver_catalog}.{silver_schema}.{table}")

# ── claim_features ────────────────────────────────────────────────────────────

@dlt.table(name="claim_features", comment="ML feature table for severity prediction — grain: one row per claim.")
def claim_features():
    claims = silver("claims")
    pols   = silver("policies").select("policy_id", "policy_type", "excess", "coverage_limit", "premium_monthly")
    ph     = silver("policyholders").select("policyholder_id", "age", "driving_years", "driving_points")
    veh    = silver("vehicles").select("vehicle_id", "vehicle_type", "year", "estimated_value", "mileage")
    inc    = silver("incidents").select(
        "claim_id", "weather_condition", "road_condition", "visibility",
        "num_vehicles_involved", "witness_count", "police_report_filed"
    )
    ass_agg = (
        silver("claim_assessments")
        .groupBy("claim_id")
        .agg(
            F.count("assessment_id").alias("num_assessments"),
            F.avg("estimated_repair_cost_gbp").alias("avg_repair_cost_gbp"),
            F.max("estimated_repair_cost_gbp").alias("max_repair_cost_gbp"),
            F.sum("labour_hours").alias("total_labour_hours"),
            F.countDistinct("assessor_tier").alias("distinct_assessor_tiers"),
        )
    )

    return (
        claims
        .join(pols,    on="policy_id",       how="left")
        .join(ph,      on="policyholder_id", how="left")
        .join(veh,     on="vehicle_id",      how="left")
        .join(inc,     on="claim_id",        how="left")
        .join(ass_agg, on="claim_id",        how="left")
        .select(
            "claim_id",
            F.when(F.col("severity_label") == "minor",      0)
             .when(F.col("severity_label") == "moderate",   1)
             .when(F.col("severity_label") == "severe",     2)
             .when(F.col("severity_label") == "total_loss", 3)
             .otherwise(F.lit(None)).alias("severity_encoded"),
            F.col("severity_label"),
            # Claim type features
            F.when(F.col("claim_type") == "weather",   1).otherwise(0).alias("f_type_weather"),
            F.when(F.col("claim_type") == "collision", 1).otherwise(0).alias("f_type_collision"),
            F.when(F.col("claim_type") == "theft",     1).otherwise(0).alias("f_type_theft"),
            F.when(F.col("claim_type") == "vandalism", 1).otherwise(0).alias("f_type_vandalism"),
            F.when(F.col("claim_type") == "fire",      1).otherwise(0).alias("f_type_fire"),
            F.col("claim_amount_gbp").alias("f_claim_amount_gbp"),
            F.col("fraud_risk_score").alias("f_fraud_risk_score"),
            F.when(F.col("is_storm_related"), 1).otherwise(0).alias("f_is_storm"),
            # Policy features
            F.col("excess").alias("f_excess_gbp"),
            F.col("coverage_limit").alias("f_coverage_limit_gbp"),
            F.col("premium_monthly").alias("f_premium_monthly_gbp"),
            F.when(F.col("policy_type") == "Comprehensive",       1).otherwise(0).alias("f_pol_comprehensive"),
            F.when(F.col("policy_type") == "ThirdPartyFireTheft", 1).otherwise(0).alias("f_pol_tpft"),
            F.when(F.col("policy_type") == "ThirdPartyOnly",      1).otherwise(0).alias("f_pol_tpo"),
            # Policyholder features
            F.col("age").alias("f_ph_age"),
            F.col("driving_years").alias("f_driving_years"),
            F.col("driving_points").alias("f_driving_points"),
            # Vehicle features
            (F.lit(2026) - F.col("year")).alias("f_vehicle_age"),
            F.col("estimated_value").alias("f_vehicle_value_gbp"),
            (F.col("mileage") / 1000.0).alias("f_mileage_k"),
            F.when(F.col("vehicle_type").isin("SUV", "Crossover"), 1).otherwise(0).alias("f_vtype_suv"),
            F.when(F.col("vehicle_type") == "Estate", 1).otherwise(0).alias("f_vtype_estate"),
            # Incident features
            F.when(F.col("weather_condition") == "flood",       1).otherwise(0).alias("f_weather_flood"),
            F.when(F.col("weather_condition") == "heavy_rain",  1).otherwise(0).alias("f_weather_rain"),
            F.when(F.col("weather_condition") == "strong_wind", 1).otherwise(0).alias("f_weather_wind"),
            F.when(F.col("weather_condition") == "clear",       1).otherwise(0).alias("f_weather_clear"),
            F.when(F.col("road_condition") == "flooded",        1).otherwise(0).alias("f_road_flooded"),
            F.when(F.col("road_condition") == "icy",            1).otherwise(0).alias("f_road_icy"),
            F.when(F.col("visibility")     == "poor",           1).otherwise(0).alias("f_visibility_poor"),
            F.col("num_vehicles_involved").alias("f_num_vehicles"),
            F.col("witness_count").alias("f_witness_count"),
            F.when(F.col("police_report_filed") == "Y", 1).otherwise(0).alias("f_police_report"),
            # Assessment features
            F.coalesce("num_assessments",    F.lit(0)).alias("f_num_assessments"),
            F.coalesce("avg_repair_cost_gbp",F.lit(0.0)).alias("f_avg_repair_cost_gbp"),
            F.coalesce("max_repair_cost_gbp",F.lit(0.0)).alias("f_max_repair_cost_gbp"),
            F.coalesce("total_labour_hours", F.lit(0.0)).alias("f_total_labour_hours"),
        )
    )

# ── policyholder_risk_profile ─────────────────────────────────────────────────

@dlt.table(name="policyholder_risk_profile", comment="ML feature table for policyholder risk scoring — grain: one row per policyholder.")
def policyholder_risk_profile():
    ph     = silver("policyholders")
    claims = silver("claims")
    pols   = silver("policies").select("policy_id", "policyholder_id", "policy_type", "premium_monthly")

    claims_agg = (
        claims.groupBy("policyholder_id").agg(
            F.count("claim_id").alias("total_claims"),
            F.sum(F.when(F.col("is_storm_related"), 1).otherwise(0)).alias("storm_claims"),
            F.avg("claim_amount_gbp").alias("avg_claim_amount_gbp"),
            F.max("claim_amount_gbp").alias("max_claim_amount_gbp"),
            F.avg("fraud_risk_score").alias("avg_fraud_score"),
            F.max("fraud_risk_score").alias("max_fraud_score"),
            F.sum(F.when(F.col("fraud_flag"), 1).otherwise(0)).alias("flagged_claims"),
            F.sum(F.when(F.col("severity_label") == "total_loss", 1).otherwise(0)).alias("total_loss_claims"),
            F.sum(F.when(F.col("severity_label") == "severe",     1).otherwise(0)).alias("severe_claims"),
        )
    )
    pol_agg = (
        pols.groupBy("policyholder_id").agg(
            F.count("policy_id").alias("total_policies"),
            F.avg("premium_monthly").alias("avg_premium_monthly_gbp"),
        )
    )

    return (
        ph.select("policyholder_id", "age", "gender", "region", "driving_years", "driving_points")
        .join(claims_agg, on="policyholder_id", how="left")
        .join(pol_agg,    on="policyholder_id", how="left")
        .withColumn("total_claims",       F.coalesce("total_claims",       F.lit(0)))
        .withColumn("total_policies",     F.coalesce("total_policies",     F.lit(0)))
        .withColumn("avg_claim_amount_gbp", F.coalesce("avg_claim_amount_gbp", F.lit(0.0)))
        .withColumn("avg_fraud_score",    F.coalesce("avg_fraud_score",    F.lit(0.0)))
        .withColumn("flagged_claims",     F.coalesce("flagged_claims",     F.lit(0)))
        .withColumn("total_loss_claims",  F.coalesce("total_loss_claims",  F.lit(0)))
        .withColumn("claim_frequency",    F.col("total_claims") / F.greatest(F.col("driving_years"), F.lit(1)))
        .withColumn("risk_score",
            F.least(F.lit(1.0),
                (F.col("driving_points") / 12.0 * 0.2) +   # normalised to UK max (12 points)
                (F.col("claim_frequency") * 0.3) +
                (F.col("avg_fraud_score") * 0.4) +
                (F.col("total_loss_claims") * 0.1)
            ))
        .withColumn("risk_tier",
            F.when(F.col("risk_score") < 0.2, F.lit("low"))
             .when(F.col("risk_score") < 0.5, F.lit("medium"))
             .when(F.col("risk_score") < 0.7, F.lit("high"))
             .otherwise(F.lit("very_high")))
    )
