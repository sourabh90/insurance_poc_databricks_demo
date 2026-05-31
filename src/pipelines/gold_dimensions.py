# Databricks notebook source
# Pipeline: Silver → Gold Dimensions

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

silver_catalog = spark.conf.get("silver_catalog", "silver_dev")
silver_schema  = spark.conf.get("silver_schema",  "refined_claims")

def silver(table: str):
    return spark.table(f"{silver_catalog}.{silver_schema}.{table}")

# ── dim_date ──────────────────────────────────────────────────────────────────

@dlt.table(name="dim_date", comment="Date dimension — covers 2023-01-01 to 2027-12-31")
def dim_date():
    return (
        spark.range(0, 365 * 5)
        .withColumn("date",             F.date_add(F.lit("2023-01-01").cast("date"), F.col("id").cast(IntegerType())))
        .withColumn("date_key",         F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year",             F.year("date"))
        .withColumn("quarter",          F.quarter("date"))
        .withColumn("month",            F.month("date"))
        .withColumn("month_name",       F.date_format(F.col("date"), "MMMM"))
        .withColumn("week_of_year",     F.weekofyear("date"))
        .withColumn("day_of_month",     F.dayofmonth("date"))
        .withColumn("day_of_week",      F.dayofweek("date"))
        .withColumn("day_name",         F.date_format(F.col("date"), "EEEE"))
        .withColumn("is_weekend",       F.dayofweek("date").isin(1, 7))
        .withColumn("is_storm_period",
            (F.col("date") >= F.lit("2026-02-03").cast("date")) &
            (F.col("date") <= F.lit("2026-02-17").cast("date")))
        .drop("id")
    )

# ── dim_geography ─────────────────────────────────────────────────────────────

@dlt.table(name="dim_geography", comment="Distinct UK region/city combinations from claims and policyholders")
def dim_geography():
    claims = silver("claims").select("incident_region", "incident_city").distinct()
    ph     = silver("policyholders").select(
        F.col("region").alias("incident_region"),
        F.col("city").alias("incident_city")
    ).distinct()
    storm_regions = ["West Yorkshire", "South Yorkshire", "Somerset", "Greater Manchester", "Devon"]
    return (
        claims.unionByName(ph).distinct()
        .withColumn("is_storm_region", F.col("incident_region").isin(storm_regions))
        .withColumn("uk_area",
            F.when(F.col("incident_region").isin("Greater London"), F.lit("London"))
             .when(F.col("incident_region").isin("West Yorkshire", "South Yorkshire", "North Yorkshire"), F.lit("Yorkshire"))
             .when(F.col("incident_region").isin("West Midlands", "Nottinghamshire"), F.lit("Midlands"))
             .when(F.col("incident_region").isin("Greater Manchester", "Merseyside", "Lancashire"), F.lit("North West"))
             .when(F.col("incident_region").isin("Tyne and Wear"), F.lit("North East"))
             .when(F.col("incident_region").isin("Somerset", "Devon", "Bristol"), F.lit("South West"))
             .when(F.col("incident_region").isin("Hampshire", "Kent", "Essex"), F.lit("South East"))
             .otherwise(F.lit("Other")))
        .withColumn("geo_key", F.concat_ws("-", F.col("incident_region"), F.col("incident_city")))
    )

# ── dim_policyholder ──────────────────────────────────────────────────────────

@dlt.table(name="dim_policyholder", comment="Policyholder dimension with UK risk profile")
def dim_policyholder():
    return (
        silver("policyholders")
        .select(
            "policyholder_id", "first_name", "last_name",
            "gender", "age", "region", "city", "postcode",
            "driving_years", "driving_points",
        )
        .withColumn("age_band",
            F.when(F.col("age") < 25, F.lit("17-24"))
             .when(F.col("age") < 35, F.lit("25-34"))
             .when(F.col("age") < 45, F.lit("35-44"))
             .when(F.col("age") < 55, F.lit("45-54"))
             .when(F.col("age") < 65, F.lit("55-64"))
             .otherwise(F.lit("65+")))
        # UK penalty points: 0=clean, 3-6=minor, 9+=major (ban at 12)
        .withColumn("risk_tier",
            F.when(F.col("driving_points") == 0, F.lit("low"))
             .when(F.col("driving_points") <= 6, F.lit("medium"))
             .otherwise(F.lit("high")))
    )

# ── dim_vehicle ───────────────────────────────────────────────────────────────

@dlt.table(name="dim_vehicle", comment="Vehicle dimension with age and value bands in GBP")
def dim_vehicle():
    return (
        silver("vehicles")
        .select(
            "vehicle_id", "policyholder_id",
            "make", "vehicle_type", "year", "colour",
            "estimated_value", "mileage",
        )
        .withColumn("vehicle_age", F.lit(2026) - F.col("year"))
        .withColumn("vehicle_age_band",
            F.when(F.col("vehicle_age") <= 3,  F.lit("0-3 years"))
             .when(F.col("vehicle_age") <= 7,  F.lit("4-7 years"))
             .when(F.col("vehicle_age") <= 12, F.lit("8-12 years"))
             .otherwise(F.lit("13+ years")))
        .withColumn("value_band_gbp",
            F.when(F.col("estimated_value") < 8000,  F.lit("<£8K"))
             .when(F.col("estimated_value") < 20000, F.lit("£8K-£20K"))
             .when(F.col("estimated_value") < 40000, F.lit("£20K-£40K"))
             .otherwise(F.lit("£40K+")))
    )

# ── dim_policy ────────────────────────────────────────────────────────────────

@dlt.table(name="dim_policy", comment="Policy dimension with UK coverage attributes")
def dim_policy():
    return (
        silver("policies")
        .select(
            "policy_id", "policyholder_id", "vehicle_id",
            "policy_type", "coverage_type", "status",
            "premium_monthly", "excess", "coverage_limit",
            "start_date", "end_date",
        )
        .withColumn("annual_premium_gbp", F.round(F.col("premium_monthly") * 12, 2))
        .withColumn("policy_duration_days", F.datediff(F.col("end_date"), F.col("start_date")))
        .withColumn("premium_band",
            F.when(F.col("premium_monthly") < 60,  F.lit("<£60/mo"))
             .when(F.col("premium_monthly") < 100, F.lit("£60-£100/mo"))
             .when(F.col("premium_monthly") < 200, F.lit("£100-£200/mo"))
             .otherwise(F.lit("£200+/mo")))
    )
