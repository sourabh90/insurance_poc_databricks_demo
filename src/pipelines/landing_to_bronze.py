# Databricks notebook source
# Pipeline: Volume → Bronze (Autoloader)
#
# Reads CSV files from the landing Volume using Autoloader and loads them as
# streaming Delta tables in the bronze catalog (raw_claims schema).
# One DLT streaming table per source entity, no business transformations applied.

import dlt
import pyspark.sql.functions as F

bronze_catalog = spark.conf.get("bronze_catalog", "bronze_dev")
bronze_schema  = spark.conf.get("bronze_schema",  "raw_claims")
landing_path   = f"/Volumes/{bronze_catalog}/{bronze_schema}/landing"
schema_hints   = f"/Volumes/{bronze_catalog}/{bronze_schema}/landing/_schema_hints"

# ── Autoloader factory ────────────────────────────────────────────────────────

def autoloader_table(table_name: str, comment: str):
    @dlt.table(name=table_name, comment=comment)
    @dlt.expect("no_rescued_data", "_rescued_data IS NULL")
    def _load():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.schemaLocation", f"{schema_hints}/{table_name}")
            .load(f"{landing_path}/{table_name}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
    return _load


# ── Register one streaming table per source entity ───────────────────────────

autoloader_table("policyholders",     "Raw policyholders loaded from landing volume")
autoloader_table("vehicles",          "Raw vehicles loaded from landing volume")
autoloader_table("policies",          "Raw policies loaded from landing volume")
autoloader_table("claims",            "Raw claims loaded from landing volume — includes hailstorm flag")
autoloader_table("incidents",         "Raw incident records loaded from landing volume")
autoloader_table("claim_assessments", "Raw claim assessments loaded from landing volume")
