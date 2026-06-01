# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: Catalogs, Schemas, and Landing Volume
# MAGIC
# MAGIC Creates all Unity Catalog resources for the insurance_poc_databricks_demo project.
# MAGIC
# MAGIC | Layer  | Catalog        | Schema(s)                              |
# MAGIC |--------|----------------|----------------------------------------|
# MAGIC | Bronze | bronze_dev/prod | raw_claims (+ landing Volume)         |
# MAGIC | Silver | silver_dev/prod | refined_claims                         |
# MAGIC | Gold   | gold_dev/prod   | dimensions, facts, features, summary   |

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze_dev",     "Bronze Catalog")
dbutils.widgets.text("silver_catalog", "silver_dev",     "Silver Catalog")
dbutils.widgets.text("gold_catalog",   "gold_dev",       "Gold Catalog")
dbutils.widgets.text("bronze_schema",  "raw_claims",     "Bronze Schema")
dbutils.widgets.text("silver_schema",  "refined_claims", "Silver Schema")
dbutils.widgets.text("app_service_principal", "", "App Service Principal (optional)")

bronze_catalog      = dbutils.widgets.get("bronze_catalog")
silver_catalog      = dbutils.widgets.get("silver_catalog")
gold_catalog        = dbutils.widgets.get("gold_catalog")
bronze_schema       = dbutils.widgets.get("bronze_schema")
silver_schema       = dbutils.widgets.get("silver_schema")
app_sp              = dbutils.widgets.get("app_service_principal").strip()

GOLD_SCHEMAS      = ["dimensions", "facts", "features", "summary"]
REFERENCE_SCHEMA  = "raw_reference"

print(f"Bronze    : {bronze_catalog}.{bronze_schema}")
print(f"Reference : {bronze_catalog}.{REFERENCE_SCHEMA}")
print(f"Silver    : {silver_catalog}.{silver_schema}")
print(f"Gold      : {gold_catalog}.{{{', '.join(GOLD_SCHEMAS)}}}")

# COMMAND ----------
# Create catalogs

for catalog in [bronze_catalog, silver_catalog, gold_catalog]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    print(f"✓ catalog: {catalog}")

# COMMAND ----------
# Create schemas

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{bronze_catalog}`.`{bronze_schema}`")
print(f"✓ schema: {bronze_catalog}.{bronze_schema}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{bronze_catalog}`.`{REFERENCE_SCHEMA}`")
print(f"✓ schema: {bronze_catalog}.{REFERENCE_SCHEMA}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{silver_catalog}`.`{silver_schema}`")
print(f"✓ schema: {silver_catalog}.{silver_schema}")

for gs in GOLD_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{gold_catalog}`.`{gs}`")
    print(f"✓ schema: {gold_catalog}.{gs}")

# COMMAND ----------
# Create landing Volume in bronze (external files dropped here by ingestion process)

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS `{bronze_catalog}`.`{bronze_schema}`.`landing`
""")

landing_path = f"/Volumes/{bronze_catalog}/{bronze_schema}/landing"
print(f"✓ volume : {bronze_catalog}.{bronze_schema}.landing")
print(f"  path   : {landing_path}")
print()
print("Expected subfolders (created by generate_synthetic_data notebook):")
for t in ["policyholders", "vehicles", "policies", "claims", "incidents", "claim_assessments"]:
    print(f"  {landing_path}/{t}/")

# COMMAND ----------
# Verify

suffix = bronze_catalog.split("_")[-1]
print(f"\nCatalogs ending with '_{suffix}':")
display(spark.sql("SHOW CATALOGS").filter(f"catalog LIKE '%_{suffix}'"))

print(f"\nSchemas in {bronze_catalog}:")
display(spark.sql(f"SHOW SCHEMAS IN `{bronze_catalog}`"))

# COMMAND ----------
# Grant Databricks App service principal access to billing-related resources

if app_sp:
    # app_service_principal must be the SP's applicationId (UUID), not the display name.
    # Find it: Databricks UI → Apps → <app> → Service principal → Application ID
    # Or: databricks apps get <app-name> --profile <profile> | jq .service_principal_client_id
    print(f"\nGranting permissions to app SP (applicationId): {app_sp}")
    grants = [
        # bronze_dev — FX rates reference table used by the billing app
        f"GRANT USE CATALOG ON CATALOG `{bronze_catalog}` TO `{app_sp}`",
        f"GRANT USE SCHEMA ON SCHEMA `{bronze_catalog}`.`{REFERENCE_SCHEMA}` TO `{app_sp}`",
        f"GRANT SELECT ON TABLE `{bronze_catalog}`.`{REFERENCE_SCHEMA}`.`fx_rates_usd_gbp` TO `{app_sp}`",
        # system.billing and system.lakeflow are accessible to SPs with
        # databricks-sql-access entitlement — granted automatically by Databricks Apps.
        # No explicit GRANT needed for system catalog tables.
    ]
    for stmt in grants:
        spark.sql(stmt)
        print(f"  ✓ {stmt.split(' TO ')[0].replace('GRANT ', '')}")
    print("\nGrants applied. Note: system.billing and system.lakeflow tables are")
    print("accessible to the app SP via its databricks-sql-access entitlement.")
else:
    print("\nSkipping grants — no app_service_principal provided.")
