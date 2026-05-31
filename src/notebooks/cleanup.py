# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cleanup: Drop All POC Data
# MAGIC
# MAGIC Drops everything created by this POC project:
# MAGIC - Landing Volume and all CSV files
# MAGIC - Bronze schema and all Delta tables (`raw_claims`)
# MAGIC - Silver schema and all Delta tables (`refined_claims`)
# MAGIC - Gold schemas and all Delta tables (`dimensions`, `facts`, `features`, `summary`)
# MAGIC
# MAGIC **Catalog drop is opt-in** — set `drop_catalogs = yes` only if the catalog
# MAGIC is exclusively used by this POC and you want a full teardown.
# MAGIC
# MAGIC ⚠️ This operation is irreversible. Verify catalog/schema names before running.

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze_dev",     "Bronze Catalog")
dbutils.widgets.text("silver_catalog", "silver_dev",     "Silver Catalog")
dbutils.widgets.text("gold_catalog",   "gold_dev",       "Gold Catalog")
dbutils.widgets.text("bronze_schema",  "raw_claims",     "Bronze Schema")
dbutils.widgets.text("silver_schema",  "refined_claims", "Silver Schema")
dbutils.widgets.dropdown("drop_catalogs", "no", ["no", "yes"], "Drop Catalogs too?")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog   = dbutils.widgets.get("gold_catalog")
bronze_schema  = dbutils.widgets.get("bronze_schema")
silver_schema  = dbutils.widgets.get("silver_schema")
drop_catalogs  = dbutils.widgets.get("drop_catalogs") == "yes"

GOLD_SCHEMAS = ["dimensions", "facts", "features", "summary"]
LANDING_VOL  = f"/Volumes/{bronze_catalog}/{bronze_schema}/landing"

print("=" * 60)
print("POC CLEANUP — resources to be dropped:")
print(f"  Volume  : {bronze_catalog}.{bronze_schema}.landing")
print(f"  Schema  : {bronze_catalog}.{bronze_schema}")
print(f"  Schema  : {silver_catalog}.{silver_schema}")
for gs in GOLD_SCHEMAS:
    print(f"  Schema  : {gold_catalog}.{gs}")
if drop_catalogs:
    print(f"  Catalog : {bronze_catalog}  ⚠️  CASCADE")
    print(f"  Catalog : {silver_catalog}  ⚠️  CASCADE")
    print(f"  Catalog : {gold_catalog}   ⚠️  CASCADE")
print("=" * 60)

# COMMAND ----------
# ── Step 1: Drop landing Volume (removes all CSV files) ───────────────────────

print(f"\n[1/4] Dropping landing Volume → {bronze_catalog}.{bronze_schema}.landing ...")
spark.sql(f"DROP VOLUME IF EXISTS `{bronze_catalog}`.`{bronze_schema}`.`landing`")
print(f"  ✓ Volume dropped (all landing CSV files removed)")

# COMMAND ----------
# ── Step 2: Drop bronze schema + all tables ───────────────────────────────────

print(f"\n[2/4] Dropping bronze schema → {bronze_catalog}.{bronze_schema} ...")
spark.sql(f"DROP SCHEMA IF EXISTS `{bronze_catalog}`.`{bronze_schema}` CASCADE")
print(f"  ✓ {bronze_catalog}.{bronze_schema} dropped")

# COMMAND ----------
# ── Step 3: Drop silver schema + all tables ───────────────────────────────────

print(f"\n[3/4] Dropping silver schema → {silver_catalog}.{silver_schema} ...")
spark.sql(f"DROP SCHEMA IF EXISTS `{silver_catalog}`.`{silver_schema}` CASCADE")
print(f"  ✓ {silver_catalog}.{silver_schema} dropped (includes quarantine table)")

# COMMAND ----------
# ── Step 4: Drop all gold schemas ─────────────────────────────────────────────

print(f"\n[4/4] Dropping gold schemas → {gold_catalog}.[dimensions|facts|features|summary] ...")
for gs in GOLD_SCHEMAS:
    spark.sql(f"DROP SCHEMA IF EXISTS `{gold_catalog}`.`{gs}` CASCADE")
    print(f"  ✓ {gold_catalog}.{gs} dropped")

# COMMAND ----------
# ── Optional: Drop catalogs ───────────────────────────────────────────────────

if drop_catalogs:
    print(f"\n[Optional] Dropping catalogs (CASCADE) ...")
    for cat in [bronze_catalog, silver_catalog, gold_catalog]:
        spark.sql(f"DROP CATALOG IF EXISTS `{cat}` CASCADE")
        print(f"  ✓ Catalog {cat} dropped")
else:
    print(f"\n[Optional] Skipping catalog drop (set drop_catalogs=yes to include).")
    print(f"  Empty catalogs remaining: {bronze_catalog}, {silver_catalog}, {gold_catalog}")

# COMMAND ----------
# ── Verify ────────────────────────────────────────────────────────────────────

print("\n── Verification ─────────────────────────────────────────────────")
suffix = bronze_catalog.split("_")[-1]
remaining = spark.sql("SHOW CATALOGS").filter(f"catalog LIKE '%_{suffix}'").collect()
if remaining:
    print(f"  Remaining catalogs ending with '_{suffix}':")
    for row in remaining:
        print(f"    {row[0]}")
        try:
            schemas = spark.sql(f"SHOW SCHEMAS IN `{row[0]}`").collect()
            for s in schemas:
                print(f"      └─ {s[0]}")
        except Exception:
            pass
else:
    print(f"  ✓ No catalogs ending with '_{suffix}' remain. Full cleanup complete.")
