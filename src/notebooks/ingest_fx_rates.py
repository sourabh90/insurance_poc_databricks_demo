# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## USD → GBP Exchange Rate Ingestion
# MAGIC
# MAGIC **Source**: frankfurter.app (European Central Bank rates, Mon–Fri)
# MAGIC **Mode**: First run loads 3 years of history; subsequent runs fetch incrementally.
# MAGIC **Table**: `bronze_dev.raw_claims.fx_rates_usd_gbp`

# COMMAND ----------

import requests
from datetime import date, timedelta
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

dbutils.widgets.text("bronze_catalog", "bronze_dev")
bronze_catalog = dbutils.widgets.get("bronze_catalog")

TARGET_TABLE  = f"{bronze_catalog}.raw_claims.fx_rates_usd_gbp"
HISTORY_START = "2022-06-01"   # ~3 years of history
FX_API_BASE   = "https://api.frankfurter.app"

# COMMAND ----------

def fetch_rates(start: str, end: str) -> list:
    """Fetch USD→GBP daily rates from frankfurter.app for a date range.
    Returns list of (date_str, rate) tuples — weekdays only (ECB schedule).
    """
    url = f"{FX_API_BASE}/{start}..{end}?from=USD&to=GBP"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [(dt, float(rates["GBP"])) for dt, rates in data.get("rates", {}).items()]

# COMMAND ----------

# Determine the window to fetch
table_exists = spark.catalog.tableExists(TARGET_TABLE)

if table_exists:
    max_date = spark.table(TARGET_TABLE).agg(F.max("rate_date")).collect()[0][0]
    fetch_from = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Incremental load from {fetch_from}")
else:
    fetch_from = HISTORY_START
    print(f"Initial load from {fetch_from} (3-year history)")

fetch_to = date.today().strftime("%Y-%m-%d")

if fetch_from > fetch_to:
    print("Table already up to date — nothing to fetch.")
    dbutils.notebook.exit("up_to_date")

# COMMAND ----------

# Fetch in annual chunks to stay within API limits
rows = []
year_start = fetch_from
while year_start <= fetch_to:
    year_end = min(f"{year_start[:4]}-12-31", fetch_to)
    print(f"  Fetching {year_start} → {year_end} ...")
    chunk = fetch_rates(year_start, year_end)
    rows.extend(chunk)
    next_year = str(int(year_start[:4]) + 1)
    year_start = f"{next_year}-01-01"

print(f"Fetched {len(rows)} rate records")

# COMMAND ----------

schema = StructType([
    StructField("rate_date_str", StringType(), False),
    StructField("usd_to_gbp",   DoubleType(), False),
])

df = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("rate_date",    F.to_date("rate_date_str"))
    .withColumn("ingested_at",  F.current_timestamp())
    .drop("rate_date_str")
    .select("rate_date", "usd_to_gbp", "ingested_at")
)

# COMMAND ----------

if not table_exists:
    df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"Created {TARGET_TABLE} with {df.count()} rows")
else:
    df.createOrReplaceTempView("new_rates")
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS tgt
        USING new_rates AS src ON tgt.rate_date = src.rate_date
        WHEN MATCHED THEN
            UPDATE SET tgt.usd_to_gbp = src.usd_to_gbp, tgt.ingested_at = src.ingested_at
        WHEN NOT MATCHED THEN
            INSERT (rate_date, usd_to_gbp, ingested_at)
            VALUES (src.rate_date, src.usd_to_gbp, src.ingested_at)
    """)
    print(f"Merged {len(rows)} records into {TARGET_TABLE}")

# COMMAND ----------

spark.table(TARGET_TABLE).orderBy(F.col("rate_date").desc()).show(5)
