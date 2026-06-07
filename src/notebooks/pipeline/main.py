# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## insurance_poc_databricks_demo — Main Notebook

# COMMAND ----------

catalog = spark.conf.get("bundle.catalog", "workspace")
schema = spark.conf.get("bundle.schema", "default")

print(f"Running in catalog: {catalog}, schema: {schema}")

# COMMAND ----------

df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(df)
