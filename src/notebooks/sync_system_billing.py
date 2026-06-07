# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sync: system.billing + system.lakeflow → monitoring catalog
# MAGIC
# MAGIC Replicates six system tables into a user-owned catalog so the
# MAGIC Databricks App service principal can query them without needing
# MAGIC metastore-admin grants on the system catalog.
# MAGIC
# MAGIC | Target table                                    | Source                                          | Strategy                                  |
# MAGIC |-------------------------------------------------|-------------------------------------------------|-------------------------------------------|
# MAGIC | system_billing.usage                            | system.billing.usage                            | Incremental append (7-day overlap window) |
# MAGIC | system_billing.list_prices                      | system.billing.list_prices                      | Full replace                              |
# MAGIC | system_billing.pipelines                        | system.lakeflow.pipelines                       | Full replace                              |
# MAGIC | system_billing.query_history                    | system.query.history                            | Incremental append (7-day overlap window) |
# MAGIC | system_billing.pipeline_update_timeline         | system.lakeflow.pipeline_update_timeline        | Incremental append (7-day overlap window) |
# MAGIC | system_billing.job_task_run_timeline            | system.lakeflow.job_task_run_timeline           | Incremental append (7-day overlap window) |

# COMMAND ----------

from datetime import date, timedelta

dbutils.widgets.text("monitoring_catalog",    "monitoring_dev", "Monitoring Catalog")
dbutils.widgets.text("app_service_principal", "",               "App Service Principal (optional)")

monitoring_catalog = dbutils.widgets.get("monitoring_catalog")
app_sp             = dbutils.widgets.get("app_service_principal").strip()
schema             = "system_billing"
full_schema        = f"`{monitoring_catalog}`.`{schema}`"

print(f"Target catalog : {monitoring_catalog}")
print(f"Target schema  : {monitoring_catalog}.{schema}")
print(f"App SP         : {app_sp or '(none — skipping grants)'}")

# COMMAND ----------
# Ensure schema exists

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
print(f"✓ schema: {monitoring_catalog}.{schema}")

# COMMAND ----------
# list_prices — full replace (small lookup, prices update over time)

spark.sql(f"""
  CREATE OR REPLACE TABLE {full_schema}.list_prices
  AS SELECT * FROM system.billing.list_prices
""")
count = spark.sql(f"SELECT COUNT(*) FROM {full_schema}.list_prices").collect()[0][0]
print(f"✓ list_prices : {count:,} rows (full replace)")

# COMMAND ----------
# pipelines — full replace (small deduplicated lookup)

spark.sql(f"""
  CREATE OR REPLACE TABLE {full_schema}.pipelines
  AS
  SELECT pipeline_id, FIRST(name) AS name
  FROM system.lakeflow.pipelines
  GROUP BY pipeline_id
""")
count = spark.sql(f"SELECT COUNT(*) FROM {full_schema}.pipelines").collect()[0][0]
print(f"✓ pipelines   : {count:,} rows (full replace)")

# COMMAND ----------
# usage — incremental append with 7-day overlap window
#
# First run  : full load from system.billing.usage
# Subsequent : delete last 7 days, reinsert to pick up late-arriving records,
#              then insert any new days beyond the previous high-water mark

table_ref = f"{full_schema}.usage"
table_exists = spark.catalog.tableExists(f"{monitoring_catalog}.{schema}.usage")

if not table_exists:
    spark.sql(f"""
      CREATE TABLE {table_ref}
      AS SELECT * FROM system.billing.usage
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {table_ref}").collect()[0][0]
    print(f"✓ usage       : {count:,} rows (initial full load)")
else:
    max_date_row = spark.sql(f"SELECT MAX(usage_date) FROM {table_ref}").collect()[0][0]
    if max_date_row is None:
        cutoff = date(2020, 1, 1)
    else:
        cutoff = max_date_row - timedelta(days=7)

    spark.sql(f"DELETE FROM {table_ref} WHERE usage_date >= '{cutoff}'")
    spark.sql(f"""
      INSERT INTO {table_ref}
      SELECT * FROM system.billing.usage
      WHERE usage_date >= '{cutoff}'
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {table_ref}").collect()[0][0]
    print(f"✓ usage       : {count:,} rows total (incremental from {cutoff})")

# COMMAND ----------
# query_history — incremental append with 7-day overlap window

qh_ref = f"{full_schema}.query_history"
qh_exists = spark.catalog.tableExists(f"{monitoring_catalog}.{schema}.query_history")

if not qh_exists:
    spark.sql(f"""
      CREATE TABLE {qh_ref}
      AS SELECT * FROM system.query.history
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {qh_ref}").collect()[0][0]
    print(f"✓ query_history : {count:,} rows (initial full load)")
else:
    max_date_row = spark.sql(f"SELECT MAX(DATE(start_time)) FROM {qh_ref}").collect()[0][0]
    if max_date_row is None:
        cutoff = date(2020, 1, 1)
    else:
        cutoff = max_date_row - timedelta(days=7)

    spark.sql(f"DELETE FROM {qh_ref} WHERE DATE(start_time) >= '{cutoff}'")
    spark.sql(f"""
      INSERT INTO {qh_ref}
      SELECT * FROM system.query.history
      WHERE DATE(start_time) >= '{cutoff}'
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {qh_ref}").collect()[0][0]
    print(f"✓ query_history : {count:,} rows total (incremental from {cutoff})")

# COMMAND ----------
# job_task_run_timeline — incremental append with 7-day overlap window
# Requires metastore admin to enable system.lakeflow tables for the workspace.

try:
    jt_ref = f"{full_schema}.job_task_run_timeline"
    jt_exists = spark.catalog.tableExists(f"{monitoring_catalog}.{schema}.job_task_run_timeline")

    if not jt_exists:
        spark.sql(f"""
          CREATE TABLE {jt_ref}
          AS SELECT * FROM system.lakeflow.job_task_run_timeline
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM {jt_ref}").collect()[0][0]
        print(f"✓ job_task_run_timeline : {count:,} rows (initial full load)")
    else:
        max_date_row = spark.sql(f"SELECT MAX(DATE(period_start_time)) FROM {jt_ref}").collect()[0][0]
        if max_date_row is None:
            cutoff = date(2020, 1, 1)
        else:
            cutoff = max_date_row - timedelta(days=7)

        spark.sql(f"DELETE FROM {jt_ref} WHERE DATE(period_start_time) >= '{cutoff}'")
        spark.sql(f"""
          INSERT INTO {jt_ref}
          SELECT * FROM system.lakeflow.job_task_run_timeline
          WHERE DATE(period_start_time) >= '{cutoff}'
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM {jt_ref}").collect()[0][0]
        print(f"✓ job_task_run_timeline : {count:,} rows total (incremental from {cutoff})")
except Exception as e:
    print(f"⚠ job_task_run_timeline skipped — system.lakeflow not enabled for this workspace: {e}")

# COMMAND ----------
# pipeline_update_timeline — incremental append with 7-day overlap window
# Requires metastore admin to enable system.lakeflow tables for the workspace.

try:
    put_ref = f"{full_schema}.pipeline_update_timeline"
    put_exists = spark.catalog.tableExists(f"{monitoring_catalog}.{schema}.pipeline_update_timeline")

    if not put_exists:
        spark.sql(f"""
          CREATE TABLE {put_ref}
          AS
          SELECT t.*, p.name AS pipeline_name
          FROM system.lakeflow.pipeline_update_timeline t
          LEFT JOIN system.lakeflow.pipelines p ON t.pipeline_id = p.pipeline_id
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM {put_ref}").collect()[0][0]
        print(f"✓ pipeline_update_timeline : {count:,} rows (initial full load)")
    else:
        max_date_row = spark.sql(f"SELECT MAX(DATE(period_start_time)) FROM {put_ref}").collect()[0][0]
        if max_date_row is None:
            cutoff = date(2020, 1, 1)
        else:
            cutoff = max_date_row - timedelta(days=7)

        spark.sql(f"DELETE FROM {put_ref} WHERE DATE(period_start_time) >= '{cutoff}'")
        spark.sql(f"""
          INSERT INTO {put_ref}
          SELECT t.*, p.name AS pipeline_name
          FROM system.lakeflow.pipeline_update_timeline t
          LEFT JOIN system.lakeflow.pipelines p ON t.pipeline_id = p.pipeline_id
          WHERE DATE(t.period_start_time) >= '{cutoff}'
        """)
        count = spark.sql(f"SELECT COUNT(*) FROM {put_ref}").collect()[0][0]
        print(f"✓ pipeline_update_timeline : {count:,} rows total (incremental from {cutoff})")
except Exception as e:
    print(f"⚠ pipeline_update_timeline skipped — system.lakeflow not enabled for this workspace: {e}")

# COMMAND ----------
# Grant app SP SELECT on all tables (USE CATALOG + USE SCHEMA granted by setup_job)

if app_sp:
    print(f"\nGranting SELECT to app SP: {app_sp}")
    for tbl in ["usage", "list_prices", "pipelines",
                "query_history", "pipeline_update_timeline", "job_task_run_timeline"]:
        if spark.catalog.tableExists(f"{monitoring_catalog}.{schema}.{tbl}"):
            spark.sql(f"GRANT SELECT ON TABLE {full_schema}.{tbl} TO `{app_sp}`")
            print(f"  ✓ SELECT on {monitoring_catalog}.{schema}.{tbl}")
        else:
            print(f"  ⚠ Skipped {tbl} — table does not exist (system table likely not enabled)")
else:
    print("\nSkipping table grants — no app_service_principal provided.")
