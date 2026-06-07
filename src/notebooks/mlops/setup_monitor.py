# Databricks notebook source
# Setup: Lakehouse Monitor on claim_predictions
# Creates an InferenceLog monitor that tracks model accuracy, prediction drift,
# and per-class distributions over time across batch scoring runs.
# Run once to set up; thereafter trigger a refresh after each batch_score run.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lakehouse Monitoring Setup — Claim Severity Predictions
# MAGIC
# MAGIC Creates a **Lakehouse Monitor** (InferenceLog type) on the
# MAGIC `claim_predictions` table. After each `batch_score` run adds a new
# MAGIC cohort of predictions, the monitor computes:
# MAGIC
# MAGIC | Output table | Contents |
# MAGIC |---|---|
# MAGIC | `monitoring.claim_predictions_profile_metrics` | Column-level stats per time window (mean, stddev, % nulls, histogram) |
# MAGIC | `monitoring.claim_predictions_drift_metrics` | Drift scores vs previous window (KL divergence, chi-squared, JS distance) |
# MAGIC
# MAGIC The monitor slices metrics by `predicted_severity_label` and `model_version`
# MAGIC so you can compare behaviour across model releases.
# MAGIC
# MAGIC > **First run note:** Drift metrics require at least two time windows of data.
# MAGIC > Run `batch_score` multiple times (with different data batches) to build
# MAGIC > enough history for meaningful drift detection.

# COMMAND ----------
dbutils.widgets.text("gold_catalog",  "gold_dev", "Gold Catalog")
dbutils.widgets.text("wait_minutes",  "10",       "Max wait for refresh (minutes)")

gold_catalog   = dbutils.widgets.get("gold_catalog")
wait_minutes   = int(dbutils.widgets.get("wait_minutes"))

table_name     = f"{gold_catalog}.features.claim_predictions"
output_schema  = f"{gold_catalog}.monitoring"

print(f"Table    : {table_name}")
print(f"Output   : {output_schema}")

# COMMAND ----------
# MAGIC %md ### 1. Create monitoring output schema

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{gold_catalog}`.`monitoring`")
print(f"✓ Schema: {output_schema}")

# COMMAND ----------
# MAGIC %md ### 2. Create monitor (or trigger refresh if it already exists)

# COMMAND ----------
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorInferenceLog,
    MonitorInferenceLogProblemType,
)

w = WorkspaceClient()

def _get_monitor():
    try:
        return w.quality_monitors.get(table_name=table_name)
    except Exception as e:
        if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
            return None
        raise

existing = _get_monitor()

if existing:
    print("Monitor already exists — triggering refresh.")
    refresh = w.quality_monitors.run_refresh(table_name=table_name)
    refresh_id = refresh.refresh_id
    print(f"✓ Refresh triggered  (id: {refresh_id})")
else:
    monitor = w.quality_monitors.create(
        table_name=table_name,
        inference_log=MonitorInferenceLog(
            # Time column used to assign rows to daily / weekly windows
            timestamp_col="scored_at",
            # Model output and ground-truth label
            prediction_col="predicted_severity",
            label_col="severity_encoded",
            # Slice metrics by model version — shows per-release accuracy trends
            model_id_col="model_version",
            # Granularities for aggregation windows
            granularities=["1 day", "1 week"],
            problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
        ),
        output_schema_name=output_schema,
        # Slice charts by label and model version in the built-in dashboard
        slicing_exprs=["predicted_severity_label", "model_version"],
    )
    print(f"✓ Monitor created for {table_name}")

    # Trigger the first refresh so metrics are available immediately
    refresh = w.quality_monitors.run_refresh(table_name=table_name)
    refresh_id = refresh.refresh_id
    print(f"✓ Initial refresh triggered  (id: {refresh_id})")

# COMMAND ----------
# MAGIC %md ### 3. Wait for refresh to complete

# COMMAND ----------
deadline = time.time() + wait_minutes * 60
state    = "PENDING"

while time.time() < deadline:
    info  = w.quality_monitors.get_refresh(table_name=table_name, refresh_id=refresh_id)
    state = info.state.value if info.state else "UNKNOWN"
    print(f"  Refresh state: {state}")
    if state in ("SUCCESS", "FAILED", "CANCELED"):
        break
    time.sleep(30)

if state == "SUCCESS":
    print(f"\n✓ Refresh complete — metrics tables ready in {output_schema}")
elif state == "PENDING" or state == "RUNNING":
    print(f"\n⚠ Refresh still running after {wait_minutes} min.")
    print(f"  Check progress: w.quality_monitors.get_refresh('{table_name}', '{refresh_id}')")
else:
    print(f"\n✗ Refresh ended with state: {state}")

# COMMAND ----------
# MAGIC %md ### 4. Profile metrics — column statistics per time window

# COMMAND ----------
profile_table = f"`{gold_catalog}`.`monitoring`.`claim_predictions_profile_metrics`"

try:
    display(
        spark.sql(f"""
        SELECT
          window,
          column_name,
          data_type,
          ROUND(percent_null,   2) AS pct_null,
          ROUND(mean,           4) AS mean,
          ROUND(std_dev,        4) AS std_dev,
          ROUND(min_value,      4) AS min,
          ROUND(max_value,      4) AS max
        FROM {profile_table}
        WHERE column_name IN (
          'predicted_severity', 'severity_encoded',
          'prob_minor', 'prob_moderate', 'prob_severe', 'prob_total_loss'
        )
        ORDER BY window DESC, column_name
        LIMIT 50
        """)
    )
except Exception as e:
    print(f"Profile metrics not yet available: {e}")
    print("Re-run this cell after the refresh completes.")

# COMMAND ----------
# MAGIC %md ### 5. Drift metrics — distribution shift vs previous window

# COMMAND ----------
drift_table = f"`{gold_catalog}`.`monitoring`.`claim_predictions_drift_metrics`"

try:
    display(
        spark.sql(f"""
        SELECT
          window,
          column_name,
          ROUND(js_distance,   4) AS js_distance,
          ROUND(kl_divergence, 4) AS kl_divergence,
          drift_detected
        FROM {drift_table}
        WHERE column_name IN (
          'predicted_severity', 'prob_minor', 'prob_moderate',
          'prob_severe', 'prob_total_loss'
        )
        ORDER BY window DESC, js_distance DESC
        LIMIT 30
        """)
    )
except Exception as e:
    print(f"Drift metrics not yet available: {e}")
    print("Drift requires at least 2 time windows — run batch_score multiple times to build history.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Monitor summary
# MAGIC
# MAGIC ```
# MAGIC Profile metrics  : {gold_catalog}.monitoring.claim_predictions_profile_metrics
# MAGIC Drift metrics    : {gold_catalog}.monitoring.claim_predictions_drift_metrics
# MAGIC ```
# MAGIC
# MAGIC **Drift interpretation:**
# MAGIC | JS distance | Meaning |
# MAGIC |---|---|
# MAGIC | < 0.05 | No significant drift |
# MAGIC | 0.05 – 0.10 | Mild drift — monitor closely |
# MAGIC | > 0.10 | Significant drift — consider retraining |
# MAGIC
# MAGIC **Recommended workflow after each `batch_score` run:**
# MAGIC ```bash
# MAGIC databricks bundle run insurance_poc_validate_model   # validate model quality
# MAGIC databricks bundle run insurance_poc_batch_inference  # score new claims
# MAGIC databricks bundle run insurance_poc_setup_monitor    # refresh monitoring metrics
# MAGIC ```

# COMMAND ----------
monitor_info = _get_monitor()
if monitor_info:
    print(f"Monitor status   : {monitor_info.status}")
    print(f"Profile table    : {output_schema}.claim_predictions_profile_metrics")
    print(f"Drift table      : {output_schema}.claim_predictions_drift_metrics")
    if hasattr(monitor_info, 'dashboard_id') and monitor_info.dashboard_id:
        print(f"Dashboard ID     : {monitor_info.dashboard_id}")
        print(f"View in UI       : Catalog Explorer → {table_name} → Quality tab")
