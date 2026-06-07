# Databricks notebook source
# Batch Inference: Claim Severity Classifier
# Reads  : {gold_catalog}.features.claim_features  (120K rows, 36 features)
# Writes : {gold_catalog}.features.claim_predictions

# COMMAND ----------
# MAGIC %pip install lightgbm -q

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Batch Inference — Claim Severity Classifier
# MAGIC
# MAGIC Loads the `champion` model from UC registry, scores all claims in
# MAGIC `claim_features` using a Pandas UDF (parallel Spark execution), and
# MAGIC writes predictions + per-class probabilities to `claim_predictions`.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Input**  | `{gold_catalog}.features.claim_features` |
# MAGIC | **Model**  | `{gold_catalog}.models.{ml_model_name}@champion` |
# MAGIC | **Output** | `{gold_catalog}.features.claim_predictions` |
# MAGIC | **Method** | Pandas UDF with broadcast model — parallel across Spark partitions |

# COMMAND ----------
dbutils.widgets.text("gold_catalog",  "gold_dev",                  "Gold Catalog")
dbutils.widgets.text("ml_model_name", "claim_severity_classifier",  "UC Model Name")

gold_catalog  = dbutils.widgets.get("gold_catalog")
model_name    = dbutils.widgets.get("ml_model_name")
uc_model_path = f"{gold_catalog}.models.{model_name}"

print(f"Input  : {gold_catalog}.features.claim_features")
print(f"Model  : {uc_model_path}@champion")
print(f"Output : {gold_catalog}.features.claim_predictions")

# COMMAND ----------
import mlflow
import mlflow.sklearn
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DoubleType
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")

# Resolve champion alias → version number (stored for lineage)
client        = MlflowClient()
mv            = client.get_model_version_by_alias(uc_model_path, "champion")
model_version = mv.version
print(f"✓ Champion model version : {model_version}")

# COMMAND ----------
# MAGIC %md ### 1. Load model and create Pandas UDF

# COMMAND ----------
FEATURE_COLS = [
    # Claim type (5)
    "f_type_weather", "f_type_collision", "f_type_theft", "f_type_vandalism", "f_type_fire",
    # Claim attributes (3)
    "f_claim_amount_gbp", "f_fraud_risk_score", "f_is_storm",
    # Policy (6)
    "f_excess_gbp", "f_coverage_limit_gbp", "f_premium_monthly_gbp",
    "f_pol_comprehensive", "f_pol_tpft", "f_pol_tpo",
    # Policyholder (3)
    "f_ph_age", "f_driving_years", "f_driving_points",
    # Vehicle (5)
    "f_vehicle_age", "f_vehicle_value_gbp", "f_mileage_k",
    "f_vtype_suv", "f_vtype_estate",
    # Incident (10)
    "f_weather_flood", "f_weather_rain", "f_weather_wind", "f_weather_clear",
    "f_road_flooded", "f_road_icy", "f_visibility_poor",
    "f_num_vehicles", "f_witness_count", "f_police_report",
    # Assessment aggregates (4)
    "f_num_assessments", "f_avg_repair_cost_gbp", "f_max_repair_cost_gbp", "f_total_labour_hours",
]

LABELS = ["minor", "moderate", "severe", "total_loss"]

# Load sklearn model and broadcast across the cluster so each executor
# gets a local copy rather than re-fetching from the registry per partition.
sklearn_model = mlflow.sklearn.load_model(f"models:/{uc_model_path}@champion")
bc_model      = spark.sparkContext.broadcast(sklearn_model)
print(f"✓ Model loaded and broadcast ({len(FEATURE_COLS)} features, {len(LABELS)} classes)")

# Pandas UDF — receives one column per feature, returns array<double> of
# class probabilities [P(minor), P(moderate), P(severe), P(total_loss)].
@F.pandas_udf(ArrayType(DoubleType()))
def score_udf(*cols):
    import pandas as pd
    X = pd.concat(list(cols), axis=1)
    X.columns = FEATURE_COLS
    return pd.Series(list(bc_model.value.predict_proba(X.fillna(0).astype(float))))

# COMMAND ----------
# MAGIC %md ### 2. Score all claims

# COMMAND ----------
predictions = (
    spark.table(f"{gold_catalog}.features.claim_features")
    .withColumn("probs", score_udf(*[F.col(c) for c in FEATURE_COLS]))
    # Per-class probabilities
    .withColumn("prob_minor",      F.col("probs")[0])
    .withColumn("prob_moderate",   F.col("probs")[1])
    .withColumn("prob_severe",     F.col("probs")[2])
    .withColumn("prob_total_loss", F.col("probs")[3])
    # Predicted class — index of max probability (array_position is 1-based)
    .withColumn("predicted_severity",
        (F.array_position(F.col("probs"), F.array_max(F.col("probs"))) - 1).cast("int"))
    .withColumn("predicted_severity_label",
        F.when(F.col("predicted_severity") == 0, "minor")
         .when(F.col("predicted_severity") == 1, "moderate")
         .when(F.col("predicted_severity") == 2, "severe")
         .otherwise("total_loss"))
    # Lineage columns
    .withColumn("model_version", F.lit(model_version))
    .withColumn("scored_at",     F.current_timestamp())
    .select(
        "claim_id",
        "severity_encoded",          # actual label — kept for offline evaluation
        "severity_label",
        "predicted_severity",
        "predicted_severity_label",
        "prob_minor", "prob_moderate", "prob_severe", "prob_total_loss",
        "model_version",
        "scored_at",
    )
)

# COMMAND ----------
# MAGIC %md ### 3. Write to gold features schema

# COMMAND ----------
(
    predictions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{gold_catalog}.features.claim_predictions")
)

count = spark.table(f"{gold_catalog}.features.claim_predictions").count()
print(f"✓ Written {count:,} rows → {gold_catalog}.features.claim_predictions")

# COMMAND ----------
# MAGIC %md ### 4. Offline evaluation — predicted vs actual

# COMMAND ----------
results = spark.table(f"{gold_catalog}.features.claim_predictions")

total   = results.count()
correct = results.filter(F.col("predicted_severity") == F.col("severity_encoded")).count()
print(f"Overall accuracy : {correct / total:.4f}  ({correct:,} / {total:,})\n")

print("Per-class accuracy:")
for i, label in enumerate(LABELS):
    cls_df      = results.filter(F.col("severity_encoded") == i)
    cls_total   = cls_df.count()
    cls_correct = cls_df.filter(F.col("predicted_severity") == i).count()
    print(f"  {label:12s}: {cls_correct / cls_total:.4f}  ({cls_correct:,} / {cls_total:,})")

# COMMAND ----------
# MAGIC %md ### 5. Sample predictions

# COMMAND ----------
display(
    spark.table(f"{gold_catalog}.features.claim_predictions")
    .select(
        "claim_id", "severity_label", "predicted_severity_label",
        "prob_minor", "prob_moderate", "prob_severe", "prob_total_loss",
        "model_version", "scored_at",
    )
    .limit(20)
)
