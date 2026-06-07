# Databricks notebook source
# Training: Claim Severity Classifier (LightGBM, multi-class 0–3)
# Reads  : {gold_catalog}.features.claim_features  (120K rows, 36 features)
# Writes : MLflow run + UC model {gold_catalog}.models.{ml_model_name}

# COMMAND ----------
# MAGIC %pip install lightgbm -q

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Claim Severity Classifier — Training
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Input**     | `{gold_catalog}.features.claim_features` (120 K rows, 36 features) |
# MAGIC | **Target**    | `severity_encoded` — 0=minor · 1=moderate · 2=severe · 3=total_loss |
# MAGIC | **Algorithm** | LightGBM multi-class (balanced class weights, early stopping) |
# MAGIC | **Output**    | MLflow run + UC model `{gold_catalog}.models.{ml_model_name}` with alias `champion` |

# COMMAND ----------
dbutils.widgets.text("gold_catalog",       "gold_dev",                                "Gold Catalog")
dbutils.widgets.text("ml_experiment_name", "/Shared/insurance_poc/claim_severity",    "MLflow Experiment Path")
dbutils.widgets.text("ml_model_name",      "claim_severity_classifier",               "UC Model Name")

gold_catalog    = dbutils.widgets.get("gold_catalog")
experiment_name = dbutils.widgets.get("ml_experiment_name")
model_name      = dbutils.widgets.get("ml_model_name")
uc_model_path   = f"{gold_catalog}.models.{model_name}"

print(f"Features   : {gold_catalog}.features.claim_features")
print(f"Experiment : {experiment_name}")
print(f"UC model   : {uc_model_path}")

# COMMAND ----------
# Ensure models schema exists before registering
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{gold_catalog}`.`models`")
print(f"✓ schema: {gold_catalog}.models")

# COMMAND ----------
import mlflow
import mlflow.sklearn
import pandas as pd
import lightgbm as lgb
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report
from mlflow.models.signature import infer_signature
from mlflow import MlflowClient

FEATURE_COLS = [
    # Claim type (5 binary flags)
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
    # Incident weather & road (7)
    "f_weather_flood", "f_weather_rain", "f_weather_wind", "f_weather_clear",
    "f_road_flooded", "f_road_icy", "f_visibility_poor",
    # Incident metadata (3)
    "f_num_vehicles", "f_witness_count", "f_police_report",
    # Assessment aggregates (4)
    "f_num_assessments", "f_avg_repair_cost_gbp", "f_max_repair_cost_gbp", "f_total_labour_hours",
]

TARGET_COL   = "severity_encoded"
LABEL_NAMES  = ["minor", "moderate", "severe", "total_loss"]

# COMMAND ----------
# MAGIC %md ### 1. Load features

# COMMAND ----------
pdf = (
    spark.table(f"{gold_catalog}.features.claim_features")
    .select(FEATURE_COLS + [TARGET_COL])
    .dropna(subset=[TARGET_COL])
    .toPandas()
)

X = pdf[FEATURE_COLS].fillna(0).astype(float)
y = pdf[TARGET_COL].astype(int)

print(f"Dataset : {len(X):,} rows × {len(FEATURE_COLS)} features")
print("Class distribution:")
for cls, count in y.value_counts().sort_index().items():
    print(f"  {cls} ({LABEL_NAMES[cls]}): {count:,}  ({count / len(y) * 100:.1f}%)")

# COMMAND ----------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

# COMMAND ----------
# MAGIC %md ### 2. Train with MLflow tracking

# COMMAND ----------
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_name)

params = {
    "objective":         "multiclass",
    "num_class":         4,
    "n_estimators":      400,
    "learning_rate":     0.05,
    "max_depth":         6,
    "num_leaves":        63,
    "min_child_samples": 20,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "class_weight":      "balanced",
    "random_state":      42,
    "n_jobs":            -1,
    "verbose":           -1,
}

with mlflow.start_run(run_name="lgbm_severity_v1") as run:
    mlflow.log_params(params)
    mlflow.set_tag("gold_catalog",  gold_catalog)
    mlflow.set_tag("model_type",    "multiclass_severity")
    mlflow.set_tag("n_features",    len(FEATURE_COLS))
    mlflow.set_tag("train_rows",    len(X_train))

    model = LGBMClassifier(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=100),
        ],
    )

    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    # ── Metrics ──────────────────────────────────────────────────────────────
    accuracy     = accuracy_score(y_test, y_pred)
    f1_weighted  = f1_score(y_test, y_pred, average="weighted")
    f1_macro     = f1_score(y_test, y_pred, average="macro")
    f1_per_class = f1_score(y_test, y_pred, average=None)
    best_iter    = model.best_iteration_ or params["n_estimators"]

    mlflow.log_metric("test_accuracy",    accuracy)
    mlflow.log_metric("test_f1_weighted", f1_weighted)
    mlflow.log_metric("test_f1_macro",    f1_macro)
    mlflow.log_metric("best_iteration",   best_iter)
    for label, f1 in zip(LABEL_NAMES, f1_per_class):
        mlflow.log_metric(f"test_f1_{label}", f1)

    # ── Artifacts ────────────────────────────────────────────────────────────
    report = classification_report(y_test, y_pred, target_names=LABEL_NAMES)
    mlflow.log_text(report, "classification_report.txt")

    importance_df = (
        pd.DataFrame({"feature": FEATURE_COLS, "importance": model.feature_importances_})
        .sort_values("importance", ascending=False)
    )
    mlflow.log_text(importance_df.to_csv(index=False), "feature_importance.csv")

    # ── Log & register model in Unity Catalog ────────────────────────────────
    signature = infer_signature(X_train, y_proba)
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.head(5),
        registered_model_name=uc_model_path,
    )

    run_id = run.info.run_id

# COMMAND ----------
# MAGIC %md ### 3. Promote to champion alias

# COMMAND ----------
client = MlflowClient()
versions = client.search_model_versions(f"name='{uc_model_path}'")
latest   = max(versions, key=lambda v: int(v.version))
client.set_registered_model_alias(uc_model_path, "champion", latest.version)

print(f"✓ Run ID         : {run_id}")
print(f"✓ Best iteration : {best_iter}")
print(f"✓ Accuracy       : {accuracy:.4f}")
print(f"✓ F1 (weighted)  : {f1_weighted:.4f}")
print(f"✓ F1 (macro)     : {f1_macro:.4f}")
print()
for label, f1 in zip(LABEL_NAMES, f1_per_class):
    print(f"  F1 [{label:12s}] : {f1:.4f}")
print()
print(report)
print(f"✓ Registered     : {uc_model_path}  (version {latest.version}, alias: champion)")

# COMMAND ----------
# MAGIC %md ### 4. Top-10 features by importance

# COMMAND ----------
display(importance_df.head(10))
