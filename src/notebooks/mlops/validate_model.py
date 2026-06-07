# Databricks notebook source
# Model Validation: Claim Severity Classifier
# Generates a fresh synthetic batch (different seed from training),
# applies feature engineering inline, scores with champion model,
# and applies threshold gates to produce a PASS / FAIL decision.

# COMMAND ----------
# MAGIC %pip install lightgbm faker -q

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model Validation — Fresh Synthetic Batch
# MAGIC
# MAGIC Generates **N new claims** using Faker with a different random seed from
# MAGIC training (seed=42). The validation batch intentionally uses a slightly
# MAGIC different distribution (lower storm rate, different region weights) to
# MAGIC test that the model generalises beyond its training distribution.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Model**       | `{gold_catalog}.models.{ml_model_name}@champion` |
# MAGIC | **Data**        | Fresh synthetic batch — Faker seed ≠ training seed |
# MAGIC | **Output**      | PASS / FAIL per threshold gate + MLflow validation run |
# MAGIC | **Gates**       | accuracy ≥ 0.75 · F1-weighted ≥ 0.73 · F1-macro ≥ 0.65 · F1-total_loss ≥ 0.55 |

# COMMAND ----------
dbutils.widgets.text("gold_catalog",       "gold_dev",                  "Gold Catalog")
dbutils.widgets.text("ml_model_name",      "claim_severity_classifier",  "UC Model Name")
dbutils.widgets.text("n_claims",           "10000",                      "Validation Claims (N)")
dbutils.widgets.text("random_seed",        "999",                        "Random Seed (≠ 42 used in training)")
dbutils.widgets.text("thresh_accuracy",    "0.75",                       "Gate: min accuracy")
dbutils.widgets.text("thresh_f1_weighted", "0.73",                       "Gate: min F1-weighted")
dbutils.widgets.text("thresh_f1_macro",    "0.65",                       "Gate: min F1-macro")
dbutils.widgets.text("thresh_f1_total_loss", "0.55",                     "Gate: min F1-total_loss")

gold_catalog  = dbutils.widgets.get("gold_catalog")
model_name    = dbutils.widgets.get("ml_model_name")
uc_model_path = f"{gold_catalog}.models.{model_name}"
n_claims      = int(dbutils.widgets.get("n_claims"))
seed          = int(dbutils.widgets.get("random_seed"))

THRESHOLDS = {
    "accuracy":      float(dbutils.widgets.get("thresh_accuracy")),
    "f1_weighted":   float(dbutils.widgets.get("thresh_f1_weighted")),
    "f1_macro":      float(dbutils.widgets.get("thresh_f1_macro")),
    "f1_total_loss": float(dbutils.widgets.get("thresh_f1_total_loss")),
}

print(f"Model     : {uc_model_path}@champion")
print(f"N claims  : {n_claims:,}")
print(f"Seed      : {seed}  (training used seed=42)")
print(f"Thresholds: {THRESHOLDS}")

# COMMAND ----------
# MAGIC %md ### 1. Generate fresh synthetic validation batch

# COMMAND ----------
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker("en_GB")
rng  = np.random.default_rng(seed)

# ── Policyholders ─────────────────────────────────────────────────────────────
ph_ids = [f"PH-VAL-{i:05d}" for i in range(n_claims)]
policyholders = pd.DataFrame({
    "policyholder_id": ph_ids,
    "age":            rng.integers(18, 80, n_claims),
    "driving_years":  rng.integers(0, 50, n_claims),
    "driving_points": rng.integers(0, 12, n_claims),
})

# ── Vehicles ──────────────────────────────────────────────────────────────────
vh_ids = [f"VH-VAL-{i:05d}" for i in range(n_claims)]
vehicles = pd.DataFrame({
    "vehicle_id":      vh_ids,
    "vehicle_type":    rng.choice(["Saloon", "Hatchback", "SUV", "Crossover", "Estate", "Van"], n_claims),
    "year":            rng.integers(2005, 2025, n_claims),
    "estimated_value": rng.uniform(2000, 60000, n_claims),
    "mileage":         rng.uniform(0, 150000, n_claims),
})

# ── Policies ──────────────────────────────────────────────────────────────────
pol_ids = [f"POL-VAL-{i:05d}" for i in range(n_claims)]
policies = pd.DataFrame({
    "policy_id":       pol_ids,
    "policyholder_id": ph_ids,
    "policy_type":     rng.choice(["Comprehensive", "ThirdPartyFireTheft", "ThirdPartyOnly"],
                                   n_claims, p=[0.60, 0.30, 0.10]),
    "excess":          rng.choice([250, 500, 750, 1000], n_claims).astype(float),
    "coverage_limit":  rng.uniform(5000, 100000, n_claims),
    "premium_monthly": rng.uniform(30, 300, n_claims),
})

# ── Claims ────────────────────────────────────────────────────────────────────
# Intentionally different distribution: storm rate 10% (vs 35% in training)
# to test generalisation beyond the Storm Freya scenario.
clm_ids        = [f"CLM-VAL-{i:05d}" for i in range(n_claims)]
severity_labels = ["minor", "moderate", "severe", "total_loss"]

claims = pd.DataFrame({
    "claim_id":         clm_ids,
    "policy_id":        pol_ids,
    "policyholder_id":  ph_ids,
    "vehicle_id":       vh_ids,
    "claim_type":       rng.choice(["weather", "collision", "theft", "vandalism", "fire"],
                                    n_claims, p=[0.35, 0.35, 0.15, 0.10, 0.05]),
    "claim_amount_gbp": rng.uniform(100, 50000, n_claims),
    "fraud_risk_score": rng.uniform(0, 1, n_claims),
    "is_storm_related": rng.random(n_claims) < 0.10,   # 10% storm vs 35% in training
    "severity_label":   rng.choice(severity_labels, n_claims, p=[0.26, 0.41, 0.26, 0.07]),
})
claims["severity_encoded"] = claims["severity_label"].map(
    {"minor": 0, "moderate": 1, "severe": 2, "total_loss": 3}
)

# ── Incidents ─────────────────────────────────────────────────────────────────
incidents = pd.DataFrame({
    "claim_id":              clm_ids,
    "weather_condition":     rng.choice(["flood", "heavy_rain", "strong_wind", "clear", "fog", "snow"],
                                         n_claims, p=[0.05, 0.20, 0.15, 0.40, 0.10, 0.10]),
    "road_condition":        rng.choice(["dry", "wet", "flooded", "icy"],
                                         n_claims, p=[0.50, 0.30, 0.10, 0.10]),
    "visibility":            rng.choice(["good", "moderate", "poor"],
                                         n_claims, p=[0.60, 0.25, 0.15]),
    "num_vehicles_involved": rng.integers(1, 5, n_claims),
    "witness_count":         rng.integers(0, 6, n_claims),
    "police_report_filed":   rng.choice(["Y", "N"], n_claims, p=[0.40, 0.60]),
})

# ── Assessments (1–3 per claim) ───────────────────────────────────────────────
ass_rows = []
for clm_id in clm_ids:
    for _ in range(int(rng.integers(1, 4))):
        ass_rows.append({
            "claim_id":                  clm_id,
            "estimated_repair_cost_gbp": float(rng.uniform(100, 20000)),
            "labour_hours":              float(rng.uniform(1, 40)),
        })
ass_df = pd.DataFrame(ass_rows)
ass_agg = ass_df.groupby("claim_id").agg(
    f_num_assessments    =("estimated_repair_cost_gbp", "count"),
    f_avg_repair_cost_gbp=("estimated_repair_cost_gbp", "mean"),
    f_max_repair_cost_gbp=("estimated_repair_cost_gbp", "max"),
    f_total_labour_hours =("labour_hours", "sum"),
).reset_index()

print(f"✓ Generated {n_claims:,} fresh claims  (storm rate: {claims['is_storm_related'].mean():.1%})")
print(f"  Severity distribution:")
for label in severity_labels:
    pct = (claims["severity_label"] == label).mean()
    print(f"    {label:12s}: {pct:.1%}")

# COMMAND ----------
# MAGIC %md ### 2. Apply feature engineering (replicates gold_features DLT logic)

# COMMAND ----------
df = (
    claims
    .merge(policies[["policy_id", "policy_type", "excess", "coverage_limit", "premium_monthly"]],
           on="policy_id", how="left")
    .merge(policyholders[["policyholder_id", "age", "driving_years", "driving_points"]],
           on="policyholder_id", how="left")
    .merge(vehicles[["vehicle_id", "vehicle_type", "year", "estimated_value", "mileage"]],
           on="vehicle_id", how="left")
    .merge(incidents, on="claim_id", how="left")
    .merge(ass_agg,   on="claim_id", how="left")
)

# Claim type
df["f_type_weather"]   = (df["claim_type"] == "weather").astype(int)
df["f_type_collision"] = (df["claim_type"] == "collision").astype(int)
df["f_type_theft"]     = (df["claim_type"] == "theft").astype(int)
df["f_type_vandalism"] = (df["claim_type"] == "vandalism").astype(int)
df["f_type_fire"]      = (df["claim_type"] == "fire").astype(int)
# Claim attributes
df["f_claim_amount_gbp"] = df["claim_amount_gbp"]
df["f_fraud_risk_score"] = df["fraud_risk_score"]
df["f_is_storm"]         = df["is_storm_related"].astype(int)
# Policy
df["f_excess_gbp"]          = df["excess"]
df["f_coverage_limit_gbp"]  = df["coverage_limit"]
df["f_premium_monthly_gbp"] = df["premium_monthly"]
df["f_pol_comprehensive"]   = (df["policy_type"] == "Comprehensive").astype(int)
df["f_pol_tpft"]            = (df["policy_type"] == "ThirdPartyFireTheft").astype(int)
df["f_pol_tpo"]             = (df["policy_type"] == "ThirdPartyOnly").astype(int)
# Policyholder
df["f_ph_age"]         = df["age"]
df["f_driving_years"]  = df["driving_years"]
df["f_driving_points"] = df["driving_points"]
# Vehicle
df["f_vehicle_age"]      = 2026 - df["year"]
df["f_vehicle_value_gbp"] = df["estimated_value"]
df["f_mileage_k"]        = df["mileage"] / 1000.0
df["f_vtype_suv"]        = df["vehicle_type"].isin(["SUV", "Crossover"]).astype(int)
df["f_vtype_estate"]     = (df["vehicle_type"] == "Estate").astype(int)
# Incident
df["f_weather_flood"]   = (df["weather_condition"] == "flood").astype(int)
df["f_weather_rain"]    = (df["weather_condition"] == "heavy_rain").astype(int)
df["f_weather_wind"]    = (df["weather_condition"] == "strong_wind").astype(int)
df["f_weather_clear"]   = (df["weather_condition"] == "clear").astype(int)
df["f_road_flooded"]    = (df["road_condition"] == "flooded").astype(int)
df["f_road_icy"]        = (df["road_condition"] == "icy").astype(int)
df["f_visibility_poor"] = (df["visibility"] == "poor").astype(int)
df["f_num_vehicles"]    = df["num_vehicles_involved"]
df["f_witness_count"]   = df["witness_count"]
df["f_police_report"]   = (df["police_report_filed"] == "Y").astype(int)
# Assessment aggregates
df["f_num_assessments"]     = df["f_num_assessments"].fillna(0)
df["f_avg_repair_cost_gbp"] = df["f_avg_repair_cost_gbp"].fillna(0)
df["f_max_repair_cost_gbp"] = df["f_max_repair_cost_gbp"].fillna(0)
df["f_total_labour_hours"]  = df["f_total_labour_hours"].fillna(0)

print(f"✓ Feature engineering complete — {len(df):,} rows × 36 features")

# COMMAND ----------
# MAGIC %md ### 3. Score with champion model

# COMMAND ----------
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from sklearn.metrics import accuracy_score, f1_score, classification_report

mlflow.set_registry_uri("databricks-uc")

client        = MlflowClient()
mv            = client.get_model_version_by_alias(uc_model_path, "champion")
model_version = mv.version
model         = mlflow.sklearn.load_model(f"models:/{uc_model_path}@champion")

FEATURE_COLS = [
    "f_type_weather", "f_type_collision", "f_type_theft", "f_type_vandalism", "f_type_fire",
    "f_claim_amount_gbp", "f_fraud_risk_score", "f_is_storm",
    "f_excess_gbp", "f_coverage_limit_gbp", "f_premium_monthly_gbp",
    "f_pol_comprehensive", "f_pol_tpft", "f_pol_tpo",
    "f_ph_age", "f_driving_years", "f_driving_points",
    "f_vehicle_age", "f_vehicle_value_gbp", "f_mileage_k",
    "f_vtype_suv", "f_vtype_estate",
    "f_weather_flood", "f_weather_rain", "f_weather_wind", "f_weather_clear",
    "f_road_flooded", "f_road_icy", "f_visibility_poor",
    "f_num_vehicles", "f_witness_count", "f_police_report",
    "f_num_assessments", "f_avg_repair_cost_gbp", "f_max_repair_cost_gbp", "f_total_labour_hours",
]
LABELS = ["minor", "moderate", "severe", "total_loss"]

X_val  = df[FEATURE_COLS].fillna(0).astype(float)
y_val  = df["severity_encoded"].astype(int)

y_pred       = model.predict(X_val)
f1_per_class = f1_score(y_val, y_pred, average=None)

metrics = {
    "accuracy":      accuracy_score(y_val, y_pred),
    "f1_weighted":   f1_score(y_val, y_pred, average="weighted"),
    "f1_macro":      f1_score(y_val, y_pred, average="macro"),
    "f1_total_loss": f1_per_class[3],
    **{f"f1_{lbl}": f1_per_class[i] for i, lbl in enumerate(LABELS)},
}

print(f"✓ Scored {len(X_val):,} claims with model version {model_version}\n")
print(f"  accuracy    : {metrics['accuracy']:.4f}")
print(f"  f1_weighted : {metrics['f1_weighted']:.4f}")
print(f"  f1_macro    : {metrics['f1_macro']:.4f}")
for lbl in LABELS:
    print(f"  f1_{lbl:12s}: {metrics[f'f1_{lbl}']:.4f}")

# COMMAND ----------
# MAGIC %md ### 4. Threshold gates — PASS / FAIL

# COMMAND ----------
gate_results = {k: (metrics[k], THRESHOLDS[k], metrics[k] >= THRESHOLDS[k])
                for k in THRESHOLDS}
overall_pass = all(v[2] for v in gate_results.values())

print("=" * 55)
print(f"  VALIDATION RESULT: {'✅ PASS' if overall_pass else '❌ FAIL'}")
print("=" * 55)
print(f"  {'Gate':<20} {'Score':>7}  {'Threshold':>9}  {'Result':>6}")
print(f"  {'-'*48}")
for gate, (score, threshold, passed) in gate_results.items():
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {gate:<20} {score:>7.4f}  {threshold:>9.4f}  {status}")
print("=" * 55)

# COMMAND ----------
# MAGIC %md ### 5. Log validation run to MLflow

# COMMAND ----------
experiment_name = f"/Shared/insurance_poc/claim_severity"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name=f"validation_seed{seed}_v{model_version}") as run:
    mlflow.set_tag("run_type",      "validation")
    mlflow.set_tag("model_version", model_version)
    mlflow.set_tag("model_alias",   "champion")
    mlflow.set_tag("n_claims",      n_claims)
    mlflow.set_tag("random_seed",   seed)
    mlflow.set_tag("storm_rate",    f"{claims['is_storm_related'].mean():.2%}")
    mlflow.set_tag("result",        "PASS" if overall_pass else "FAIL")

    for k, v in metrics.items():
        mlflow.log_metric(f"val_{k}", v)
    for gate, (score, threshold, passed) in gate_results.items():
        mlflow.log_metric(f"gate_{gate}_score",     score)
        mlflow.log_metric(f"gate_{gate}_threshold", threshold)
        mlflow.log_metric(f"gate_{gate}_pass",      int(passed))

    report = classification_report(y_val, y_pred, target_names=LABELS)
    mlflow.log_text(report, "validation_classification_report.txt")

    print(f"✓ Logged to MLflow — Run ID: {run.info.run_id}")
    print(f"  Experiment : {experiment_name}")

# COMMAND ----------
# MAGIC %md ### 6. Classification report

# COMMAND ----------
print(classification_report(y_val, y_pred, target_names=LABELS))

# COMMAND ----------
if not overall_pass:
    failed = [k for k, (_, _, p) in gate_results.items() if not p]
    raise Exception(
        f"Validation FAILED — gates not met: {failed}. "
        f"Model version {model_version} should NOT be promoted to champion. "
        f"Consider retraining with more data or tuned hyperparameters."
    )
