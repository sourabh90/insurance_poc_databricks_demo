# MLOps Guide

## Overview

The MLOps layer trains, validates, deploys, and monitors a **claim severity classifier** on top of the gold feature table produced by the data engineering pipeline. The model predicts one of four severity classes — Minor, Moderate, Severe, or Total Loss — and is used for automated triage of incoming insurance claims.

The full lifecycle is:

```
gold_dev.features.claim_features
        │
        ▼  train_severity_model
  MLflow Experiment (training run)
  UC Model Registry  (champion alias)
        │
        ├──  validate_model   (fresh synthetic batch, quality gates)
        │
        ▼  batch_score        (Pandas UDF + broadcast model)
  gold_dev.features.claim_predictions
        │
        ▼  setup_monitor      (Lakehouse Monitoring — InferenceLog)
  gold_dev.monitoring.claim_predictions_profile_metrics
  gold_dev.monitoring.claim_predictions_drift_metrics
```

---

## ML use case

**Problem:** Classify claim severity at first-notice-of-loss (FNOL) to route claims to the right assessor tier before any human review, cutting assessment turnaround time and improving reserve accuracy.

**Target label:** `severity_encoded` derived from `claim_severity`:
| Class | Label | Description |
|---|---|---|
| 0 | Minor | Low-cost claim, standard repair |
| 1 | Moderate | Medium repair cost, extended timeline |
| 2 | Severe | High-value claim, specialist assessor |
| 3 | Total Loss | Vehicle write-off, settlement required |

**Model:** LightGBM multi-class classifier, logged and versioned in MLflow + UC Model Registry.

---

## Feature table

Source: `gold_dev.features.claim_features` — one row per claim.

### Feature groups (36 total)

| Group | Features |
|---|---|
| Claim attributes | `claim_type_enc`, `claim_amount_gbp`, `is_storm_related`, `fraud_risk_score` |
| Incident conditions | `weather_cond_enc`, `road_cond_enc`, `visibility_km`, `speed_limit_kph`, `was_police_involved` |
| Policyholder profile | `age`, `driving_experience_years`, `penalty_points`, `at_fault_accidents`, `risk_tier_enc` |
| Vehicle attributes | `vehicle_age_years`, `vehicle_value_gbp`, `mileage_annual_km` |
| Policy attributes | `policy_type_enc`, `monthly_premium_gbp`, `policy_excess_gbp`, `coverage_limit_gbp` |
| Assessment | `assessor_tier_enc`, `assessor_score`, `damage_category_enc`, `repair_cost_gbp`, `labour_hours` |
| Geography | `region_enc`, `city_enc`, `is_storm_region` |
| Derived ratios | `claim_to_premium_ratio`, `claim_to_coverage_ratio`, `repair_to_value_ratio`, `excess_to_premium_ratio`, `assessment_delay_days` |

The feature engineering logic lives in `src/pipelines/gold_features.py` and is replicated inline (in pandas) by `validate_model.py` when generating the fresh validation batch.

---

## Stage 1 — Training

**Notebook:** `src/notebooks/mlops/train_severity_model.py`
**Job:** `resources/jobs/mlops/ml_training_job.yml`

### What it does

1. Reads `gold_dev.features.claim_features` from the gold layer.
2. Splits data: 80% train, 20% test (stratified by `severity_encoded`, `random_state=42`).
3. Applies `StandardScaler` to numeric features.
4. Trains a `LightGBMClassifier` (via `lightgbm` Python package).
5. Logs to MLflow:
   - Parameters: num_leaves, learning_rate, n_estimators, max_depth, random_state
   - Metrics: accuracy, f1_weighted, f1_macro, f1_total_loss
   - Artefacts: trained model, feature importance plot, confusion matrix
6. Registers the model in UC Model Registry under `gold_dev.features.insurance_claim_severity_model`.
7. Assigns the `champion` alias to the newly registered version.

### LightGBM parameters

| Parameter | Value | Reason |
|---|---|---|
| `num_leaves` | 63 | Balanced capacity for 36 features |
| `learning_rate` | 0.05 | Conservative — stable convergence |
| `n_estimators` | 500 | Enough iterations with early stopping |
| `max_depth` | -1 | Unconstrained — leaf-wise growth |
| `random_state` | 42 | Reproducible splits |

### MLflow experiment

Experiment name: configured in `databricks.yml` via `ml_experiment_name` variable.

Each training run is tagged `run_type=training`. You can compare runs in the Databricks ML Experiments UI — filter by `run_type` to separate training runs from validation runs.

### Run the training job

```bash
databricks bundle run insurance_poc_ml_training --target dev
```

Or trigger from the Databricks Jobs UI: `[dev] insurance_poc - Job - ML Training (Claim Severity)`.

---

## Stage 2 — Validation

**Notebook:** `src/notebooks/mlops/validate_model.py`
**Job:** `resources/jobs/mlops/validate_model_job.yml`

### What it does

Generates a **fresh synthetic batch** (10,000 claims) that was never seen during training, applies the same feature engineering, scores it with the current champion model, and checks it against four quality thresholds. Fails loudly if any threshold is missed.

### Fresh synthetic batch

| Setting | Training | Validation |
|---|---|---|
| `random_seed` | 42 | 999 |
| `storm_rate` | 35% | 10% (deliberate distribution shift) |
| Claims | ~120K | 10,000 |
| Locale | `en_GB` (Faker) | `en_GB` (Faker) |

The distribution shift (lower storm rate, different random seed) means the validation batch tests generalisation rather than memorisation. Different storm mix → different severity distribution → model must handle unseen proportions.

### Quality gates

| Metric | Threshold | Reason |
|---|---|---|
| `accuracy` | ≥ 0.75 | Baseline correctness |
| `f1_weighted` | ≥ 0.73 | Handles class imbalance gracefully |
| `f1_macro` | ≥ 0.65 | All four classes must perform |
| `f1_total_loss` | ≥ 0.55 | Total-loss is rare but high-value — must not be missed |

All four gates must pass. A single failure raises an exception that stops downstream jobs in any workflow that chains them.

### MLflow logging

Each validation run is tagged `run_type=validation`. Logged metrics match the training run metrics so they appear in the same comparison view. The run description includes `champion_version` so you can trace which model version was tested.

### Run the validation job

```bash
databricks bundle run insurance_poc_validate_model --target dev
```

To override thresholds for experimentation:

```bash
databricks bundle run insurance_poc_validate_model --target dev \
  --params '{"thresh_accuracy":"0.70","thresh_f1_total_loss":"0.50"}'
```

---

## Stage 3 — Batch inference

**Notebook:** `src/notebooks/mlops/batch_score.py`
**Job:** `resources/jobs/mlops/batch_inference_job.yml`

### What it does

1. Loads the `champion` model from UC Model Registry.
2. Broadcasts the sklearn model object to all Spark executors.
3. Applies a Pandas UDF for parallelised scoring across Spark partitions.
4. Writes predictions to `gold_dev.features.claim_predictions`.

### Key pattern — Pandas UDF + broadcast

```python
# Load and broadcast once
sklearn_model = mlflow.sklearn.load_model(f"models:/{uc_model_path}@champion")
bc_model = spark.sparkContext.broadcast(sklearn_model)

@F.pandas_udf(ArrayType(DoubleType()))
def score_udf(*cols):
    X = pd.concat(list(cols), axis=1)
    X.columns = FEATURE_COLS
    return pd.Series(list(bc_model.value.predict_proba(X.fillna(0).astype(float))))
```

Broadcasting avoids sending the model to each executor repeatedly — important for large clusters scoring millions of rows.

### Output table — `claim_predictions`

| Column | Type | Description |
|---|---|---|
| `claim_id` | string | Join key back to source |
| `predicted_severity` | int | 0=Minor, 1=Moderate, 2=Severe, 3=Total Loss |
| `predicted_severity_label` | string | Human-readable label |
| `prob_minor` | double | Class probability |
| `prob_moderate` | double | Class probability |
| `prob_severe` | double | Class probability |
| `prob_total_loss` | double | Class probability |
| `severity_encoded` | int | Ground-truth label (for monitoring) |
| `model_version` | string | UC model version number |
| `scored_at` | timestamp | Batch scoring run timestamp |
| All 36 feature columns | various | Passed through for monitoring analysis |

Write mode: `overwrite`. Each batch run replaces the full table with freshly scored predictions.

### Run the inference job

```bash
databricks bundle run insurance_poc_batch_inference --target dev
```

---

## Stage 4 — Monitoring

**Notebook:** `src/notebooks/mlops/setup_monitor.py`
**Job:** `resources/jobs/mlops/setup_monitor_job.yml`

### What it does

Creates a **Lakehouse Monitor** (InferenceLog type) on the `claim_predictions` table. After the first run it creates the monitor and triggers an initial refresh. On subsequent runs it triggers a refresh to pick up the latest batch of predictions.

### Monitor configuration

| Setting | Value |
|---|---|
| Monitor type | InferenceLog — Classification |
| Table | `gold_dev.features.claim_predictions` |
| Timestamp column | `scored_at` |
| Prediction column | `predicted_severity` |
| Label column | `severity_encoded` |
| Model ID column | `model_version` |
| Granularities | 1 day, 1 week |
| Output schema | `gold_dev.monitoring` |
| Slicing expressions | `predicted_severity_label`, `model_version` |

### Output tables

**Profile metrics** (`gold_dev.monitoring.claim_predictions_profile_metrics`):

Column-level statistics per time window:

| Column | Description |
|---|---|
| `window` | Date range of the window |
| `column_name` | Feature or prediction column |
| `mean`, `std_dev` | Distribution statistics |
| `percent_null` | Missing rate |
| `min_value`, `max_value` | Range |

Sliced by `predicted_severity_label` and `model_version` so you can compare distributions across model releases.

**Drift metrics** (`gold_dev.monitoring.claim_predictions_drift_metrics`):

Distribution shift vs. the previous time window:

| Column | Description |
|---|---|
| `window` | Current window being compared |
| `column_name` | Which column was compared |
| `js_distance` | Jensen-Shannon distance (0 = identical) |
| `kl_divergence` | KL divergence (∞ = complete mismatch) |
| `drift_detected` | Boolean flag based on built-in threshold |

### Drift interpretation

| JS distance | Action |
|---|---|
| < 0.05 | No significant drift — model healthy |
| 0.05 – 0.10 | Mild drift — increase monitoring cadence |
| > 0.10 | Significant drift — investigate data shift or retrain |

### Accessing the monitor

Navigate to **Catalog Explorer → `gold_dev`.`features`.`claim_predictions` → Quality tab** in the Databricks UI. A built-in dashboard shows accuracy trends, prediction distribution, and drift scores over time.

> **First-run note:** Drift metrics require at least two time windows of data. Run `batch_score` on separate days to build enough history for drift detection to activate.

### Run the monitoring job

```bash
databricks bundle run insurance_poc_setup_monitor --target dev
```

---

## Complete workflow

### Initial setup (one-time after model training)

```bash
# 1. Train the model
databricks bundle run insurance_poc_ml_training --target dev

# 2. Validate champion quality
databricks bundle run insurance_poc_validate_model --target dev

# 3. Score all claims
databricks bundle run insurance_poc_batch_inference --target dev

# 4. Create monitor
databricks bundle run insurance_poc_setup_monitor --target dev
```

### Recurring workflow (after new data arrives)

```bash
# 1. Re-run full medallion pipeline (includes ML training step)
databricks bundle run main_job --target dev

# 2. Validate the new champion
databricks bundle run insurance_poc_validate_model --target dev

# 3. Score new claims
databricks bundle run insurance_poc_batch_inference --target dev

# 4. Refresh monitoring metrics
databricks bundle run insurance_poc_setup_monitor --target dev
```

### Checking results

```sql
-- Model performance on the current batch
SELECT
  predicted_severity_label,
  COUNT(*)                                  AS predictions,
  ROUND(AVG(CASE WHEN predicted_severity = severity_encoded THEN 1.0 ELSE 0.0 END), 3) AS accuracy,
  ROUND(AVG(prob_total_loss), 4)            AS avg_total_loss_confidence
FROM gold_dev.features.claim_predictions
GROUP BY 1 ORDER BY 1;

-- Drift metrics for prediction column
SELECT window, js_distance, kl_divergence, drift_detected
FROM gold_dev.monitoring.claim_predictions_drift_metrics
WHERE column_name = 'predicted_severity'
ORDER BY window DESC;
```

---

## Jobs reference

| Job name | DAB resource | Trigger |
|---|---|---|
| `insurance_poc_ml_training` | `resources/jobs/mlops/ml_training_job.yml` | Manual / after data pipeline |
| `insurance_poc_validate_model` | `resources/jobs/mlops/validate_model_job.yml` | Manual / after training |
| `insurance_poc_batch_inference` | `resources/jobs/mlops/batch_inference_job.yml` | Manual / after validation |
| `insurance_poc_setup_monitor` | `resources/jobs/mlops/setup_monitor_job.yml` | Manual / after inference |

---

## Key files

| File | Purpose |
|---|---|
| `src/pipelines/gold_features.py` | Feature engineering: computes the 36-feature `claim_features` table via DLT |
| `src/notebooks/mlops/train_severity_model.py` | Model training, MLflow logging, champion alias assignment |
| `src/notebooks/mlops/validate_model.py` | Fresh synthetic batch validation with quality gates |
| `src/notebooks/mlops/batch_score.py` | Pandas UDF batch scoring with broadcast model |
| `src/notebooks/mlops/setup_monitor.py` | Lakehouse Monitoring setup and refresh |
| `resources/jobs/mlops/` | DAB job YAML definitions for all 4 MLOps jobs |

---

## Configuration reference

All MLOps variables are defined in `databricks.yml`:

| Variable | Default | Description |
|---|---|---|
| `ml_experiment_name` | `/Users/mona.sourabh17@gmail.com/insurance_poc_claim_severity` | MLflow experiment path |
| `ml_model_name` | `insurance_claim_severity_model` | UC model name (without catalog prefix) |
| `gold_catalog` | `gold_dev` (dev) / `gold_prod` (prod) | Catalog containing feature and prediction tables |

The full UC model path is `{gold_catalog}.features.{ml_model_name}`.
