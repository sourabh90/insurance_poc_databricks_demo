# Data Engineering Guide

## Overview

The data engineering layer implements a **medallion architecture** (Landing → Bronze → Silver → Gold) for UK insurance claims analytics using Databricks Delta Live Tables (DLT). The scenario models Storm Freya (February 2026) across UK regions, with 35% of 120,000 synthetic claims identified as storm-related.

---

## Architecture

```
Landing Volume (CSV files)
       │
       ▼  landing_to_bronze  (Autoloader)
  Bronze Layer           bronze_dev.raw_claims
  6 Delta tables + _ingested_at, _source_file, _rescued_data

       │
       ▼  bronze_to_silver  (type casting + DQ)
  Silver Layer           silver_dev.refined_claims
  6 clean tables + 1 quarantine table
  Hard blocks → quarantine only
  Soft warnings → clean table AND quarantine

       │
       ├──  gold_dimensions   gold_dev.dimensions   (5 tables)
       ├──  gold_facts         gold_dev.facts        (2 tables)
       ├──  gold_features      gold_dev.features     (2 tables — ML input)
       └──  gold_summary       gold_dev.summary      (4 tables — BI pre-aggregations)

system.billing / system.lakeflow
       │
       ▼  sync_system_billing  (nightly)
  Monitoring Layer       monitoring_dev.system_billing
  Replicated so Databricks Apps SP can query without metastore-admin grants
```

---

## Project structure

```
src/
├── pipelines/
│   ├── landing_to_bronze.py     # Autoloader: Volume CSV → raw Delta tables
│   ├── bronze_to_silver.py      # Type casting + DQ checks + quarantine
│   ├── dq_utils.py              # DQ helpers: add_dq() and to_quarantine()
│   ├── gold_dimensions.py       # 5 dimension tables
│   ├── gold_facts.py            # 2 fact tables
│   ├── gold_features.py         # 2 ML feature tables
│   └── gold_summary.py          # 4 BI pre-aggregation tables
├── notebooks/
│   ├── pipeline/
│   │   ├── setup_catalogs.py        # Creates UC catalogs, schemas, volumes; applies SP grants
│   │   ├── generate_synthetic_data.py   # Faker data generation (600K+ rows, ~3% bad data)
│   │   ├── ingest_fx_rates.py       # ECB FX rates → bronze_dev.raw_reference
│   │   ├── main.py                  # Display catalog tables
│   │   └── cleanup.py               # Full teardown (reversible)
│   └── monitoring/
│       └── sync_system_billing.py   # Nightly sync: system.billing → monitoring_dev
├── app/
│   ├── billing/                 # Billing Usage Streamlit app
│   └── fraud/                   # Fraud Intelligence Streamlit app
├── dashboards/
│   ├── insurance_poc_databricks_demo.lvdash.json
│   └── billing_usage.lvdash.json
└── queries/
    ├── dq_summary.sql
    ├── dq_hard_blocks.sql
    ├── dq_soft_warnings.sql
    ├── dq_claims_investigation.sql
    ├── dq_daily_trend.sql
    └── dq_multi_issue_records.sql

resources/
├── pipelines/               # 6 DLT pipeline YAML definitions
├── jobs/
│   ├── pipeline/            # setup, data_gen, main, fx_rates, cleanup
│   └── monitoring/          # sync_system_billing
├── dashboards/              # 2 Lakeview dashboard definitions
├── alerts/                  # DQ failure alert
└── apps/                    # 2 Databricks Apps definitions
```

---

## Data model

### Bronze — source entities

| Table | Rows | Description |
|---|---|---|
| `policyholders` | 50K | Demographics, UK driving record (0–12 penalty points), region |
| `vehicles` | 55K | Make, model year, VIN, estimated value (£), mileage |
| `policies` | 60K | Type (Comprehensive / TPFT / TPO), monthly premium, excess, coverage limit |
| `claims` | 120K | Claim type, amount, severity, fraud score, storm flag |
| `incidents` | 120K | Weather/road conditions, visibility, police report |
| `claim_assessments` | 180K | Assessor tier, damage category, repair cost, labour hours |

All bronze tables include three system columns added by Autoloader:
- `_ingested_at` — timestamp when the record was ingested
- `_source_file` — originating CSV file path
- `_rescued_data` — any columns that didn't match the schema (JSON blob)

**Storm Freya scenario** (Feb 3–17, 2026): 42K claims (35%) flagged as storm-related, concentrated in West Yorkshire, South Yorkshire, Somerset, Greater Manchester, and Devon. Storm claims show 20% total-loss rate (vs 5% baseline) and 14% fraud rate (vs 8% baseline).

---

### Silver — data quality rules

The DQ framework uses two rule types implemented in `dq_utils.py`:

**Hard blocks** — record dropped, written to quarantine only:
| Rule | Description |
|---|---|
| Invalid ID format | IDs must match `CLM-*`, `PH-*`, `VH-*`, `POL-*`, etc. |
| Missing foreign keys | FK columns must not be null |
| Non-positive claim amounts | `claim_amount_gbp` must be > 0 |

**Soft warnings** — record passes to clean table AND written to quarantine:
| Rule | Description |
|---|---|
| Invalid email format | Must match standard email regex |
| Year out of range | Vehicle year must be between 1980 and current year |
| Fraud score out of range | `fraud_risk_score` must be in `[0.0, 1.0]` |

**Quarantine table schema** (`silver_dev.refined_claims.quarantine`):

| Column | Type | Description |
|---|---|---|
| `source_table` | string | Which entity failed (e.g. `claims`) |
| `record_id` | string | Original ID of the failed record |
| `dq_rule_violated` | string | Pipe-separated list of violated rules (e.g. `invalid_id\|missing_fk`) |
| `raw_record` | string | Full record as JSON for investigation |
| `quarantined_at` | timestamp | When the record was quarantined |

---

### Gold — analytics and ML

**Dimensions** (5 tables in `gold_dev.dimensions`):

| Table | Grain | Key columns |
|---|---|---|
| `dim_date` | Day | Storm period flag, day of week, week/month/year |
| `dim_geography` | Region + city | Storm region flag, latitude/longitude |
| `dim_policyholder` | Policyholder | Age band, risk tier (low/medium/high/very_high) |
| `dim_vehicle` | Vehicle | Age band, value band |
| `dim_policy` | Policy | Premium band, policy type |

**Facts** (2 tables in `gold_dev.facts`):

| Table | Rows | Description |
|---|---|---|
| `fact_claim` | 120K | Net payout, denormalised driver + vehicle attributes, storm flag |
| `fact_assessment` | 180K | Days-to-assessment, repair costs, assessor tier |

**ML feature tables** (2 tables in `gold_dev.features`):

| Table | Grain | Description |
|---|---|---|
| `claim_features` | Claim | 36 numeric/binary features; target: `severity_encoded` (0–3) |
| `policyholder_risk_profile` | Policyholder | Risk score (0–1) and tier per policyholder |

**Summary tables** (4 tables in `gold_dev.summary` — pre-aggregated for BI, no joins at query time):

| Table | Description |
|---|---|
| `summary_claims_daily` | Daily volume, amounts, denial rate, open count by region/type/severity |
| `summary_financial_kpis` | Loss ratio, storm damage, total-loss rate by month/policy-type/region |
| `summary_fraud_intelligence` | Fraud rate, fraud amount ratio, score distribution by month/region/type |
| `summary_triage_performance` | Assessor turnaround time, repair cost alignment, severity-action match rate |

---

### Monitoring — system billing replica

`sync_system_billing.py` replicates system tables into `monitoring_dev.system_billing` nightly so the billing app SP can query them without metastore-admin grants on the system catalog.

| Target table | Source | Strategy |
|---|---|---|
| `usage` | `system.billing.usage` | Incremental append (7-day overlap window) |
| `list_prices` | `system.billing.list_prices` | Full replace |
| `pipelines` | `system.lakeflow.pipelines` | Full replace |
| `query_history` | `system.query.history` | Incremental append (7-day overlap window) |
| `pipeline_update_timeline` | `system.lakeflow.pipeline_update_timeline` | Incremental append — skipped if not enabled |
| `job_task_run_timeline` | `system.lakeflow.job_task_run_timeline` | Incremental append — skipped if not enabled |

> `system.lakeflow` tables require a metastore admin to enable them per workspace. The sync job handles missing access gracefully — it prints a warning and continues.

---

## DLT pipelines

Six Serverless DLT pipelines defined in `resources/pipelines/`:

| Pipeline | Input → Output | Key logic |
|---|---|---|
| `landing_to_bronze` | Volume CSV → `bronze_dev.raw_claims` | Autoloader with schema rescue, adds 3 system columns |
| `bronze_to_silver` | `bronze_dev` → `silver_dev.refined_claims` | Type casting, hard/soft DQ checks, quarantine writes |
| `gold_dimensions` | `silver_dev` → `gold_dev.dimensions` | 5 dimension tables with derived columns (bands, flags) |
| `gold_facts` | `silver_dev` + `gold_dev.dimensions` → `gold_dev.facts` | Denormalised fact tables, net payout calculation |
| `gold_features` | `silver_dev` → `gold_dev.features` | 36 ML features + policyholder risk scoring |
| `gold_summary` | `gold_dev.facts` → `gold_dev.summary` | 4 pre-aggregated BI tables |

---

## Jobs reference

| Job | Description | Schedule |
|---|---|---|
| `setup_job` | Creates UC catalogs, schemas, volumes; applies app SP grants | Manual (one-time) |
| `data_gen_job` | Generates 600K+ rows of synthetic data via Faker | Manual |
| `main_job` | Runs all 6 DLT pipelines in order + ML training as step 5 | Manual |
| `fx_rates_job` | Ingests ECB FX rates into `bronze_dev.raw_reference` | Manual |
| `sync_system_billing_job` | Replicates system tables to monitoring catalog | Daily 02:00 Europe/London |
| `cleanup_job` | Full teardown of catalogs, schemas, volumes | Manual |

---

## Getting started

### Prerequisites

- Databricks CLI v0.279.0+, authenticated to your workspace
- Unity Catalog enabled
- Serverless compute available (Serverless DLT + Serverless Starter Warehouse)
- GitHub Actions secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`

### One-time setup

```bash
# 1. Bootstrap UC catalogs
python scripts/bootstrap_catalogs.py --target dev --profile <your-profile>

# 2. Deploy all bundle resources
databricks bundle deploy --target dev

# 3. Create schemas, volumes, apply SP grants
databricks bundle run setup_job --target dev

# 4. Ingest FX rates
databricks bundle run fx_rates_job --target dev

# 5. Generate synthetic data
databricks bundle run data_gen_job --target dev

# 6. Sync system billing (first full load)
databricks bundle run sync_system_billing --target dev

# 7. Run full medallion pipeline + ML training
databricks bundle run main_job --target dev
```

### Teardown

```bash
databricks bundle run cleanup_job --target dev
```

---

## Data quality investigation

Pre-written queries in `src/queries/`:

```sql
-- Failures by table and rule
SELECT source_table, dq_rule_violated, COUNT(*) AS failures
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Records with multiple violations
SELECT *, LENGTH(dq_rule_violated) - LENGTH(REPLACE(dq_rule_violated, '|', '')) + 1 AS issue_count
FROM silver_dev.refined_claims.quarantine
WHERE dq_rule_violated LIKE '%|%'
ORDER BY issue_count DESC;

-- Daily trend
SELECT DATE(quarantined_at) AS day, source_table, COUNT(*) AS failures
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2 ORDER BY 1 DESC;
```

---

## Applications

### Billing Usage App

Interactive Streamlit dashboard for workspace compute cost analytics.

| Feature | Detail |
|---|---|
| KPIs | Total cost (£), total DBUs, active days, coverage period |
| Usage over time | Area chart by product, daily/weekly/monthly granularity |
| 30-day forecast | `ai_forecast()` overlay |
| Product breakdown | Cost by `billing_origin_product` |
| Top N workloads | DBU and cost by job/notebook/pipeline |
| Performance drill-down | SQL query times, DLT pipeline durations, job task breakdown |

Data sources: `monitoring_dev.system_billing.*` + `bronze_dev.raw_reference.fx_rates_usd_gbp`

**Source:** `src/app/billing/` · **DAB resource:** `resources/apps/app.yml`

### Fraud Intelligence App

Risk and fraud analytics dashboard for the fraud investigation team.

| Feature | Detail |
|---|---|
| KPIs | Total claims, flagged claims, fraud rate %, fraud amount at risk (£) |
| Fraud rate by region | Horizontal bar chart sorted by fraud rate |
| Storm Freya vs Baseline | Grouped bar — fraud rate % for storm vs non-storm |
| Heatmap | Region × Claim Type fraud rate matrix |
| Sidebar filters | Month range, region, claim type, storm toggle |

Data source: `gold_dev.summary.summary_fraud_intelligence`

**Source:** `src/app/fraud/` · **DAB resource:** `resources/apps/fraud_app.yml`

### App service principal grants

Each app runs as its own Databricks service principal. After deploying a new app for the first time, retrieve its SP client ID:

```bash
databricks apps get databricks-billing-usage-dev --output json | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id'))"

databricks apps get fraud-intelligence-dev --output json | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id'))"
```

Set `app_service_principal` and `fraud_app_service_principal` in `databricks.yml` and re-run `setup_job`.

---

## Dashboards

### Claims Analytics Dashboard

Lakeview dashboard backed by the gold summary tables.

| Page | Source table | Content |
|---|---|---|
| Claims Operations | `summary_claims_daily` | Daily volume, severity breakdown, denial rate |
| Financial Performance | `summary_financial_kpis` | Loss ratio, storm financial exposure |
| Fraud Intelligence | `summary_fraud_intelligence` | Fraud rate by region and claim type |
| Triage Performance | `summary_triage_performance` | Assessor turnaround, repair cost alignment |

**Source:** `src/dashboards/insurance_poc_databricks_demo.lvdash.json`

### Billing Usage Dashboard

Lakeview dashboard for workspace billing analysis backed by `monitoring_dev.system_billing`.

**Source:** `src/dashboards/billing_usage.lvdash.json`

---

## Alerting

A daily DQ alert runs at **08:00 Europe/London** and sends an email when the quarantine table contains new failures from the past 24 hours. Defined in `resources/alerts/alert_dq_failures.yml`.

---

## CI/CD

GitHub Actions workflow at `.github/workflows/ci.yml`. Triggers on pushes to `main` and `feature*` branches.

```
push to main / feature* branch
        │
        ├── Lint          ruff check src/ tests/
        ├── Unit Tests    pytest tests/ (PySpark local mode)
        └── Validate      databricks bundle validate --target dev
                │
                └── Deploy  (push events only)
                        ├── Bootstrap UC catalogs
                        ├── databricks bundle deploy --target dev --auto-approve
                        ├── Deploy billing app
                        └── Deploy fraud intelligence app
```

**Required GitHub secrets** (Settings → Environments → dev):

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | Personal access token with CAN_MANAGE on bundle resources |

---

## Multi-environment configuration

| Variable | dev | prod |
|---|---|---|
| `bronze_catalog` | `bronze_dev` | `bronze_prod` |
| `silver_catalog` | `silver_dev` | `silver_prod` |
| `gold_catalog` | `gold_dev` | `gold_prod` |
| `monitoring_catalog` | `monitoring_dev` | `monitoring_prod` |
| `app_service_principal` | billing app SP client ID | billing app SP client ID (prod) |
| `fraud_app_service_principal` | fraud app SP client ID | fraud app SP client ID (prod) |

```bash
databricks bundle deploy --target prod
databricks bundle run main_job --target prod
```

**First-time prod app SP setup:**
```bash
# Deploy bundle (creates prod apps)
databricks bundle deploy --target prod

# Get prod SP IDs
databricks apps get databricks-billing-usage-prod --output json | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id'))"

# Update prod target in databricks.yml, then re-run setup_job
databricks bundle run setup_job --target prod
```
