# Insurance POC — Databricks Demo

A production-grade proof-of-concept demonstrating a complete **medallion architecture** (Bronze → Silver → Gold) for UK insurance claims analytics, built on Databricks. The scenario models Storm Freya (February 2026) across UK regions, with 35% of 120,000 claims identified as storm-related.

## What this project demonstrates

- **Medallion architecture** — Landing Zone → Bronze (raw) → Silver (refined) → Gold (analytics/ML)
- **Streaming ingestion** — Autoloader with schema rescue and lineage tracking
- **Data quality framework** — Hard blocks (quarantine and drop) and soft warnings (flag and pass), centralized quarantine table
- **Star schema modeling** — Dimension and fact tables for OLAP queries
- **ML-ready feature engineering** — Severity prediction and policyholder risk scoring
- **BI pre-aggregations** — Summary tables for dashboards and KPI reporting
- **Declarative infrastructure** — Databricks Asset Bundles (DAB) with `dev` / `prod` targets
- **Observability** — DQ alert with daily email notification, SQL investigation queries

---

## Architecture overview

```
Landing Volume (CSV)
       │
       ▼
  Bronze Layer           raw_claims schema
  (landing_to_bronze)    6 Delta tables + _ingested_at, _source_file, _rescued_data

       │
       ▼
  Silver Layer           refined_claims schema
  (bronze_to_silver)     6 clean Delta tables + 1 quarantine table
                         Hard blocks: drop record → quarantine only
                         Soft warnings: keep record AND write to quarantine

       │
       ├──────────────── Gold Dimensions   (dim_date, dim_geography, dim_policyholder,
       │                                    dim_vehicle, dim_policy)
       ├──────────────── Gold Facts        (fact_claim, fact_assessment)
       ├──────────────── Gold Features     (claim_features, policyholder_risk_profile)
       └──────────────── Gold Summary      (summary_claims_daily, summary_financial_kpis,
                                            summary_fraud_intelligence, summary_triage_performance)
```

---

## Project structure

```
insurance_poc_databricks_demo/
├── databricks.yml                  # Bundle root config — variables and targets (dev/prod)
├── docs/
│   ├── source_data_model.md        # Bronze layer — 6 entities and ERD
│   ├── silver_data_model.md        # Silver layer — DQ rules, hard/soft blocks
│   └── gold_data_model.md          # Gold layer — star schema, ML features, summary KPIs
├── src/
│   ├── notebooks/
│   │   ├── setup_catalogs.py       # Creates UC catalogs, schemas, and volumes
│   │   ├── generate_synthetic_data.py  # Faker-based data generation (600K+ rows, 3% bad data)
│   │   ├── main.py                 # Display catalog tables
│   │   └── cleanup.py              # Full teardown (reversible)
│   ├── pipelines/
│   │   ├── landing_to_bronze.py    # Autoloader: Volume CSV → raw Delta tables
│   │   ├── bronze_to_silver.py     # Type casting + DQ checks + quarantine
│   │   ├── gold_dimensions.py      # 5 dimension tables
│   │   ├── gold_facts.py           # 2 fact tables (claim, assessment)
│   │   ├── gold_features.py        # 2 ML feature tables
│   │   └── gold_summary.py         # 4 BI pre-aggregation tables
│   ├── queries/
│   │   ├── dq_summary.sql          # DQ failures by table and rule
│   │   ├── dq_hard_blocks.sql      # Dropped records
│   │   ├── dq_soft_warnings.sql    # Flagged-but-passed records
│   │   ├── dq_claims_investigation.sql  # JSON extraction from failed claims
│   │   ├── dq_daily_trend.sql      # Trend analysis over time
│   │   └── dq_multi_issue_records.sql   # Records with multiple DQ violations
│   ├── dashboards/
│   │   └── insurance_poc_databricks_demo.lvdash.json  # BI dashboard config
│   └── app/
│       ├── app.yaml                # Streamlit app config
│       └── app.py                  # Table browser Streamlit app
└── resources/
    ├── pipelines/                  # DLT pipeline resource definitions (6 files)
    ├── jobs/                       # Orchestration job definitions (4 files)
    ├── dashboards/                 # Dashboard resource definition
    ├── alerts/                     # DQ failure alert (daily, mailed to owner)
    └── apps/                       # Streamlit app deployment config
```

---

## Data model

### Bronze — source entities

| Entity | Rows | Description |
|---|---|---|
| `policyholders` | 50K | Customer demographics, UK driving record (0–12 penalty points), region |
| `vehicles` | 55K | Make, model year, VIN, estimated value (£), mileage |
| `policies` | 60K | Type (Comprehensive / TPFT / TPO), monthly premium, excess, coverage limit |
| `claims` | 120K | Claim type, amount, severity, fraud score, storm flag |
| `incidents` | 120K | Weather/road conditions, visibility, police report |
| `claim_assessments` | 180K | Assessor tier, damage category, repair cost, recommended action |

**Storm Freya scenario** (Feb 3–17, 2026): 42K claims (35%) flagged as storm-related, concentrated in West Yorkshire, South Yorkshire, Somerset, Greater Manchester, and Devon. Storm claims show 20% total-loss rate (vs 5% baseline) and 14% fraud rate (vs 8% baseline).

### Silver — DQ rules

**Hard blocks** (record dropped, written to quarantine only):
- Invalid ID format (must match `CLM-*`, `PH-*`, `VH-*`, etc.)
- Missing foreign keys
- Non-positive claim amounts

**Soft warnings** (record passes to clean table AND written to quarantine):
- Invalid email format
- Year out of range
- Fraud score outside `[0.0, 1.0]`

All violations land in a single `quarantine` table with columns: `source_table`, `record_id`, `dq_rule_violated` (pipe-separated for multi-issue records), `raw_record` (JSON), `quarantined_at`.

### Gold — analytics and ML

**Dimensions**: `dim_date` (1,826 days with storm-period flag), `dim_geography` (region/city with storm-region flag), `dim_policyholder` (age bands, risk tiers), `dim_vehicle` (age/value bands), `dim_policy` (premium bands)

**Facts**: `fact_claim` (120K rows — net payout, denormalized driver and vehicle attributes), `fact_assessment` (180K rows — days-to-assessment, repair costs)

**ML features**:
- `claim_features` — 35 numeric/binary features per claim; target: `severity_encoded` (0–3)
- `policyholder_risk_profile` — risk score (0–1) and tier (low / medium / high / very_high) per policyholder

**Summary tables** (BI pre-aggregations):
- `summary_claims_daily` — daily volume, amounts, denial rate, open count by region/type/severity
- `summary_financial_kpis` — loss ratio, storm damage, total-loss rate by month/policy-type/region
- `summary_fraud_intelligence` — fraud rate, fraud amount ratio, score distribution
- `summary_triage_performance` — assessor turnaround time, repair cost alignment, severity-action match rate

---

## Getting started

### Prerequisites

- Databricks CLI v0.296.0+, authenticated (`databricks auth login`)
- Unity Catalog enabled on your workspace
- Serverless compute available (Serverless DLT + Serverless Starter Warehouse)

### Deploy and run

```bash
# 1. Deploy all resources to dev
databricks bundle deploy --target dev

# 2. Create catalogs, schemas, and landing volume
databricks bundle run setup_job --target dev

# 3. Generate synthetic data (600K+ rows, ~3% bad data injected)
databricks bundle run data_gen_job --target dev

# 4. Run the full medallion pipeline (6 DLT pipelines, sequential)
databricks bundle run main_job --target dev
```

The main job runs pipelines in order: `landing_to_bronze` → `bronze_to_silver` → `load_gold_dimensions` → `load_gold_facts` → `load_gold_features` → `load_gold_summary`.

### Investigate data quality

```sql
-- Failures by table and rule
SELECT source_table, dq_rule_violated, COUNT(*) AS failures
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Records with multiple violations (systemic issues)
SELECT *, LENGTH(dq_rule_violated) - LENGTH(REPLACE(dq_rule_violated, '|', '')) + 1 AS issue_count
FROM silver_dev.refined_claims.quarantine
WHERE dq_rule_violated LIKE '%|%'
ORDER BY issue_count DESC;

-- Daily trend
SELECT DATE(quarantined_at) AS day, source_table, COUNT(*) AS total_failures
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2 ORDER BY 1 DESC;
```

All six investigation queries are also available pre-written in [src/queries/](src/queries/).

### Teardown

```bash
# Drop all volumes, schemas, and catalogs (fully reversible)
databricks bundle run cleanup_job --target dev
```

---

## Multi-environment configuration

The bundle ships with `dev` (default) and `prod` targets. Catalog names change per target; pipeline and job configs are shared.

| Variable | dev | prod |
|---|---|---|
| `bronze_catalog` | `bronze_dev` | `bronze_prod` |
| `silver_catalog` | `silver_dev` | `silver_prod` |
| `gold_catalog` | `gold_dev` | `gold_prod` |

```bash
databricks bundle deploy --target prod
databricks bundle run main_job --target prod
```

---

## Alerting

A daily DQ alert runs at **08:00 Europe/London** and sends an email when the quarantine table contains any new failures from the past 24 hours. Alert is defined in [resources/alerts/](resources/alerts/) and is active by default.

---

## Tech stack

| Component | Technology |
|---|---|
| Ingestion | Databricks Autoloader (streaming) |
| Transformation | Databricks Delta Live Tables (Serverless) |
| Storage | Delta Lake on Unity Catalog |
| Orchestration | Databricks Workflows |
| Infrastructure | Databricks Asset Bundles |
| BI | Databricks Lakeview Dashboard |
| App | Streamlit on Databricks Apps |
| Data generation | PySpark + Faker (`en_GB` locale) |
