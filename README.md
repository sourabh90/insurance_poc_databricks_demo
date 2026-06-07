# Insurance POC — Databricks Demo

A production-grade proof-of-concept demonstrating a complete **medallion architecture** (Bronze → Silver → Gold) for UK insurance claims analytics, built on Databricks. The scenario models Storm Freya (February 2026) across UK regions, with 35% of 120,000 claims identified as storm-related.

## What this project demonstrates

- **Medallion architecture** — Landing Zone → Bronze (raw) → Silver (refined) → Gold (analytics/ML)
- **Streaming ingestion** — Autoloader with schema rescue and lineage tracking
- **Data quality framework** — Hard blocks (quarantine and drop) and soft warnings (flag and pass), centralised quarantine table
- **Star schema modelling** — Dimension and fact tables for OLAP queries
- **ML-ready feature engineering** — Severity prediction and policyholder risk scoring
- **BI pre-aggregations** — Summary tables for dashboards and KPI reporting
- **Declarative infrastructure** — Databricks Asset Bundles (DAB) with `dev` / `prod` targets
- **Two Streamlit apps** — Billing usage analytics and fraud intelligence dashboard
- **Lakeview dashboards** — Claims analytics and billing usage
- **Observability** — DQ alert with daily email notification, SQL investigation queries
- **CI/CD** — GitHub Actions: lint, unit tests, bundle validate, deploy, app redeploy on every push

---

## Architecture overview

```
Landing Volume (CSV)
       │
       ▼
  Bronze Layer           bronze_dev.raw_claims
  (landing_to_bronze)    6 Delta tables + _ingested_at, _source_file, _rescued_data

       │
       ▼
  Silver Layer           silver_dev.refined_claims
  (bronze_to_silver)     6 clean Delta tables + 1 quarantine table
                         Hard blocks: drop record → quarantine only
                         Soft warnings: keep record AND write to quarantine

       │
       ├──────────────── Gold Dimensions   gold_dev.dimensions
       │                                   (dim_date, dim_geography, dim_policyholder,
       │                                    dim_vehicle, dim_policy)
       ├──────────────── Gold Facts        gold_dev.facts
       │                                   (fact_claim, fact_assessment)
       ├──────────────── Gold Features     gold_dev.features
       │                                   (claim_features, policyholder_risk_profile)
       └──────────────── Gold Summary      gold_dev.summary
                                           (summary_claims_daily, summary_financial_kpis,
                                            summary_fraud_intelligence, summary_triage_performance)

system.billing / system.lakeflow
       │
       ▼  (nightly sync job — sync_system_billing)
  Monitoring Layer       monitoring_dev.system_billing
                         (usage, list_prices, pipelines)
                         Replicated so Databricks Apps can query without
                         metastore-admin grants on the system catalog.
```

---

## Project structure

```
insurance_poc_databricks_demo/
├── databricks.yml                    # Bundle root — variables and targets (dev/prod)
├── scripts/
│   └── bootstrap_catalogs.py        # Pre-deploy: create UC catalogs before bundle deploy
├── docs/
│   ├── source_data_model.md         # Bronze layer — 6 entities and ERD
│   ├── silver_data_model.md         # Silver layer — DQ rules, hard/soft blocks
│   └── gold_data_model.md           # Gold layer — star schema, ML features, summary KPIs
├── src/
│   ├── notebooks/
│   │   ├── setup_catalogs.py        # Creates UC catalogs, schemas, volumes; applies app SP grants
│   │   ├── generate_synthetic_data.py   # Faker-based data generation (600K+ rows, 3% bad data)
│   │   ├── sync_system_billing.py   # Nightly sync: system.billing → monitoring_dev
│   │   ├── ingest_fx_rates.py       # Ingests ECB FX rates into bronze_dev.raw_reference
│   │   ├── main.py                  # Display catalog tables
│   │   └── cleanup.py               # Full teardown (reversible)
│   ├── pipelines/
│   │   ├── landing_to_bronze.py     # Autoloader: Volume CSV → raw Delta tables
│   │   ├── bronze_to_silver.py      # Type casting + DQ checks + quarantine
│   │   ├── dq_utils.py              # DQ helper: add_dq() and to_quarantine()
│   │   ├── gold_dimensions.py       # 5 dimension tables
│   │   ├── gold_facts.py            # 2 fact tables (claim, assessment)
│   │   ├── gold_features.py         # 2 ML feature tables
│   │   └── gold_summary.py          # 4 BI pre-aggregation tables
│   ├── queries/
│   │   ├── dq_summary.sql           # DQ failures by table and rule
│   │   ├── dq_hard_blocks.sql       # Dropped records
│   │   ├── dq_soft_warnings.sql     # Flagged-but-passed records
│   │   ├── dq_claims_investigation.sql   # JSON extraction from failed claims
│   │   ├── dq_daily_trend.sql       # Trend analysis over time
│   │   └── dq_multi_issue_records.sql    # Records with multiple DQ violations
│   ├── dashboards/
│   │   ├── insurance_poc_databricks_demo.lvdash.json   # Claims analytics dashboard
│   │   └── billing_usage.lvdash.json                   # Billing usage dashboard
│   ├── app/                         # Billing Usage Streamlit app
│   │   ├── app.py
│   │   ├── app.yaml
│   │   └── requirements.txt
│   └── fraud_app/                   # Fraud Intelligence Streamlit app
│       ├── app.py
│       ├── app.yaml
│       └── requirements.txt
└── resources/
    ├── pipelines/                   # DLT pipeline resource definitions (6 files)
    ├── jobs/                        # Orchestration job definitions (6 files)
    ├── dashboards/                  # Dashboard resource definitions (2 files)
    ├── alerts/                      # DQ failure alert (daily, emailed to owner)
    └── apps/                        # Databricks Apps resource definitions (2 files)
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

All violations land in a single `quarantine` table: `source_table`, `record_id`, `dq_rule_violated` (pipe-separated), `raw_record` (JSON), `quarantined_at`.

### Gold — analytics and ML

**Dimensions**: `dim_date`, `dim_geography`, `dim_policyholder`, `dim_vehicle`, `dim_policy`

**Facts**: `fact_claim` (120K rows — net payout, denormalised), `fact_assessment` (180K rows — days-to-assessment, repair costs)

**ML features**:
- `claim_features` — 35 numeric/binary features per claim; target: `severity_encoded` (0–3)
- `policyholder_risk_profile` — composite risk score (0–1) and tier (low / medium / high / very_high)

**Summary tables** (BI pre-aggregations — no joins needed at query time):
- `summary_claims_daily` — daily volume, amounts, denial rate, open count by region/type/severity
- `summary_financial_kpis` — loss ratio, storm damage, total-loss rate by month/policy-type/region
- `summary_fraud_intelligence` — fraud rate, fraud amount ratio, score distribution by month/region/type
- `summary_triage_performance` — assessor turnaround time, repair cost alignment, severity-action match rate

### Monitoring — system billing replica

| Table | Source | Refresh |
|---|---|---|
| `monitoring_dev.system_billing.usage` | `system.billing.usage` | Nightly incremental (7-day overlap window) |
| `monitoring_dev.system_billing.list_prices` | `system.billing.list_prices` | Nightly full replace |
| `monitoring_dev.system_billing.pipelines` | `system.lakeflow.pipelines` | Nightly full replace |

Databricks Apps run as their own service principal. Granting a SP access to the `system` catalog requires metastore-admin privileges. This monitoring layer avoids that requirement by replicating the three tables the billing app needs into a user-owned catalog.

---

## Getting started

### Prerequisites

- Databricks CLI v0.279.0+, authenticated to your workspace
- Unity Catalog enabled on your workspace
- Serverless compute available (Serverless DLT + Serverless Starter Warehouse)
- GitHub Actions secrets configured: `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (see [CI/CD](#cicd))

### One-time workspace setup

> **Default Storage workspaces**: if your account uses UC Default Storage, catalogs must be created via the Databricks UI (Catalog Explorer → Create catalog). The bootstrap script detects this and prints instructions. Create `bronze_dev`, `silver_dev`, `gold_dev`, and `monitoring_dev` before proceeding.

```bash
# 1. Bootstrap UC catalogs (creates bronze/silver/gold/monitoring — skips existing ones)
python scripts/bootstrap_catalogs.py --target dev --profile <your-profile>

# 2. Deploy all bundle resources (pipelines, jobs, dashboards, apps)
databricks bundle deploy --target dev

# 3. Create schemas, volumes, and apply app SP grants
databricks bundle run insurance_poc_databricks_demo_setup --target dev

# 4. Ingest FX rates (USD → GBP, used by billing app)
databricks bundle run insurance_poc_fx_rates --target dev

# 5. Generate synthetic claims data (600K+ rows, ~3% bad data injected)
databricks bundle run insurance_poc_databricks_demo_data_gen --target dev

# 6. Sync system billing tables to monitoring catalog (first full load)
databricks bundle run insurance_poc_sync_system_billing --target dev

# 7. Run the full medallion pipeline
databricks bundle run insurance_poc_databricks_demo_job --target dev
```

The main job runs pipelines sequentially: `landing_to_bronze` → `bronze_to_silver` → `load_gold_dimensions` → `load_gold_facts` → `load_gold_features` → `load_gold_summary`.

### Teardown

```bash
# Drop all volumes, schemas, and catalogs (fully reversible)
databricks bundle run insurance_poc_databricks_demo_cleanup --target dev
```

---

## Jobs reference

| Job resource key | Name | Description | Schedule |
|---|---|---|---|
| `insurance_poc_databricks_demo_setup` | Job - 1. Setup | Creates schemas, volumes; applies app SP grants | Manual |
| `insurance_poc_databricks_demo_data_gen` | Job - 2. Generate Data | Faker synthetic data (600K+ rows) | Manual |
| `insurance_poc_databricks_demo_job` | Job - 3. Main Orchestration | Runs all 6 DLT pipelines in order | Manual |
| `insurance_poc_fx_rates` | Job - FX Rates | Ingests ECB FX rates into bronze_dev.raw_reference | Manual |
| `insurance_poc_sync_system_billing` | Job - Sync System Billing | Replicates system.billing tables to monitoring_dev | Daily 02:00 Europe/London |
| `insurance_poc_databricks_demo_cleanup` | Job - Cleanup | Full teardown of catalogs and schemas | Manual |

---

## Applications

### Billing Usage App

**URL (dev):** `https://databricks-billing-usage-dev-<workspace-id>.aws.databricksapps.com`

Interactive Streamlit dashboard for compute cost analytics. Intended for workspace admins and engineering leads.

| Feature | Detail |
|---|---|
| KPIs | Total cost (£), total DBUs, active days, coverage period |
| Usage over time | Area chart by product, daily/weekly/monthly granularity |
| 30-day forecast | `ai_forecast()` overlay, respects selected granularity |
| Product breakdown | Cost by `billing_origin_product` |
| Top N workloads | DBU and cost by job/notebook/pipeline |
| Sidebar filters | Date range, product multi-select, granularity, Top N |

**Data sources:**
- `monitoring_dev.system_billing.usage` — compute usage records
- `monitoring_dev.system_billing.list_prices` — Databricks list pricing
- `monitoring_dev.system_billing.pipelines` — pipeline name lookup
- `bronze_dev.raw_reference.fx_rates_usd_gbp` — USD → GBP conversion

> The billing app reads from the monitoring catalog rather than `system.billing` directly, because Databricks Apps service principals cannot be granted access to the system catalog without metastore-admin privileges.

**Source:** `src/app/` · **DAB resource:** `resources/apps/app.yml`

---

### Fraud Intelligence App

**URL (dev):** `https://fraud-intelligence-dev-<workspace-id>.aws.databricksapps.com`

Risk and fraud analytics dashboard for the fraud investigation team.

| Feature | Detail |
|---|---|
| KPIs | Total claims, flagged claims, fraud rate %, fraud amount at risk (£) |
| Fraud rate by region | Horizontal bar chart (Reds colour scale), sorted by fraud rate |
| Storm Freya vs Baseline | Grouped bar — fraud rate % and amount ratio % for storm vs non-storm |
| Fraud trend over time | Monthly line chart split by claim type |
| Fraud rate by claim type | Bar chart with avg fraud score on hover |
| Region scatter plot | Fraud rate vs amount ratio, bubble sized by claim volume |
| Heatmap | Region × Claim Type fraud rate matrix — instantly surfaces hotspots |
| Detail table | Full breakdown by month, region, claim type, storm flag |
| Sidebar filters | Month range, region multi-select, claim type multi-select, storm toggle |

**Data source:** `gold_dev.summary.summary_fraud_intelligence`

**Source:** `src/fraud_app/` · **DAB resource:** `resources/apps/fraud_app.yml`

---

### App service principal grants

Each app runs as its own Databricks service principal. Grants are applied by `setup_catalogs.py` and are re-applied whenever `setup_job` is run. After deploying a new app for the first time, retrieve its SP client ID and add it to `databricks.yml`:

```bash
# Billing app
databricks apps get databricks-billing-usage-dev --output json | grep service_principal_client_id

# Fraud app
databricks apps get fraud-intelligence-dev --output json | grep service_principal_client_id
```

Then set `app_service_principal` and `fraud_app_service_principal` in the `dev` target of `databricks.yml` and re-run `setup_job`.

---

## Dashboards

### Claims Analytics Dashboard

Lakeview dashboard backed by the gold summary tables.

| Page / Widget | Source table | What it shows |
|---|---|---|
| Claims Operations | `summary_claims_daily` | Daily volume, severity breakdown, denial rate, storm vs baseline |
| Financial Performance | `summary_financial_kpis` | Loss ratio, premiums vs payouts, storm financial exposure |
| Fraud Intelligence | `summary_fraud_intelligence` | Fraud rate by region and claim type, storm period spike |
| Triage Performance | `summary_triage_performance` | Assessor turnaround, repair cost by damage category |

**Source:** `src/dashboards/insurance_poc_databricks_demo.lvdash.json`
**DAB resource:** `resources/dashboards/dashboard.yml`

---

### Billing Usage Dashboard

Lakeview dashboard for workspace billing analysis.

**Source:** `src/dashboards/billing_usage.lvdash.json`
**DAB resource:** `resources/dashboards/billing_usage_dashboard.yml`

---

## CI/CD

GitHub Actions workflow at `.github/workflows/ci.yml`. Triggers on pushes to `main` and any `feature*` branch. Pull requests to `main` run lint, tests, and validate only — deploy is skipped.

### Pipeline stages

```
push to main / feature* branch
        │
        ├── Lint          ruff check src/ tests/
        ├── Unit Tests    pytest tests/ (PySpark local mode)
        └── Validate ─────────────────────────────────── (needs: lint, test)
                │         databricks bundle validate --target dev
                │
                └── Deploy ───────────────────────────── (push events only)
                        │
                        ├── Bootstrap UC catalogs
                        │     python scripts/bootstrap_catalogs.py --target dev
                        ├── Bundle deploy
                        │     databricks bundle deploy --target dev
                        ├── Start + deploy billing app
                        │     databricks apps start databricks-billing-usage-dev  (if not ACTIVE)
                        │     databricks apps deploy databricks-billing-usage-dev
                        └── Start + deploy fraud intelligence app
                              databricks apps start fraud-intelligence-dev  (if not ACTIVE)
                              databricks apps deploy fraud-intelligence-dev
```

### Required GitHub secrets

Set these under **Settings → Environments → dev** in your repository:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `https://dbc-xxxxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | A personal access token with CAN_MANAGE on the bundle resources |

### Local deployment

The bundle uses `host:` rather than `profile:` so any CLI profile authenticated to the workspace works without coordination:

```bash
databricks bundle deploy --target dev                        # auto-picks matching profile
databricks bundle deploy --target dev --profile cog-personal # explicit profile if multiple match
```

---

## Investigate data quality

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

All six investigation queries are pre-written in [src/queries/](src/queries/).

---

## Alerting

A daily DQ alert runs at **08:00 Europe/London** and sends an email when the quarantine table contains new failures from the past 24 hours. Defined in [resources/alerts/](resources/alerts/) and active by default.

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
databricks bundle run insurance_poc_databricks_demo_job --target prod
```

---

## Tech stack

| Component | Technology |
|---|---|
| Ingestion | Databricks Autoloader (streaming) |
| Transformation | Databricks Delta Live Tables (Serverless) |
| Storage | Delta Lake on Unity Catalog |
| Orchestration | Databricks Workflows |
| Infrastructure | Databricks Asset Bundles |
| BI Dashboards | Databricks Lakeview |
| Applications | Streamlit on Databricks Apps (2 apps) |
| Data generation | PySpark + Faker (`en_GB` locale) |
| CI/CD | GitHub Actions |
| Linting | Ruff |
| Testing | pytest + PySpark local mode |
