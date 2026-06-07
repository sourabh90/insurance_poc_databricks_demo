# Insurance POC — Databricks Demo

A production-grade proof-of-concept demonstrating a complete **medallion architecture** (Bronze → Silver → Gold) for UK insurance claims analytics, built on Databricks. The scenario models Storm Freya (February 2026) across UK regions, with 35% of 120,000 synthetic claims identified as storm-related.

---

## What this project demonstrates

- **Medallion architecture** — Landing Zone → Bronze (Autoloader) → Silver (DQ + quarantine) → Gold (star schema + ML features)
- **Data quality framework** — Hard blocks and soft warnings with a centralised quarantine table
- **MLOps pipeline** — Feature engineering (DLT) → LightGBM training → MLflow tracking → batch inference → Lakehouse Monitoring
- **Two Streamlit apps** — Billing usage analytics and fraud intelligence dashboard
- **Lakeview dashboards** — Claims analytics and billing usage
- **Declarative infrastructure** — Databricks Asset Bundles (DAB) with `dev` / `prod` targets
- **CI/CD** — GitHub Actions: lint, unit tests, bundle validate, deploy on every push

---

## Documentation

| Guide | Contents |
|---|---|
| [Data Engineering](docs/data_engineering.md) | Medallion architecture, DLT pipelines, data model, jobs, apps, dashboards, CI/CD, multi-env config |
| [MLOps](docs/mlops.md) | Claim severity classifier, feature table, training, validation, batch inference, monitoring |
| [Source Data Model](docs/source_data_model.md) | Bronze layer — 6 source entities and ERD |
| [Silver Data Model](docs/silver_data_model.md) | Silver layer — DQ rules, hard/soft blocks |
| [Gold Data Model](docs/gold_data_model.md) | Gold layer — star schema, ML features, summary KPIs |

---

## Quick start

```bash
# 1. Bootstrap UC catalogs
python scripts/bootstrap_catalogs.py --target dev --profile <your-profile>

# 2. Deploy all bundle resources
databricks bundle deploy --target dev

# 3. One-time setup (schemas, volumes, SP grants)
databricks bundle run insurance_poc_databricks_demo_setup --target dev

# 4. Ingest FX rates
databricks bundle run insurance_poc_fx_rates --target dev

# 5. Generate synthetic data (600K+ rows)
databricks bundle run insurance_poc_databricks_demo_data_gen --target dev

# 6. Sync system billing tables
databricks bundle run insurance_poc_sync_system_billing --target dev

# 7. Run full medallion pipeline + ML training
databricks bundle run insurance_poc_databricks_demo_job --target dev
```

See [Data Engineering](docs/data_engineering.md) for the full getting-started guide and teardown instructions.

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
| Applications | Streamlit on Databricks Apps |
| ML training | LightGBM multi-class classifier |
| ML tracking | MLflow + Unity Catalog model registry |
| Monitoring | Databricks Lakehouse Monitoring (InferenceLog) |
| Data generation | PySpark + Faker (`en_GB` locale) |
| CI/CD | GitHub Actions |
