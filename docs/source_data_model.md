# Insurance Claims — Source Data Model

**Landing layer**: `bronze_dev.raw_claims` (CSV files in Volume → Delta tables via Autoloader)

## Entity Relationship Diagram

```mermaid
erDiagram

    POLICYHOLDERS {
        string  policyholder_id  PK  "PH-XXXXXX"
        string  first_name
        string  last_name
        date    date_of_birth
        string  gender              "M / F / Other"
        string  email
        string  phone
        string  street_address
        string  city
        string  region              "UK county / region"
        string  postcode            "UK postcode"
        int     driving_points      "UK penalty points (0–12)"
        int     driving_years
        string  licence_number      "UK driving licence"
        date    created_date
    }

    VEHICLES {
        string  vehicle_id        PK  "VH-XXXXXX"
        string  policyholder_id   FK
        string  make                  "VW / Ford / Vauxhall …"
        string  vehicle_type          "Hatchback / Saloon / SUV …"
        int     year
        string  vin                   "17-char VIN"
        string  colour
        double  estimated_value       "GBP"
        int     mileage
        date    created_date
    }

    POLICIES {
        string  policy_id         PK  "POL-XXXXXX"
        string  policyholder_id   FK
        string  vehicle_id        FK
        string  policy_type           "Comprehensive / TPFT / TPO"
        string  coverage_type
        double  premium_monthly       "GBP / month"
        double  excess                "GBP — UK term for deductible"
        double  coverage_limit        "GBP"
        date    start_date
        date    end_date
        string  status                "Active / Lapsed / Cancelled"
        date    created_date
    }

    CLAIMS {
        string  claim_id          PK  "CLM-XXXXXX"
        string  policy_id         FK
        string  policyholder_id   FK
        string  vehicle_id        FK
        string  claim_type            "weather / collision / theft / vandalism / fire"
        date    claim_date
        string  incident_region       "UK region where incident occurred"
        string  incident_city
        string  severity_label        "minor / moderate / severe / total_loss"
        double  fraud_risk_score      "0.0 – 1.0"
        double  claim_amount_gbp      "GBP"
        string  claim_status          "open / under_review / approved / denied / settled"
        string  is_storm_related      "Y / N — Storm Freya Feb 2026"
        string  description
        date    created_date
    }

    INCIDENTS {
        string  incident_id       PK  "INC-XXXXXX"
        string  claim_id          FK
        string  incident_type         "collision / flood_damage / storm_damage …"
        date    incident_date
        string  weather_condition     "clear / heavy_rain / flood / snow / fog / strong_wind / hail"
        string  road_condition        "dry / wet / flooded / icy / waterlogged …"
        string  visibility            "good / moderate / poor"
        string  police_report_filed   "Y / N"
        int     num_vehicles_involved
        int     witness_count
        date    created_date
    }

    CLAIM_ASSESSMENTS {
        string  assessment_id          PK  "ASS-XXXXXX"
        string  claim_id               FK
        string  assessor_id                "ASR-XXXX"
        string  assessor_tier              "junior / senior / specialist"
        date    assessment_date
        string  damage_category            "exterior_bodywork / mechanical / total_loss / glass_windscreen …"
        double  estimated_repair_cost_gbp  "GBP"
        double  labour_hours
        double  parts_cost_gbp             "GBP"
        string  recommended_action         "repair / replace / write_off / deny / further_investigation"
        date    created_date
    }

    POLICYHOLDERS  ||--o{  VEHICLES          : "owns (1 PH → many vehicles)"
    POLICYHOLDERS  ||--o{  POLICIES          : "holds (1 PH → many policies)"
    VEHICLES       ||--o{  POLICIES          : "covered by (1 vehicle → many policies)"
    POLICIES       ||--o{  CLAIMS            : "generates (1 policy → many claims)"
    POLICYHOLDERS  ||--o{  CLAIMS            : "files (1 PH → many claims)"
    VEHICLES       ||--o{  CLAIMS            : "involved in (1 vehicle → many claims)"
    CLAIMS         ||--||  INCIDENTS         : "triggered by (1 claim → 1 incident)"
    CLAIMS         ||--o{  CLAIM_ASSESSMENTS : "assessed via (1 claim → 1–3 assessments)"
```

---

## Table Descriptions

### POLICYHOLDERS — 50,000 rows
Master record for every insured customer. Age distribution skewed **35–55**; **70% have zero penalty points**. Regions weighted toward Greater London, West Yorkshire and West Midlands.

### VEHICLES — 55,000 rows
Vehicles owned by policyholders. UK-popular makes (VW, Ford, Vauxhall, Toyota, BMW). One policyholder can own multiple vehicles. Values follow a log-normal distribution (~£15K median).

### POLICIES — 60,000 rows
Insurance policies linking a policyholder to a specific vehicle. Policy types follow UK conventions: **Comprehensive** (most common), **Third Party Fire & Theft**, **Third Party Only**. One vehicle can have multiple policies over time.

### CLAIMS — 120,000 rows
Core fact table. **35% of claims (42,000) are Storm Freya related** (Feb 3–17 2026, concentrated in Yorkshire and Somerset). Storm claims show:
- Higher severity distribution (more `severe` / `total_loss`)
- 14% fraud risk rate vs 8% baseline
- Weather `claim_type` dominant

### INCIDENTS — 120,000 rows
One incident record per claim (1:1). Captures the physical circumstances of the event — weather, road conditions, visibility, police involvement. UK weather conditions include `flood`, `heavy_rain`, `strong_wind` reflecting realistic British weather patterns.

### CLAIM_ASSESSMENTS — 180,000 rows
Between 1–3 assessments per claim (~1.5 average). `assessor_tier` determines the level of expertise assigned. `recommended_action` of `write_off` is the UK equivalent of `total_loss`.

---

## Key Business Rules

| Rule | Detail |
|---|---|
| A policy must link to exactly one policyholder and one vehicle | `POLICIES.policyholder_id` and `POLICIES.vehicle_id` are NOT NULL |
| A claim must reference a valid policy | `CLAIMS.policy_id` is NOT NULL |
| Every claim has exactly one incident record | `INCIDENTS.claim_id` is unique |
| A claim can have 1–3 assessments | `CLAIM_ASSESSMENTS.claim_id` repeats |
| Storm Freya period | `claim_date` between 2026-02-03 and 2026-02-17, `is_storm_related = Y` |
| Fraud flag threshold | `fraud_risk_score >= 0.6` → `fraud_flag = true` in silver layer |

---

## Medallion Architecture

```
Volume (CSV)                     bronze_dev.raw_claims
─────────────────────────────    ──────────────────────────────────────────
policyholders/   (51,500 rows)   policyholders  vehicles  policies
vehicles/        (56,650 rows)   claims         incidents  claim_assessments
policies/        (61,800 rows)
claims/         (123,600 rows)         ↓  Bronze → Silver (DQ + type casting)
incidents/      (123,600 rows)
claim_assessments/(185,400 rows) silver_dev.refined_claims
                                 ──────────────────────────────────────────
~3% of rows are intentional      Same 6 tables (clean) + quarantine table
bad data for DQ validation       (all DQ-failed records with raw JSON)

                                       ↓  Silver → Gold (transforms)

                                 gold_dev.dimensions   gold_dev.facts
                                 gold_dev.features     gold_dev.summary
```
