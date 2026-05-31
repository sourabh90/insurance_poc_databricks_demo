# Insurance Claims — Gold Data Model

**Layer**: `gold_dev` — 4 schemas serving distinct purposes

| Schema | Purpose | Tables |
|---|---|---|
| `dimensions` | Reference / master data for star schema | dim_date, dim_geography, dim_policyholder, dim_vehicle, dim_policy |
| `facts` | Transactional grain tables | fact_claim, fact_assessment |
| `features` | ML-ready feature tables | claim_features, policyholder_risk_profile |
| `summary` | Pre-aggregated BI KPI tables | summary_claims_daily, summary_financial_kpis, summary_fraud_intelligence, summary_triage_performance |

---

## 1. Star Schema — Dimensions & Facts

```mermaid
erDiagram

    DIM_DATE {
        int      date_key         PK  "yyyyMMdd integer"
        date     date
        int      year
        int      quarter
        int      month
        string   month_name
        int      week_of_year
        int      day_of_month
        int      day_of_week
        string   day_name
        boolean  is_weekend
        boolean  is_storm_period      "True for 2026-02-03 to 2026-02-17"
    }

    DIM_GEOGRAPHY {
        string   geo_key          PK  "region-city concatenation"
        string   incident_region
        string   incident_city
        string   uk_area              "Yorkshire / Midlands / North West …"
        boolean  is_storm_region      "True for West/South Yorkshire, Somerset, Devon, Gtr Manchester"
    }

    DIM_POLICYHOLDER {
        string   policyholder_id  PK
        string   first_name
        string   last_name
        string   gender
        int      age
        string   region
        string   city
        string   postcode
        int      driving_years
        int      driving_points
        string   age_band             "17-24 / 25-34 / 35-44 / 45-54 / 55-64 / 65+"
        string   risk_tier            "low / medium / high (based on driving_points)"
    }

    DIM_VEHICLE {
        string   vehicle_id       PK
        string   policyholder_id  FK
        string   make
        string   vehicle_type
        int      year
        string   colour
        double   estimated_value      "GBP"
        int      mileage
        int      vehicle_age          "Derived: 2026 minus year"
        string   vehicle_age_band     "0-3 / 4-7 / 8-12 / 13+ years"
        string   value_band_gbp       "<£8K / £8K-£20K / £20K-£40K / £40K+"
    }

    DIM_POLICY {
        string   policy_id        PK
        string   policyholder_id  FK
        string   vehicle_id       FK
        string   policy_type          "Comprehensive / TPFT / TPO"
        string   coverage_type
        string   status
        double   premium_monthly      "GBP"
        double   excess               "GBP"
        double   coverage_limit       "GBP"
        date     start_date
        date     end_date
        double   annual_premium_gbp   "Derived: premium_monthly × 12"
        int      policy_duration_days
        string   premium_band         "<£60/mo / £60-£100/mo / £100-£200/mo / £200+/mo"
    }

    FACT_CLAIM {
        string   claim_id         PK
        string   policy_id        FK
        string   policyholder_id  FK
        string   vehicle_id       FK
        int      claim_date_key   FK  "→ DIM_DATE"
        string   incident_region  FK  "→ DIM_GEOGRAPHY (via region)"
        string   claim_type
        string   claim_status
        string   severity_label
        boolean  is_storm_related
        boolean  fraud_flag
        double   fraud_risk_score
        double   claimed_amount_gbp
        double   excess
        double   net_payout_gbp       "claimed_amount - excess (floor 0)"
        string   policy_type
        double   premium_monthly
        double   annual_premium_gbp
        double   coverage_limit
        int      age
        string   gender
        string   ph_region
        int      driving_points
        string   make
        string   vehicle_type
        int      vehicle_year
        double   vehicle_value_gbp
    }

    FACT_ASSESSMENT {
        string   assessment_id          PK
        string   claim_id               FK  "→ FACT_CLAIM"
        int      assessment_date_key    FK  "→ DIM_DATE"
        string   assessor_id
        string   assessor_tier
        string   damage_category
        string   recommended_action
        double   estimated_repair_cost_gbp
        double   labour_hours
        double   parts_cost_gbp
        double   total_assessment_cost_gbp
        string   claim_type
        string   severity_label
        boolean  is_storm_related
        string   incident_region
        date     claim_date
        int      days_to_assessment     "assessment_date minus claim_date"
    }

    DIM_DATE         ||--o{  FACT_CLAIM       : "claim_date_key"
    DIM_DATE         ||--o{  FACT_ASSESSMENT  : "assessment_date_key"
    DIM_GEOGRAPHY    ||--o{  FACT_CLAIM       : "incident_region"
    DIM_POLICYHOLDER ||--o{  FACT_CLAIM       : "policyholder_id"
    DIM_VEHICLE      ||--o{  FACT_CLAIM       : "vehicle_id"
    DIM_POLICY       ||--o{  FACT_CLAIM       : "policy_id"
    FACT_CLAIM       ||--o{  FACT_ASSESSMENT  : "claim_id"
```

---

## 2. ML Feature Tables — `gold_dev.features`

Two feature tables, one per ML use case. All columns are numeric or binary-encoded — no free-text fields.

```mermaid
erDiagram

    CLAIM_FEATURES {
        string  claim_id              PK
        int     severity_encoded          "0=minor 1=moderate 2=severe 3=total_loss"
        string  severity_label            "Target label"
        int     f_type_weather
        int     f_type_collision
        int     f_type_theft
        int     f_type_vandalism
        int     f_type_fire
        double  f_claim_amount_gbp
        double  f_fraud_risk_score
        int     f_is_storm
        double  f_excess_gbp
        double  f_coverage_limit_gbp
        double  f_premium_monthly_gbp
        int     f_pol_comprehensive
        int     f_pol_tpft
        int     f_pol_tpo
        int     f_ph_age
        int     f_driving_years
        int     f_driving_points
        int     f_vehicle_age
        double  f_vehicle_value_gbp
        double  f_mileage_k
        int     f_vtype_suv
        int     f_vtype_estate
        int     f_weather_flood
        int     f_weather_rain
        int     f_weather_wind
        int     f_weather_clear
        int     f_road_flooded
        int     f_road_icy
        int     f_visibility_poor
        int     f_num_vehicles
        int     f_witness_count
        int     f_police_report
        int     f_num_assessments
        double  f_avg_repair_cost_gbp
        double  f_max_repair_cost_gbp
        double  f_total_labour_hours
    }

    POLICYHOLDER_RISK_PROFILE {
        string  policyholder_id       PK
        int     age
        string  gender
        string  region
        int     driving_years
        int     driving_points
        int     total_claims
        int     storm_claims
        double  avg_claim_amount_gbp
        double  max_claim_amount_gbp
        double  avg_fraud_score
        double  max_fraud_score
        int     flagged_claims
        int     total_loss_claims
        int     severe_claims
        int     total_policies
        double  avg_premium_monthly_gbp
        double  claim_frequency           "total_claims / driving_years"
        double  risk_score                "Composite 0–1 (driving_points + claim_freq + fraud)"
        string  risk_tier                 "low / medium / high / very_high"
    }
```

### Feature Engineering Notes

| Feature group | Source tables joined | ML use case |
|---|---|---|
| Claim type flags | `claims` | Severity prediction |
| Policy / excess / limit | `claims` + `policies` | Severity & fraud |
| Policyholder driving profile | `claims` + `policyholders` | Risk scoring |
| Vehicle age & value | `claims` + `vehicles` | Severity prediction |
| Weather / road / visibility | `incidents` | Severity prediction |
| Assessment cost aggregates | `claim_assessments` | Severity & triage |
| Claim history per PH | `claims` aggregated | Risk scoring |

---

## 3. Pre-Aggregated Summary Tables — `gold_dev.summary`

Flat aggregation tables used by BI dashboards. No joins needed at query time.

```mermaid
erDiagram

    SUMMARY_CLAIMS_DAILY {
        date    claim_date            PK  "Grain: date + region + type + severity"
        string  incident_region       PK
        string  claim_type            PK
        string  severity_label        PK
        boolean is_storm_related      PK
        string  month
        date    week_start
        int     claim_count
        double  total_claimed_gbp
        double  avg_claimed_gbp
        double  max_claimed_gbp
        double  approved_amount_gbp
        int     denied_count
        double  settled_amount_gbp
        int     open_count
        int     total_loss_count
    }

    SUMMARY_FINANCIAL_KPIS {
        string  month                 PK  "Grain: month + policy_type + region"
        string  policy_type           PK
        string  incident_region       PK
        int     total_claims
        double  total_claims_gbp
        double  total_net_payout_gbp
        double  avg_claim_gbp
        double  total_monthly_premium_gbp
        double  total_loss_gbp
        double  storm_claims_gbp
        int     unique_claimants
        double  loss_ratio                "net_payout / monthly_premium"
    }

    SUMMARY_FRAUD_INTELLIGENCE {
        string  month                 PK  "Grain: month + region + claim_type + storm flag"
        string  incident_region       PK
        string  claim_type            PK
        boolean is_storm_related      PK
        int     total_claims
        int     flagged_claims
        double  avg_fraud_score
        double  max_fraud_score
        double  flagged_claims_gbp
        double  total_claims_gbp
        double  fraud_rate                "flagged / total"
        double  fraud_amount_ratio        "flagged_gbp / total_gbp"
    }

    SUMMARY_TRIAGE_PERFORMANCE {
        string  month                 PK  "Grain: month + assessor_tier + damage + action + severity"
        string  assessor_tier         PK
        string  damage_category       PK
        string  recommended_action    PK
        string  severity_label        PK
        boolean is_storm_related      PK
        int     total_assessments
        double  avg_days_to_assess
        int     max_days_to_assess
        double  avg_repair_cost_gbp
        double  total_repair_cost_gbp
        double  avg_labour_hours
        int     unique_claims_assessed
        double  severity_action_alignment_rate
    }
```

### Dashboard Mapping

| Summary Table | BI Dashboard |
|---|---|
| `summary_claims_daily` | Claims Operations — daily volume, severity breakdown by region |
| `summary_financial_kpis` | Financial Performance — loss ratio, premiums vs payouts by month |
| `summary_fraud_intelligence` | Fraud Intelligence — fraud rate by region and claim type, storm vs baseline |
| `summary_triage_performance` | Triage & ML Performance — assessment turnaround, write-off alignment |

---

## Data Lineage Overview

```
silver_dev.refined_claims
  ├── policyholders ──────┐
  ├── vehicles ───────────┤
  ├── policies ───────────┤──► gold_dev.dimensions  (dim_policyholder, dim_vehicle, dim_policy, dim_geography, dim_date)
  ├── claims ─────────────┤
  ├── incidents ──────────┤
  └── claim_assessments ──┘
                          │
                          ├──► gold_dev.facts       (fact_claim, fact_assessment)
                          │
                          ├──► gold_dev.features    (claim_features, policyholder_risk_profile)
                          │
                          └──► gold_dev.summary     (summary_claims_daily, summary_financial_kpis,
                                                     summary_fraud_intelligence, summary_triage_performance)
```
