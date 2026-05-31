-- Soft-warning records: rows that passed to silver but are flagged
-- These rules use expect (warn) — record is in BOTH clean table AND quarantine
-- Rules: invalid_email_format, year_out_of_range, non_positive_vehicle_value,
--        end_date_before_start_date, non_positive_premium,
--        invalid_severity_label, fraud_score_out_of_range

SELECT
  source_table,
  record_id,
  dq_rule_violated,
  quarantined_at,
  raw_record
FROM silver_dev.refined_claims.quarantine
WHERE dq_rule_violated NOT RLIKE
  'invalid_policyholder_id|invalid_vehicle_id|invalid_policy_id|invalid_claim_id|invalid_incident_id|invalid_assessment_id|missing_policyholder_fk|missing_vehicle_fk|missing_policy_fk|missing_claim_fk|non_positive_claim_amount'
ORDER BY source_table, quarantined_at DESC;
