-- Hard-blocked records: rows dropped from silver clean tables
-- These rules use expect_or_drop — the record never made it to silver
-- Rules: invalid_*_id, missing_*_fk, non_positive_claim_amount

SELECT
  source_table,
  record_id,
  dq_rule_violated,
  quarantined_at,
  raw_record
FROM silver_dev.refined_claims.quarantine
WHERE dq_rule_violated RLIKE
  'invalid_policyholder_id|invalid_vehicle_id|invalid_policy_id|invalid_claim_id|invalid_incident_id|invalid_assessment_id|missing_policyholder_fk|missing_vehicle_fk|missing_policy_fk|missing_claim_fk|non_positive_claim_amount'
ORDER BY source_table, quarantined_at DESC;
