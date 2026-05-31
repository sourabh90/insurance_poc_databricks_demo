-- Claims DQ investigation: extract key fields from raw_record JSON
-- Use this to understand what went wrong in failed claim records

SELECT
  record_id                                           AS claim_id,
  dq_rule_violated,
  quarantined_at,
  get_json_object(raw_record, '$.policy_id')          AS policy_id,
  get_json_object(raw_record, '$.claim_type')         AS claim_type,
  get_json_object(raw_record, '$.claim_date')         AS claim_date,
  get_json_object(raw_record, '$.claim_amount_gbp')   AS claim_amount_gbp,
  get_json_object(raw_record, '$.severity_label')     AS severity_label,
  get_json_object(raw_record, '$.fraud_risk_score')   AS fraud_risk_score,
  get_json_object(raw_record, '$.is_hailstorm_related') AS is_hailstorm_related,
  get_json_object(raw_record, '$.incident_state')     AS incident_state
FROM silver_dev.refined_claims.quarantine
WHERE source_table = 'claims'
ORDER BY quarantined_at DESC;
