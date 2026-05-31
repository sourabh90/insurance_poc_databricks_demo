-- Records with multiple DQ violations in a single row
-- These indicate systemic upstream data issues, not one-off errors

SELECT
  source_table,
  record_id,
  dq_rule_violated,
  SIZE(SPLIT(dq_rule_violated, '\\|'))  AS issue_count,
  raw_record,
  quarantined_at
FROM silver_dev.refined_claims.quarantine
WHERE dq_rule_violated LIKE '%|%'
ORDER BY issue_count DESC, source_table;
