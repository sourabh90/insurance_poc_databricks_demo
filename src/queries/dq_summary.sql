-- DQ Summary: failures by source table and rule
-- Run after every Bronze → Silver pipeline execution
-- Change catalog/schema prefix for prod: silver_prod.refined_claims

SELECT
  source_table,
  dq_rule_violated,
  COUNT(*)                                      AS failed_records,
  MIN(quarantined_at)                           AS first_seen,
  MAX(quarantined_at)                           AS last_seen,
  COUNT(DISTINCT DATE(quarantined_at))          AS distinct_run_days
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2
ORDER BY failed_records DESC;
