-- Daily DQ failure trend
-- Use to monitor whether data quality is improving or degrading over time

SELECT
  DATE(quarantined_at)                  AS run_date,
  source_table,
  COUNT(*)                              AS failed_records,
  COUNT(DISTINCT dq_rule_violated)      AS distinct_rules_violated,
  SUM(CASE WHEN dq_rule_violated RLIKE 'invalid.*_id|missing.*_fk|non_positive_claim_amount'
           THEN 1 ELSE 0 END)          AS hard_blocks,
  SUM(CASE WHEN dq_rule_violated NOT RLIKE 'invalid.*_id|missing.*_fk|non_positive_claim_amount'
           THEN 1 ELSE 0 END)          AS soft_warnings
FROM silver_dev.refined_claims.quarantine
GROUP BY 1, 2
ORDER BY run_date DESC, source_table;
