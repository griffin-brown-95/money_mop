-- Example 1: Daily Cashback by Group
SELECT 
  category, 
  experiment_group, 
  sum(transactions) as count_transactions,
  SUM(total_cashback) AS total_cashback,
  SUM(total_spend) AS total_spend,
  ROUND(SUM(total_cashback) / SUM(total_spend), 4) AS effective_cashback_pct
FROM 
  money_mop.gold_experiment_metrics
WHERE
  experiment_name is not null
GROUP BY 
  category, 
  experiment_group
ORDER BY 
  category, 
  experiment_group;