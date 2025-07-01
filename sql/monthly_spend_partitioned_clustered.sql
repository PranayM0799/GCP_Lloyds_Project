CREATE OR REPLACE TABLE `lloyds-banking-thp.customer_analytics.monthly_spend_partitioned_clustered`
(
  customer_id STRING,
  month STRING,
  transaction_date_for_partition DATE,
  transaction_count INTEGER,
  total_spend BIGNUMERIC,
  average_spend BIGNUMERIC
)
PARTITION BY transaction_date_for_partition
CLUSTER BY customer_id, month;