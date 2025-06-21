CREATE OR REPLACE VIEW customer_analytics.monthly_spend AS
SELECT
  customer_id,
  FORMAT_TIMESTAMP('%Y-%m', timestamp) AS month,
  COUNT(*) AS transaction_count,
  SUM(amount) AS total_spend,
  AVG(amount) AS average_spend
FROM customer_analytics.transactions
GROUP BY customer_id, month;
