CREATE OR REPLACE VIEW customer_analytics.top_customers_ltv AS
SELECT *
FROM (
  SELECT
    customer_id,
    SUM(amount) AS lifetime_value,
    PERCENT_RANK() OVER (ORDER BY SUM(amount) DESC) AS pct_rank
  FROM customer_analytics.transactions
  GROUP BY customer_id
)
WHERE pct_rank <= 0.05;
