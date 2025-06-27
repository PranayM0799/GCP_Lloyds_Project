CREATE OR REPLACE TABLE
    `lloyds-banking-thp.customer_analytics.top_customers_ltv_clustered` -- Give it a new name to distinguish
CLUSTER BY
    customer_id -- Cluster by customer_id for efficient lookups and joins
AS
SELECT
    t.customer_id,
    SUM(t.total_spend) AS lifetime_value,
    -- Calculate percentage rank dynamically
    ROW_NUMBER() OVER (ORDER BY SUM(t.total_spend) DESC) AS rank_by_ltv, -- Renamed to avoid confusion with 'rank' keyword
    (ROW_NUMBER() OVER (ORDER BY SUM(t.total_spend) DESC) * 1.0 / COUNT(DISTINCT t.customer_id) OVER ()) * 100 AS pct_rank
FROM
    `lloyds-banking-thp.customer_analytics.monthly_spend_partitioned_clustered` t -- Use your newly created monthly_spend table
GROUP BY
    t.customer_id
-- Filter for the top 5% based on their LTV
QUALIFY (ROW_NUMBER() OVER (ORDER BY SUM(t.total_spend) DESC) * 1.0 / COUNT(DISTINCT t.customer_id) OVER ()) <= 0.05;