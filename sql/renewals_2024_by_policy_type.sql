-- Q2 (brief): How many renewals were sent in 2024, split by policy type?
-- Assumption: a 'renewal' event = a renewal sent. 'unknown' = policy type missing in the source.
SELECT
    p.product_type     AS policy_type,
    COUNT(*)           AS renewals
FROM fact_policy_event f
JOIN dim_product p ON p.product_key = f.product_key
JOIN dim_date    d ON d.date_key    = f.date_key
WHERE f.event_type = 'renewal'
  AND d.date_year = 2024
GROUP BY p.product_type
ORDER BY renewals DESC
