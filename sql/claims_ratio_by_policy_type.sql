-- Claims relative to policies written, by policy type.
SELECT
    p.product_type                                                  AS policy_type,
    SUM(CASE WHEN f.event_type IN ('purchase', 'renewal') THEN 1 ELSE 0 END) AS policies_written,
    SUM(CASE WHEN f.event_type = 'claim' THEN 1 ELSE 0 END)         AS claims,
    ROUND(SUM(CASE WHEN f.event_type = 'claim' THEN 1 ELSE 0 END)
          / SUM(CASE WHEN f.event_type IN ('purchase', 'renewal') THEN 1 ELSE 0 END), 3) AS claims_per_policy_written
FROM fact_policy_event f
JOIN dim_product p ON p.product_key = f.product_key
GROUP BY p.product_type
ORDER BY claims_per_policy_written DESC
