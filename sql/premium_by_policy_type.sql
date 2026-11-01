-- Premium statistics for new business (purchases), by policy type.
SELECT
    p.product_type                    AS policy_type,
    COUNT(*)                          AS purchases,
    ROUND(AVG(f.premium_amount), 2)   AS avg_premium,
    ROUND(MEDIAN(f.premium_amount), 2) AS median_premium,
    MIN(f.premium_amount)             AS min_premium,
    MAX(f.premium_amount)             AS max_premium
FROM fact_policy_event f
JOIN dim_product p ON p.product_key = f.product_key
WHERE f.event_type = 'purchase'
GROUP BY p.product_type
ORDER BY avg_premium DESC
