-- Total written premium (purchases + renewals) by brand and region.
SELECT
    p.brand,
    r.region,
    COUNT(*)              AS policies_written,
    SUM(f.premium_amount) AS total_premium
FROM fact_policy_event f
JOIN dim_product p ON p.product_key = f.product_key
JOIN dim_region  r ON r.region_key  = f.region_key
WHERE f.event_type IN ('purchase', 'renewal')
GROUP BY p.brand, r.region
ORDER BY p.brand, total_premium DESC
