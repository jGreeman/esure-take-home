-- The 10 busiest days for renewals, with the policy types renewed.
SELECT
    d.date,
    d.date_day_name,
    COUNT(*)                                          AS renewals,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.product_type))) AS policy_types
FROM fact_policy_event f
JOIN dim_date    d ON d.date_key    = f.date_key
JOIN dim_product p ON p.product_key = f.product_key
WHERE f.event_type = 'renewal'
GROUP BY d.date, d.date_day_name
ORDER BY renewals DESC, d.date
LIMIT 10
