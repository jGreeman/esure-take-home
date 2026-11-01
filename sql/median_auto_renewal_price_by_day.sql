-- Q3 (brief): What is the median renewal price in auto, by day?
-- Renewal price = premium_amount on the renewal event. Rows with no premium are ignored by MEDIAN.
-- Most days have a single auto renewal, so the median is often just that one price.
SELECT
    d.date                        AS renewal_date,
    COUNT(*)                      AS auto_renewals,
    ROUND(MEDIAN(f.premium_amount), 2) AS median_renewal_price
FROM fact_policy_event f
JOIN dim_product p ON p.product_key = f.product_key
JOIN dim_date    d ON d.date_key    = f.date_key
WHERE f.event_type = 'renewal'
  AND p.product_type = 'auto'
GROUP BY d.date
ORDER BY d.date
