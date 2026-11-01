-- Cancellations by region and quarter.
SELECT
    r.region,
    d.date_year,
    d.date_quarter,
    COUNT(*) AS cancellations
FROM fact_policy_event f
JOIN dim_region r ON r.region_key = f.region_key
JOIN dim_date   d ON d.date_key   = f.date_key
WHERE f.event_type = 'cancellation'
GROUP BY r.region, d.date_year, d.date_quarter
ORDER BY r.region, d.date_year, d.date_quarter
