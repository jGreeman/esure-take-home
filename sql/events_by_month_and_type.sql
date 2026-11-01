-- Monthly trend of each event type.
SELECT
    d.date_year,
    d.date_month,
    d.date_month_name,
    f.event_type,
    COUNT(*) AS events
FROM fact_policy_event f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.date_year, d.date_month, d.date_month_name, f.event_type
ORDER BY d.date_year, d.date_month, f.event_type
