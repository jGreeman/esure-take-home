-- Customers whose most recent policy status event is a cancellation (churned), by region.
-- Uses the same status logic as 01_active_customers.sql.
WITH status_events AS (
    SELECT
        f.policy_id, f.customer_key, f.region_key, f.event_type,
        ROW_NUMBER() OVER (PARTITION BY f.policy_id ORDER BY f.event_ts DESC, f.event_id) AS rn
    FROM fact_policy_event f
    WHERE f.event_type IN ('purchase', 'renewal', 'cancellation')
)
SELECT
    r.region,
    COUNT(DISTINCT s.customer_key) AS churned_customers
FROM status_events s
JOIN dim_region r ON r.region_key = s.region_key
WHERE s.rn = 1 AND s.event_type = 'cancellation'
GROUP BY r.region
ORDER BY churned_customers DESC
