-- Q1 (brief): How many customers currently have active policies?
--
-- Definition (assumption, see README):
--   * A policy's status is set by its most recent purchase, renewal or cancellation.
--     Claims don't change status, so they're ignored here.
--   * A policy is active if that latest event is a purchase or renewal.
--   * "Currently" = as of the latest event date in the data (no real "today" in a static dataset).
--   * The customer is taken from that latest status event (policy_ids can appear with several customers).
WITH status_events AS (
    SELECT
        f.policy_id,
        f.customer_key,
        f.event_type,
        ROW_NUMBER() OVER (PARTITION BY f.policy_id ORDER BY f.event_ts DESC, f.event_id) AS rn
    FROM fact_policy_event f
    WHERE f.event_type IN ('purchase', 'renewal', 'cancellation')
),
latest_status AS (
    SELECT * FROM status_events WHERE rn = 1
)
SELECT
    COUNT(DISTINCT c.customer_id) AS active_customers,
    COUNT(*)                      AS active_policies
FROM latest_status s
JOIN dim_customer c ON c.customer_key = s.customer_key
WHERE s.event_type IN ('purchase', 'renewal')
