-- How many events carry each data-quality flag (a row can have several).
SELECT
    flag,
    COUNT(*)                                                        AS events,
    ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM fact_policy_event), 1) AS pct_of_events
FROM fact_policy_event
LATERAL VIEW EXPLODE(dq_flags) flags AS flag
GROUP BY flag
ORDER BY events DESC
