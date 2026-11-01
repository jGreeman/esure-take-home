-- Average coverage by age band. Events with an unknown (nulled) age are shown separately.
SELECT
    CASE
        WHEN age_of_insured IS NULL THEN 'unknown'
        WHEN age_of_insured < 18    THEN 'under 18'
        WHEN age_of_insured < 30    THEN '18-29'
        WHEN age_of_insured < 45    THEN '30-44'
        WHEN age_of_insured < 60    THEN '45-59'
        ELSE '60+'
    END                                AS age_band,
    COUNT(*)                           AS events,
    ROUND(AVG(coverage_amount), 2)     AS avg_coverage
FROM fact_policy_event
GROUP BY 1
ORDER BY MIN(COALESCE(age_of_insured, 999))
