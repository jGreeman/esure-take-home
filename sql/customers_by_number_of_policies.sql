-- How many customers hold one, two, three... distinct policies.
WITH policies_per_customer AS (
    SELECT c.customer_id, COUNT(DISTINCT f.policy_id) AS n_policies
    FROM fact_policy_event f
    JOIN dim_customer c ON c.customer_key = f.customer_key
    GROUP BY c.customer_id
)
SELECT n_policies, COUNT(*) AS customers
FROM policies_per_customer
GROUP BY n_policies
ORDER BY n_policies
