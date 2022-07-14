WITH
cohort AS (
    SELECT
        account_id
    FROM users_2
    WHERE revenue > 0
    GROUP BY account_id
    HAVING MIN(date_key) == '2019-02-01'
),
fact_1 AS (
    SELECT
        u.account_id,
        u.date_key,
        u.revenue,
        CASE WHEN u.revenue > 0 THEN 0 ELSE 1 END as is_inactive
    FROM cohort c
        LEFT JOIN users_2 u ON c.account_id = u.account_id
    WHERE u.date_key >= '2019-02-01'
),
fact_2 AS (
    SELECT
        *,
        LAG(is_inactive, 1) OVER (PARTITION BY account_id ORDER BY date_key) as is_inactive_prev,
        CASE WHEN LAG(is_inactive, 1) OVER (PARTITION BY account_id ORDER BY date_key) = is_inactive THEN 0 ELSE 1 END as change
    FROM fact_1
),
fact_3 AS (
    SELECT
        *,
        SUM(change) OVER (PARTITION BY account_id ORDER BY date_key) as act_partition
    FROM fact_2
),
fact_4 AS (
    SELECT
        *,
        SUM(is_inactive) OVER (PARTITION BY account_id, act_partition ORDER BY date_key) as conseq_inactive
    FROM fact_3
    ORDER BY account_id, date_key
),
fact_5 AS (
    SELECT
        *,
        CASE WHEN conseq_inactive >= 3 THEN 1 ELSE 0 END as is_3m_churned
    FROM fact_4
),
fact_6 AS (
    SELECT
        *,
        CASE WHEN conseq_inactive >= 6 THEN 1 ELSE 0 END as is_6m_churned
    FROM fact_4
),
fact_7 AS (
    SELECT
        *,
        CASE WHEN conseq_inactive >= 12 THEN 1 ELSE 0 END as is_12m_churned
    FROM fact_4
),
fact_8 AS (
    SELECT
        date_key,
        SUM(is_3m_churned) AS churned_3m,
        COUNT(*) - SUM(is_3m_churned) AS not_churned_3m,
        COUNT(*) AS total_3m
    FROM fact_5
    GROUP BY date_key
    ORDER BY date_key
),
fact_9 AS (
    SELECT
        date_key,
        SUM(is_6m_churned) AS churned_6m,
        COUNT(*) - SUM(is_6m_churned) AS not_churned_6m,
        COUNT(*) AS total_6m
    FROM fact_6
    GROUP BY date_key
    ORDER BY date_key
),
fact_10 AS (
    SELECT
        date_key,
        SUM(is_12m_churned) AS churned_12m,
        COUNT(*) - SUM(is_12m_churned) AS not_churned_12m,
        COUNT(*) AS total_12m
    FROM fact_7
    GROUP BY date_key
    ORDER BY date_key
)

SELECT
    a.date_key,
    churned_3m,
    not_churned_3m
FROM fact_8 a
    JOIN fact_9 b ON a.date_key = b.date_key
    JOIN fact_10 c ON a.date_key = c.date_key