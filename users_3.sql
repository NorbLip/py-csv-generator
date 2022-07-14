WITH
cohort AS (
    SELECT
        account_id
    FROM users_2
    WHERE revenue > 0
    GROUP BY account_id
    HAVING MIN(date_key) = '2019-02-01'
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
        CASE WHEN conseq_inactive >= 6 THEN 1 ELSE 0 END as is_6m_churned
    FROM fact_4
),
fact_6 AS (
    SELECT
        account_id as cc,
        date_key as dk,
        conseq_inactive as ci,
        is_6m_churned as m6,
        LAG(is_6m_churned) OVER (PARTITION BY account_id ORDER BY date_key) as p_m6
    FROM fact_5
    GROUP BY 1,2,3,4
),
fact_7 AS (
    SELECT
        cc,
        dk,
        ci,
        m6,
        p_m6,
        SUM(CASE WHEN ci = 0 AND p_m6 != 0 THEN 1 ELSE 0 END) as rt
    FROM fact_6
    GROUP BY 1,2,3,4,5
)

SELECT
    dk,
    SUM(m6),
    SUM(rt),
    CAST((SUM(p_m6) - SUM(rt)) AS INT) + 0 as still_m6,
    SUM(CASE WHEN ci = 0 THEN 1 ELSE 0 END) as act,
    SUM(CASE WHEN ci != 0 THEN 1 ELSE 0 END) as inact
FROM fact_7
GROUP BY 1
