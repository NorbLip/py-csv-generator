WITH calendar AS (
    SELECT
        date_key
    FROM users
    WHERE date_key >= '2019-01-01'
    GROUP BY date_key
    ORDER BY date_key
),
distinct_accounts AS (
    SELECT
        account_id
    FROM users
    WHERE date_key >= '2019-01-01'
        AND revenue IS NOT NULL
        AND revenue > 0
    GROUP BY account_id
    ORDER BY account_id
),
distinct_sources AS (
    SELECT
        source
    FROM users
    WHERE source = 'display_ads'
    GROUP BY source
    ORDER BY source
),
fact AS (
    SELECT
        date_key,
        account_id,
        revenue,
        source,
        date_key || account_id || source AS join_key
    FROM users
    WHERE source = 'display_ads'
        AND date_key >= '2019-01-01'
        AND revenue IS NOT NULL
        AND revenue > 0
),
crossed_joined AS (
    SELECT
        date_key,
        account_id,
        source,
        date_key || account_id || source AS join_key
    FROM calendar
    CROSS JOIN distinct_accounts
    CROSS JOIN distinct_sources
    WHERE join_key NOT IN (
        SELECT join_key FROM fact
    )
),
unioned AS (
    SELECT
        date_key,
        account_id,
        revenue,
        source
    FROM fact
    UNION
    SELECT
        date_key,
        account_id,
        0 AS revenue,
        source
    FROM crossed_joined
)

SELECT * FROM unioned ORDER BY account_id
