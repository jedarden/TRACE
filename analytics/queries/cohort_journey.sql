-- User journey by acquisition cohort (first touch network)
-- Analyzes how different acquisition sources behave over time
WITH user_cohorts AS (
    SELECT
        user_id,
        FIRST_VALUE(network) OVER (PARTITION BY user_id ORDER BY ts) AS acquisition_network,
        MIN(ts) OVER (PARTITION BY user_id) AS first_touch_ts
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
),
user_sessions AS (
    SELECT
        e.user_id,
        c.acquisition_network,
        c.first_touch_ts,
        e.session_id,
        MIN(e.ts) AS session_start,
        COUNT(*) AS events,
        COUNT(DISTINCT e.url) AS unique_pages,
        EXTRACT(DAY FROM (MIN(e.ts) - c.first_touch_ts)) AS day_number
    FROM {{events_table}} e
    INNER JOIN user_cohorts c ON e.user_id = c.user_id
    WHERE e.ts >= '{{start_date}}'::TIMESTAMP
        AND e.ts < '{{end_date}}'::TIMESTAMP
    GROUP BY e.user_id, c.acquisition_network, c.first_touch_ts, e.session_id
)
SELECT
    acquisition_network AS cohort,
    day_number,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS sessions,
    AVG(events) AS avg_events_per_session,
    AVG(unique_pages) AS avg_pages_per_session
FROM user_sessions
GROUP BY cohort, day_number
ORDER BY cohort, day_number;
