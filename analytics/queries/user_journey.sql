-- Reconstruct complete user journey across all sessions
-- Shows the chronological sequence of sessions for a specific user
-- Parameter: user_id (optional filter)
WITH user_events AS (
    SELECT
        *,
        LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts,
        EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts))) / 60 AS gap_minutes
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
),
session_markers AS (
    SELECT
        *,
        SUM(CASE WHEN gap_minutes > 30 OR gap_minutes IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY ts) AS session_seq
    FROM user_events
),
sessions AS (
    SELECT
        user_id,
        session_seq,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT url) AS unique_pages,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT AS duration_seconds,
        FIRST_VALUE(url) OVER (PARTITION BY user_id, session_seq ORDER BY ts) AS landing_page,
        FIRST_VALUE(network) OVER (PARTITION BY user_id, session_seq ORDER BY ts) AS source_network,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY user_id, session_seq ORDER BY ts) AS campaign,
        ARRAY_AGG(DISTINCT type ORDER BY type) AS event_types
    FROM session_markers
    GROUP BY user_id, session_seq
)
SELECT
    user_id,
    session_seq,
    session_start,
    session_end,
    event_count,
    unique_pages,
    duration_seconds,
    landing_page,
    source_network,
    campaign,
    event_types
FROM sessions
ORDER BY user_id, session_start
LIMIT 500;
