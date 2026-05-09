-- Reconstruct sessions from events using gap-based sessionization
-- Sessions are reconstructed by detecting gaps > 30 minutes between events
WITH event_gaps AS (
    SELECT
        *,
        LAG(ts) OVER (PARTITION BY session_id ORDER BY ts) AS prev_ts,
        EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (PARTITION BY session_id ORDER BY ts))) / 60 AS gap_minutes
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
),
session_assignments AS (
    SELECT
        *,
        SUM(CASE WHEN gap_minutes > 30 OR gap_minutes IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY session_id ORDER BY ts) AS reconstructed_session_seq
    FROM event_gaps
),
sessions AS (
    SELECT
        session_id || '_' || reconstructed_session_seq::TEXT AS reconstructed_session_id,
        session_id,
        user_id,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT url) AS unique_pages,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT AS duration_seconds,
        FIRST_VALUE(url) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS landing_page,
        FIRST_VALUE(network) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS source_network,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS campaign,
        FIRST_VALUE(device_type) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS device_type
    FROM session_assignments
    WHERE EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT / 3600 <= 4
    GROUP BY session_id, reconstructed_session_seq, user_id
)
SELECT
    reconstructed_session_id AS session_id,
    user_id,
    session_start,
    session_end,
    event_count,
    unique_pages,
    duration_seconds,
    landing_page,
    (CASE WHEN event_count = 1 THEN true ELSE false END) AS is_bounce,
    source_network,
    campaign,
    device_type
FROM sessions
ORDER BY session_start DESC
LIMIT 1000;
