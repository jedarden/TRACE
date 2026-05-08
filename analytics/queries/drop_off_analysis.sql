-- Analyze where users drop off in their journey
-- Identifies the last action users take before leaving
WITH session_sequences AS (
    SELECT
        session_id,
        user_id,
        ARRAY_AGG(type ORDER BY ts) AS event_sequence,
        ARRAY_AGG(url ORDER BY ts) AS url_sequence,
        COUNT(*) AS total_events
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id, user_id
),
last_events AS (
    SELECT
        session_id,
        user_id,
        event_sequence,
        url_sequence,
        total_events,
        event_sequence[ARRAY_LENGTH(event_sequence, 1)] AS last_event_type,
        url_sequence[ARRAY_LENGTH(url_sequence, 1)] AS last_url,
        url_sequence[ARRAY_LENGTH(url_sequence, 1) - 1] AS second_to_last_url
    FROM session_sequences
    WHERE ARRAY_LENGTH(event_sequence, 1) > 0
)
SELECT
    last_event_type,
    last_url,
    second_to_last_url,
    COUNT(*) AS sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(total_events) AS avg_events_before_dropoff
FROM last_events
GROUP BY last_event_type, last_url, second_to_last_url
ORDER BY sessions DESC
LIMIT 50;
