-- Analyze returning user behavior
-- Segments users by their session frequency and engagement
WITH user_sessions_summary AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) AS total_sessions,
        MIN(ts) AS first_session,
        MAX(ts) AS last_session,
        COUNT(*) AS total_events,
        COUNT(DISTINCT url) AS total_unique_pages,
        COUNT(DISTINCT DATE_TRUNC('day', ts)) AS active_days
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
    GROUP BY user_id
),
user_segments AS (
    SELECT
        user_id,
        total_sessions,
        CASE
            WHEN total_sessions = 1 THEN 'new'
            WHEN total_sessions = 2 THEN 'returning_2'
            WHEN total_sessions BETWEEN 3 AND 5 THEN 'returning_3_5'
            WHEN total_sessions BETWEEN 6 AND 10 THEN 'returning_6_10'
            ELSE 'returning_11_plus'
        END AS user_segment
    FROM user_sessions_summary
)
SELECT
    s.user_segment,
    COUNT(*) AS user_count,
    SUM(s.total_sessions) AS total_sessions,
    SUM(s.total_events) AS total_events,
    AVG(s.total_sessions) AS avg_sessions_per_user,
    AVG(s.active_days) AS avg_active_days,
    AVG(s.total_unique_pages) AS avg_unique_pages
FROM user_sessions_summary s
INNER JOIN user_segments g ON s.user_id = g.user_id
GROUP BY s.user_segment
ORDER BY
    CASE s.user_segment
        WHEN 'new' THEN 1
        WHEN 'returning_2' THEN 2
        WHEN 'returning_3_5' THEN 3
        WHEN 'returning_6_10' THEN 4
        ELSE 5
    END;
