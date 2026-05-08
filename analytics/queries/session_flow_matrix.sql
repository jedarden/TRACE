-- Session flow transition matrix for visualization
-- Shows how users navigate between pages
WITH page_transitions AS (
    SELECT
        session_id,
        url,
        LEAD(url) OVER (PARTITION BY session_id ORDER BY ts) AS next_url,
        type,
        LEAD(type) OVER (PARTITION BY session_id ORDER BY ts) AS next_type
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
        AND type = 'pageview'
)
SELECT
    url AS from_page,
    next_url AS to_page,
    COUNT(*) AS transition_count,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM page_transitions
WHERE next_url IS NOT NULL
GROUP BY from_page, to_page
ORDER BY transition_count DESC
LIMIT 100;
