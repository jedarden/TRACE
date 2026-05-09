-- Most common user paths through the site
-- Identifies frequent navigation patterns
WITH session_paths AS (
    SELECT
        session_id,
        ARRAY_AGG(url ORDER BY ts) AS path,
        COUNT(*) AS steps,
        MIN(ts) AS session_start
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
        AND type = 'pageview'
    GROUP BY session_id
    HAVING COUNT(*) <= 10  -- Limit to reasonable path lengths
),
path_frequencies AS (
    SELECT
        path,
        steps,
        COUNT(*) AS frequency
    FROM session_paths
    GROUP BY path, steps
)
SELECT
    path,
    steps,
    frequency,
    ROUND(100.0 * frequency / SUM(frequency) OVER (), 2) AS percentage
FROM path_frequencies
ORDER BY frequency DESC
LIMIT 50;
