-- Top landing pages and bounce rate
WITH sessions AS (
    SELECT
        session_id,
        MIN(url) AS landing_url,
        COUNT(*) AS events,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY 1
)
SELECT
    landing_url,
    COUNT(*) AS sessions,
    SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) AS bounced,
    ROUND(
        100.0 * SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0),
        2
    ) AS bounce_rate_pct
FROM sessions
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
