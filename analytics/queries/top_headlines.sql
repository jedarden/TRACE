-- Performance by Taboola headline
SELECT
    params->>'tb_headline' AS headline,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM {{events_table}}
WHERE params->>'tb_headline' IS NOT NULL
    AND ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 50;
