-- Performance by creative image
SELECT
    params->>'tb_image' AS image_id,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    COUNT(DISTINCT params->>'tb_headline') AS num_headlines
FROM {{events_table}}
WHERE params->>'tb_image' IS NOT NULL
    AND ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 50;
