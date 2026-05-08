-- Compare performance across ad networks
SELECT
    params->>'utm_source' AS network,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    COUNT(DISTINCT params->>'tb_headline') AS num_headlines
FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1
ORDER BY clicks DESC;
