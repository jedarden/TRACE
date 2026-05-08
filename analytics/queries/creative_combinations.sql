-- Best headline + image combinations
SELECT
    params->>'tb_headline' AS headline,
    params->>'tb_image' AS image,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr
FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
WHERE params->>'tb_headline' IS NOT NULL
    AND params->>'tb_image' IS NOT NULL
    AND ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2
HAVING COUNT(*) FILTER (WHERE type = 'click') >= 10
ORDER BY clicks DESC
LIMIT 20;
