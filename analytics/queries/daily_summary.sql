-- Daily event summary by type and source
SELECT
    DATE(ts) AS date,
    params->>'utm_source' AS source,
    type,
    COUNT(*) AS events,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;
