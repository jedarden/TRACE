-- Traffic by hour of day
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    type,
    COUNT(*) AS events
FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1, 2;
