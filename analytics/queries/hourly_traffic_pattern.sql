-- Traffic by hour of day
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    type,
    COUNT(*) AS events
FROM {{events_table}}
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1, 2;
