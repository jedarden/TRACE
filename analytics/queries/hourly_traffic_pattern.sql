-- Traffic by hour of day
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    type,
    COUNT(*) AS events
FROM {{events_table}}
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
    AND {{ts_partition_filter:CURRENT_DATE - INTERVAL '7 days'}}
GROUP BY 1, 2
ORDER BY 1, 2;
