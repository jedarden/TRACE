-- Common page sequences within sessions
WITH session_events AS (
    SELECT
        session_id,
        type,
        url,
        ts,
        LAG(type) OVER (PARTITION BY session_id ORDER BY ts) AS prev_type,
        LAG(url) OVER (PARTITION BY session_id ORDER BY ts) AS prev_url
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
        AND {{ts_partition_filter}}
        AND session_id IS NOT NULL
)
SELECT
    prev_type,
    type,
    COUNT(*) AS flow_count,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM session_events
WHERE prev_type IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;
