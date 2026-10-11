-- Detect unusual traffic spikes
WITH hourly_metrics AS (
    SELECT
        DATE_TRUNC('hour', ts) AS hour,
        COUNT(*) AS events
    FROM {{events_table}}
    WHERE ts >= DATE_ADD(CURRENT_DATE, INTERVAL '-14 days')

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
        AND {{ts_partition_filter:DATE_ADD(CURRENT_DATE, INTERVAL '-14 days')}}
    GROUP BY 1
),
hourly_baseline AS (
    SELECT
        hour,
        events,
        AVG(events) OVER (
            PARTITION BY EXTRACT(HOUR FROM hour)
            ORDER BY DATE_TRUNC('day', hour)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg,
        STDDEV(events) OVER (
            PARTITION BY EXTRACT(HOUR FROM hour)
            ORDER BY DATE_TRUNC('day', hour)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_stddev
    FROM hourly_metrics
)
SELECT
    hour,
    events,
    ROUND(baseline_avg, 2) AS baseline_avg,
    ROUND(baseline_stddev, 2) AS baseline_stddev,
    ROUND(
        100.0 * (events - baseline_avg) / NULLIF(baseline_avg, 0),
        2
    ) AS deviation_pct
FROM hourly_baseline
WHERE baseline_avg IS NOT NULL
    AND events > baseline_avg + (2 * baseline_stddev)
ORDER BY hour DESC
LIMIT 10;
