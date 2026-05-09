-- Detect unusual traffic spikes
WITH hourly_baseline AS (
    SELECT
        DATE_TRUNC('hour', ts) AS hour,
        COUNT(*) AS events,
        AVG(COUNT(*)) OVER (
            PARTITION BY EXTRACT(HOUR FROM ts)
            ORDER BY DATE_TRUNC('day', ts)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg,
        STDDEV(COUNT(*)) OVER (
            PARTITION BY EXTRACT(HOUR FROM ts)
            ORDER BY DATE_TRUNC('day', ts)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_stddev
    FROM {{events_table}}
    WHERE ts >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1
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
