-- Impression performance: volume, unique impressions, CTR measured
-- against impressions (not pageviews), and viewability by campaign.
--
-- unique_impressions counts DISTINCT params->>'imp_id': the flusher
-- collapses duplicates within one raw log file, but duplicates that
-- straddle an hour boundary (or arrive via a path without an imp_id)
-- only a DISTINCT can collapse. TRY_CAST guards the in_view_ms math —
-- the flusher drops non-integer values on the raw path, and TRY_CAST
-- keeps the report safe on any legacy rows that bypassed it.
-- Viewability uses the MRC-style 1s-continuous-in-view threshold.
SELECT
    params->>'utm_campaign' AS campaign,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'impression') AS impressions,
    COUNT(DISTINCT params->>'imp_id') FILTER (WHERE type = 'impression') AS unique_impressions,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(DISTINCT params->>'imp_id') FILTER (WHERE type = 'impression'), 0),
        2
    ) AS ctr_pct,
    COUNT(*) FILTER (
        WHERE type = 'impression'
        AND TRY_CAST(params->>'in_view_ms' AS BIGINT) >= 1000
    ) AS viewable_impressions,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE type = 'impression'
            AND TRY_CAST(params->>'in_view_ms' AS BIGINT) >= 1000
        ) / NULLIF(COUNT(*) FILTER (
            WHERE type = 'impression'
            AND (params->>'in_view_ms') IS NOT NULL
        ), 0),
        2
    ) AS viewability_pct,
    ROUND(
        AVG(TRY_CAST(params->>'in_view_ms' AS BIGINT)) FILTER (WHERE type = 'impression'),
        0
    ) AS avg_in_view_ms
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND {{ts_partition_filter}}
GROUP BY 1, 2
HAVING COUNT(*) FILTER (WHERE type = 'impression') > 0
ORDER BY unique_impressions DESC
LIMIT 50;
