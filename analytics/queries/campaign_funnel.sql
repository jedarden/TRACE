-- Conversion funnel by campaign
WITH funnel AS (
    SELECT
        params->>'utm_campaign' AS campaign,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        COUNT(*) FILTER (WHERE type = 'dwell') AS dwells
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
    GROUP BY 1
)
SELECT
    campaign,
    pageviews,
    clicks,
    ROUND(100.0 * clicks / NULLIF(pageviews, 0), 2) AS click_through_pct,
    scrolls,
    ROUND(100.0 * scrolls / NULLIF(clicks, 0), 2) AS scroll_after_click_pct,
    dwells,
    ROUND(100.0 * dwells / NULLIF(scrolls, 0), 2) AS dwell_after_scroll_pct
FROM funnel
ORDER BY pageviews DESC
LIMIT 20;
