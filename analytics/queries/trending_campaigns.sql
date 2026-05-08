-- Campaigns with increasing momentum
WITH daily_metrics AS (
    SELECT
        DATE(ts) AS date,
        params->>'utm_campaign' AS campaign,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1, 2
),
trends AS (
    SELECT
        campaign,
        AVG(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS recent_clicks,
        AVG(clicks) FILTER (WHERE date < CURRENT_DATE - INTERVAL '7 days') AS prior_clicks
    FROM daily_metrics
    GROUP BY 1
)
SELECT
    campaign,
    ROUND(recent_clicks, 2) AS recent_clicks,
    ROUND(prior_clicks, 2) AS prior_clicks,
    ROUND(
        100.0 * (recent_clicks - prior_clicks) / NULLIF(prior_clicks, 0),
        2
    ) AS momentum_pct
FROM trends
WHERE prior_clicks > 10
ORDER BY momentum_pct DESC
LIMIT 20;
