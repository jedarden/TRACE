-- Detect declining creative performance
WITH creative_daily AS (
    SELECT
        DATE(ts) AS date,
        params->>'tb_headline' AS headline,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views
    FROM {{events_table}}
    WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
        AND params->>'tb_headline' IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 100
),
daily_ctr AS (
    SELECT
        date,
        headline,
        clicks,
        views,
        ROUND(100.0 * clicks / NULLIF(views, 0), 2) AS ctr
    FROM creative_daily
),
fatigue AS (
    SELECT
        headline,
        AVG(ctr) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS recent_ctr,
        AVG(ctr) FILTER (WHERE date < CURRENT_DATE - INTERVAL '7 days'
                          AND date >= CURRENT_DATE - INTERVAL '21 days') AS prior_ctr
    FROM daily_ctr
    GROUP BY 1
)
SELECT
    headline,
    ROUND(recent_ctr, 2) AS recent_ctr,
    ROUND(prior_ctr, 2) AS prior_ctr,
    ROUND(
        100.0 * (recent_ctr - prior_ctr) / NULLIF(prior_ctr, 0),
        2
    ) AS fatigue_change_pct
FROM fatigue
WHERE prior_ctr > 0
ORDER BY fatigue_change_pct ASC
LIMIT 20;
