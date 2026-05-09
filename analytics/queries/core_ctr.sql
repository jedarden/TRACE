-- ============================================================================
-- Core CTR Query: Click-Through Rate by Campaign/Ad/Creative
-- ============================================================================
-- Calculates CTR (clicks/pageviews) across multiple dimensions:
-- - By campaign: Overall campaign performance
-- - By creative: Headline and image combinations
-- - By network: Cross-network comparison
--
-- Usage:
--   Replace {{events_table}} with your events table name
--   Replace {{start_date}} and {{end_date}} with your date range
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CTR by Campaign (with network breakdown)
-- ----------------------------------------------------------------------------

-- Single campaign view with drill-down
SELECT
    network,
    campaign_id,
    campaign_name,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND campaign_id IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 10
ORDER BY clicks DESC
LIMIT 100;

-- ----------------------------------------------------------------------------
-- CTR by Creative (headline + image combinations)
-- ----------------------------------------------------------------------------

SELECT
    network,
    creative_id,
    headline,
    image_id,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    COUNT(DISTINCT campaign_id) AS num_campaigns,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND creative_id IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 20
ORDER BY ctr_pct DESC, clicks DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- CTR by Network (daily trend)
-- ----------------------------------------------------------------------------

SELECT
    DATE_TRUNC('day', ts) AS date,
    network,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT campaign_id) AS active_campaigns,
    COUNT(DISTINCT creative_id) AS active_creatives,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ----------------------------------------------------------------------------
-- CTR by Campaign x Creative Matrix (for creative fatigue analysis)
-- ----------------------------------------------------------------------------

SELECT
    campaign_id,
    creative_id,
    headline,
    DATE_TRUNC('day', ts) AS date,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    -- Calculate cumulative views per creative to detect fatigue
    SUM(COUNT(*) FILTER (WHERE type = 'pageview')) OVER (
        PARTITION BY campaign_id, creative_id
        ORDER BY DATE_TRUNC('day', ts)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_views
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND campaign_id IS NOT NULL
    AND creative_id IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 5
ORDER BY campaign_id, creative_id, date;
