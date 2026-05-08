-- Cross-Network Normalization Views for TRACE Analytics
--
-- This file contains SQL views that normalize campaign data across different
-- ad networks (Taboola, Outbrain, MGID, RevContent) into a common schema.
--
-- Usage: Load these views in your DuckDB/Trino session before running queries.

-- ============================================================================
-- DuckDB Views
-- ============================================================================

-- Install required extensions
-- INSTALL httpfs;
-- LOAD httpfs;

-- ============================================================================
-- Normalized Campaigns View
-- ============================================================================
-- This view extracts normalized campaign fields from the params JSON.
-- Use this for unified analysis across all ad networks.

CREATE OR REPLACE VIEW normalized_campaigns AS
WITH network_detection AS (
    SELECT
        *,
        -- Detect network from utm_source or parameter presence
        COALESCE(
            params->>'utm_source',
            CASE
                WHEN params ? 'tb_image' OR params ? 'tb_headline' THEN 'taboola'
                WHEN params ? 'ob_creative' OR params ? 'ob_item' THEN 'outbrain'
                WHEN params ? 'mg_id' OR params ? 'mg_title' THEN 'mgid'
                WHEN params ? 'rc_id' OR params ? 'rc_title' THEN 'revcontent'
                ELSE 'unknown'
            END
        ) AS detected_network
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
)
SELECT
    ts,
    ip,
    ua,
    url,
    type,
    params,
    -- Network detection
    detected_network AS network,
    -- Campaign identifiers
    params->>'utm_campaign' AS campaign_id,
    -- Normalized creative fields
    CASE detected_network
        WHEN 'taboola' THEN params->>'tb_image'
        WHEN 'outbrain' THEN params->>'ob_creative'
        WHEN 'mgid' THEN params->>'mg_id'
        WHEN 'revcontent' THEN params->>'rc_id'
        ELSE NULL
    END AS creative_id,
    -- Normalized headline/title
    CASE detected_network
        WHEN 'taboola' THEN params->>'tb_headline'
        WHEN 'mgid' THEN params->>'mg_title'
        WHEN 'revcontent' THEN params->>'rc_title'
        ELSE NULL
    END AS headline,
    -- Normalized image ID
    CASE detected_network
        WHEN 'taboola' THEN params->>'tb_image'
        WHEN 'mgid' THEN params->>'mg_image'
        WHEN 'revcontent' THEN params->>'rc_thumb'
        ELSE NULL
    END AS image_id,
    -- Item identifiers
    CASE detected_network
        WHEN 'taboola' THEN params->>'tb_item'
        WHEN 'outbrain' THEN params->>'ob_item'
        WHEN 'mgid' THEN params->>'mg_id'
        WHEN 'revcontent' THEN params->>'rc_id'
        ELSE params->>'item'
    END AS item_id
FROM network_detection;

-- ============================================================================
-- Network Performance Summary
-- ============================================================================
-- Compare performance across all ad networks

CREATE OR REPLACE VIEW network_performance AS
SELECT
    network,
    DATE_TRUNC('day', ts) AS date,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    COUNT(DISTINCT campaign_id) AS active_campaigns,
    COUNT(DISTINCT creative_id) AS unique_creatives
FROM normalized_campaigns
WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY 1, 2;

-- ============================================================================
-- Top Performing Creatives (Cross-Network)
-- ============================================================================
-- Find best-performing headlines and creatives across all networks

CREATE OR REPLACE VIEW top_creatives AS
SELECT
    network,
    headline,
    creative_id,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    COUNT(DISTINCT campaign_id) AS campaigns_used,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM normalized_campaigns
WHERE headline IS NOT NULL
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2, 3
HAVING COUNT(*) FILTER (WHERE type = 'click') >= 10
ORDER BY clicks DESC
LIMIT 100;

-- ============================================================================
-- Creative Fatigue Detection
-- ============================================================================
-- Detect declining creative performance across networks

CREATE OR REPLACE VIEW creative_fatigue AS
WITH creative_daily AS (
    SELECT
        network,
        creative_id,
        headline,
        DATE(ts) AS date,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views
    FROM normalized_campaigns
    WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
        AND creative_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 100
),
daily_ctr AS (
    SELECT
        network,
        creative_id,
        headline,
        date,
        clicks,
        views,
        ROUND(100.0 * clicks / NULLIF(views, 0), 2) AS ctr
    FROM creative_daily
),
fatigue_metrics AS (
    SELECT
        network,
        creative_id,
        headline,
        AVG(ctr) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS recent_ctr,
        AVG(ctr) FILTER (
            WHERE date < CURRENT_DATE - INTERVAL '7 days'
            AND date >= CURRENT_DATE - INTERVAL '21 days'
        ) AS prior_ctr
    FROM daily_ctr
    GROUP BY 1, 2, 3
)
SELECT
    network,
    creative_id,
    headline,
    recent_ctr,
    prior_ctr,
    ROUND(
        100.0 * (recent_ctr - prior_ctr) / NULLIF(prior_ctr, 0),
        2
    ) AS fatigue_change_pct
FROM fatigue_metrics
WHERE prior_ctr > 0
ORDER BY fatigue_change_pct ASC
LIMIT 50;

-- ============================================================================
-- Same Creative Across Networks
-- ============================================================================
-- Find creatives running on multiple networks (for arbitrage analysis)

CREATE OR REPLACE VIEW cross_network_creatives AS
WITH creative_fingerprints AS (
    SELECT
        -- Create a normalized fingerprint for matching
        LOWER(
            REGEXP_REPLACE(
                COALESCE(headline, ''),
                '[^a-z0-9\s]',
                ''
            )
        ) AS normalized_headline,
        network,
        creative_id,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views
    FROM normalized_campaigns
    WHERE headline IS NOT NULL
        AND ts >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1, 2, 3
)
SELECT
    normalized_headline,
    COUNT(DISTINCT network) AS num_networks,
    array_agg(DISTINCT network) AS networks,
    SUM(clicks) AS total_clicks,
    SUM(views) AS total_views,
    ROUND(
        100.0 * SUM(clicks) / NULLIF(SUM(views), 0),
        2
    ) AS overall_ctr
FROM creative_fingerprints
GROUP BY 1
HAVING COUNT(DISTINCT network) > 1
    AND SUM(clicks) >= 20
ORDER BY total_clicks DESC
LIMIT 50;

-- ============================================================================
-- Network-Specific Parameter Examples
-- ============================================================================
-- Sample queries for each network's raw parameters

-- Taboola Example
/*
SELECT
    params->>'tb_headline' AS headline,
    params->>'tb_image' AS image_id,
    params->>'tb_item' AS item_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params ? 'tb_headline'
    AND type = 'click'
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2, 3
ORDER BY clicks DESC
LIMIT 20;
*/

-- Outbrain Example
/*
SELECT
    params->>'ob_creative' AS creative_id,
    params->>'ob_item' AS item_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params ? 'ob_creative'
    AND type = 'click'
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 20;
*/

-- MGID Example
/*
SELECT
    params->>'mg_title' AS title,
    params->>'mg_id' AS creative_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params ? 'mg_title'
    AND type = 'click'
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 20;
*/

-- RevContent Example
/*
SELECT
    params->>'rc_title' AS title,
    params->>'rc_id' AS creative_id,
    params->>'rc_thumb' AS thumbnail,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params ? 'rc_title'
    AND type = 'click'
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2, 3
ORDER BY clicks DESC
LIMIT 20;
*/

-- ============================================================================
-- Trino/Presto Views (if using Trino with Iceberg)
-- ============================================================================

/*
-- Normalized campaigns view for Trino
CREATE OR REPLACE VIEW trace.normalized_campaigns AS
SELECT
    ts,
    ip,
    ua,
    url,
    type,
    params,
    -- Detect network
    COALESCE(
        try_cast(params['utm_source'] AS varchar),
        CASE
            WHEN params IS NOT NULL AND contains(map_keys(params), 'tb_image') THEN 'taboola'
            WHEN params IS NOT NULL AND contains(map_keys(params), 'ob_creative') THEN 'outbrain'
            WHEN params IS NOT NULL AND contains(map_keys(params), 'mg_id') THEN 'mgid'
            WHEN params IS NOT NULL AND contains(map_keys(params), 'rc_id') THEN 'revcontent'
            ELSE 'unknown'
        END
    ) AS network,
    try_cast(params['utm_campaign'] AS varchar) AS campaign_id,
    -- Normalized creative_id
    CASE
        WHEN network = 'taboola' THEN try_cast(params['tb_image'] AS varchar)
        WHEN network = 'outbrain' THEN try_cast(params['ob_creative'] AS varchar)
        WHEN network = 'mgid' THEN try_cast(params['mg_id'] AS varchar)
        WHEN network = 'revcontent' THEN try_cast(params['rc_id'] AS varchar)
        ELSE NULL
    END AS creative_id,
    -- Normalized headline
    CASE
        WHEN network = 'taboola' THEN try_cast(params['tb_headline'] AS varchar)
        WHEN network = 'mgid' THEN try_cast(params['mg_title'] AS varchar)
        WHEN network = 'revcontent' THEN try_cast(params['rc_title'] AS varchar)
        ELSE NULL
    END AS headline,
    -- Normalized image_id
    CASE
        WHEN network = 'taboola' THEN try_cast(params['tb_image'] AS varchar)
        WHEN network = 'mgid' THEN try_cast(params['mg_image'] AS varchar)
        WHEN network = 'revcontent' THEN try_cast(params['rc_thumb'] AS varchar)
        ELSE NULL
    END AS image_id,
    -- Item IDs
    CASE
        WHEN network = 'taboola' THEN try_cast(params['tb_item'] AS varchar)
        WHEN network = 'outbrain' THEN try_cast(params['ob_item'] AS varchar)
        WHEN network = 'mgid' THEN try_cast(params['mg_id'] AS varchar)
        WHEN network = 'revcontent' THEN try_cast(params['rc_id'] AS varchar)
        ELSE try_cast(params['item'] AS varchar)
    END AS item_id
FROM trace.events;

-- Network performance for Trino
CREATE OR REPLACE VIEW trace.network_performance AS
SELECT
    network,
    DATE_TRUNC('day', ts) AS date,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct
FROM trace.normalized_campaigns
WHERE ts >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY network, DATE_TRUNC('day', ts)
ORDER BY network, date;
*/

-- ============================================================================
-- Sample Queries Using Normalized Views
-- ============================================================================

-- Top creatives by CTR across all networks
/*
SELECT
    network,
    headline,
    clicks,
    views,
    ctr_pct
FROM top_creatives
WHERE views >= 100
ORDER BY ctr_pct DESC
LIMIT 20;
*/

-- Compare the same headline across networks
/*
SELECT
    normalized_headline,
    networks,
    total_clicks,
    overall_ctr
FROM cross_network_creatives
WHERE num_networks >= 2
ORDER BY overall_ctr DESC
LIMIT 20;
*/

-- Find fatigued creatives that need rotation
/*
SELECT
    network,
    headline,
    recent_ctr,
    prior_ctr,
    fatigue_change_pct
FROM creative_fatigue
WHERE recent_ctr < prior_ctr
    AND fatigue_change_pct < -20
ORDER BY fatigue_change_pct ASC
LIMIT 20;
*/
