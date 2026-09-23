-- Cross-Network Normalization Views for TRACE Analytics
--
-- This file contains SQL views that normalize campaign data across different
-- ad networks (Taboola, Outbrain, MGID, RevContent, Google Ads) into a common
-- schema. The parameter mappings mirror the flusher's Rust normalizer
-- (flusher/src/network_mapping.toml); analytics/tests/normalization_views.rs
-- runs fixture events through this exact file and asserts the same values the
-- Rust normalizer produces.
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
        -- Detect the network the same way the Rust normalizer does:
        --   1. utm_source value (aliases normalized: tb -> taboola, google ->
        --      googleads, ...)
        --   2. click identifiers (gclid/gclsrc for Google Ads)
        --   3. signature parameters (a prefix check in Rust; here the
        --      network's documented parameters stand in for the prefix)
        -- Subscripting a MAP with an absent key yields NULL, so
        -- `params['k'] IS NOT NULL` is the key-presence test, and LOWER()
        -- gives the same case-insensitive utm_source matching as Rust.
        CASE
            WHEN LOWER(params['utm_source']) IN ('taboola', 'tb') THEN 'taboola'
            WHEN LOWER(params['utm_source']) IN ('outbrain', 'ob') THEN 'outbrain'
            WHEN LOWER(params['utm_source']) = 'mgid' THEN 'mgid'
            WHEN LOWER(params['utm_source']) IN ('revcontent', 'rc') THEN 'revcontent'
            WHEN LOWER(params['utm_source']) IN ('google', 'googleads', 'google-ads') THEN 'googleads'
            WHEN params['gclid'] IS NOT NULL OR params['gclsrc'] IS NOT NULL THEN 'googleads'
            WHEN params['tb_image'] IS NOT NULL OR params['tb_headline'] IS NOT NULL THEN 'taboola'
            WHEN params['ob_creative'] IS NOT NULL OR params['ob_item'] IS NOT NULL THEN 'outbrain'
            WHEN params['mg_id'] IS NOT NULL OR params['mg_title'] IS NOT NULL THEN 'mgid'
            WHEN params['rc_id'] IS NOT NULL OR params['rc_title'] IS NOT NULL THEN 'revcontent'
            ELSE COALESCE(params['utm_source'], 'unknown')
        END AS detected_network
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
    CASE detected_network
        WHEN 'googleads' THEN COALESCE(
            params['utm_campaign'], params['campaignid'], params['campaign_id'])
        ELSE params['utm_campaign']
    END AS campaign_id,
    -- Normalized ad identifiers (Google Ads: ad group, or the shopping feed
    -- item for Shopping ads)
    CASE detected_network
        WHEN 'taboola' THEN params['tb_item']
        WHEN 'outbrain' THEN params['ob_item']
        WHEN 'mgid' THEN params['mg_id']
        WHEN 'revcontent' THEN params['rc_id']
        WHEN 'googleads' THEN COALESCE(
            params['adgroupid'], params['adgroup_id'],
            params['feeditemid'], params['feed_item_id'])
        ELSE COALESCE(params['ad_id'], params['adid'], params['item'])
    END AS ad_id,
    -- Normalized creative fields
    CASE detected_network
        WHEN 'taboola' THEN params['tb_image']
        WHEN 'outbrain' THEN params['ob_creative']
        WHEN 'mgid' THEN params['mg_id']
        WHEN 'revcontent' THEN params['rc_id']
        WHEN 'googleads' THEN COALESCE(
            params['adgroupid'], params['adgroup_id'],
            params['feeditemid'], params['feed_item_id'],
            params['utm_content'])
        ELSE COALESCE(params['creative'], params['creative_id'], params['asset'])
    END AS creative_id,
    -- Normalized publisher/site identifiers
    CASE detected_network
        WHEN 'taboola' THEN COALESCE(params['tb_publisher'], params['tb_site'])
        WHEN 'outbrain' THEN COALESCE(params['ob_publisher'], params['ob_site'])
        WHEN 'mgid' THEN COALESCE(params['mg_site'], params['mg_publisher'])
        WHEN 'revcontent' THEN COALESCE(params['rc_site'], params['rc_publisher'])
        WHEN 'googleads' THEN COALESCE(params['siteid'], params['site_id'])
        ELSE COALESCE(params['publisher'], params['pub'], params['site'])
    END AS publisher_id,
    -- Normalized placement identifiers (Google Ads: target id, or the
    -- placement for placement-targeted campaigns)
    CASE detected_network
        WHEN 'taboola' THEN params['tb_placement']
        WHEN 'outbrain' THEN params['ob_placement']
        WHEN 'mgid' THEN params['mg_placement']
        WHEN 'revcontent' THEN params['rc_placement']
        WHEN 'googleads' THEN COALESCE(
            params['targetid'], params['target_id'], params['placement'])
        ELSE COALESCE(params['placement'], params['position'])
    END AS placement_id,
    -- Normalized headline/title (Google Ads search ads pass the matched
    -- keyword, placement campaigns the placement)
    CASE detected_network
        WHEN 'taboola' THEN params['tb_headline']
        WHEN 'mgid' THEN params['mg_title']
        WHEN 'revcontent' THEN params['rc_title']
        WHEN 'googleads' THEN COALESCE(
            params['headline'], params['keyword'], params['placement'],
            params['utm_term'], params['target'])
        ELSE COALESCE(params['headline'], params['title'], params['head'])
    END AS headline,
    -- Normalized image ID
    CASE detected_network
        WHEN 'taboola' THEN params['tb_image']
        WHEN 'outbrain' THEN params['ob_creative']
        WHEN 'mgid' THEN params['mg_image']
        WHEN 'revcontent' THEN params['rc_thumb']
        WHEN 'googleads' THEN COALESCE(
            params['imageid'], params['image_id'], params['creative'])
        ELSE COALESCE(params['image'], params['img'], params['thumb'], params['thumbnail'])
    END AS image_id,
    -- Item identifiers
    CASE detected_network
        WHEN 'taboola' THEN params['tb_item']
        WHEN 'outbrain' THEN params['ob_item']
        WHEN 'mgid' THEN params['mg_id']
        WHEN 'revcontent' THEN params['rc_id']
        WHEN 'googleads' THEN COALESCE(
            params['feeditemid'], params['feed_item_id'],
            params['adgroupid'], params['adgroup_id'])
        ELSE COALESCE(params['item'], params['asset'])
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
WHERE ts >= CURRENT_DATE + INTERVAL '-30 days'
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
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
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
    WHERE ts >= CURRENT_DATE + INTERVAL '-30 days'
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
        AVG(ctr) FILTER (WHERE date >= CURRENT_DATE + INTERVAL '-7 days') AS recent_ctr,
        AVG(ctr) FILTER (
            WHERE date < CURRENT_DATE + INTERVAL '-7 days'
            AND date >= CURRENT_DATE + INTERVAL '-21 days'
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
        AND ts >= CURRENT_DATE + INTERVAL '-14 days'
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
    params['tb_headline'] AS headline,
    params['tb_image'] AS image_id,
    params['tb_item'] AS item_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params['tb_headline'] IS NOT NULL
    AND type = 'click'
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
GROUP BY 1, 2, 3
ORDER BY clicks DESC
LIMIT 20;
*/

-- Outbrain Example
/*
SELECT
    params['ob_creative'] AS creative_id,
    params['ob_item'] AS item_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params['ob_creative'] IS NOT NULL
    AND type = 'click'
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 20;
*/

-- MGID Example
/*
SELECT
    params['mg_title'] AS title,
    params['mg_id'] AS creative_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params['mg_title'] IS NOT NULL
    AND type = 'click'
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 20;
*/

-- RevContent Example
/*
SELECT
    params['rc_title'] AS title,
    params['rc_id'] AS creative_id,
    params['rc_thumb'] AS thumbnail,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params['rc_title'] IS NOT NULL
    AND type = 'click'
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
GROUP BY 1, 2, 3
ORDER BY clicks DESC
LIMIT 20;
*/

-- Google Ads Example
/*
SELECT
    params['keyword'] AS keyword,
    params['adgroupid'] AS ad_group_id,
    params['campaignid'] AS campaign_id,
    COUNT(*) AS clicks
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params['gclid'] IS NOT NULL
    AND type = 'click'
    AND ts >= CURRENT_DATE + INTERVAL '-7 days'
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
WITH network_detection AS (
    SELECT
        *,
        -- Detect the network (same order as the DuckDB view above).
        -- try_cast swallows the error a bare subscript raises for an absent
        -- MAP key in Trino.
        CASE
            WHEN lower(try_cast(params['utm_source'] AS varchar)) IN ('taboola', 'tb') THEN 'taboola'
            WHEN lower(try_cast(params['utm_source'] AS varchar)) IN ('outbrain', 'ob') THEN 'outbrain'
            WHEN lower(try_cast(params['utm_source'] AS varchar)) = 'mgid' THEN 'mgid'
            WHEN lower(try_cast(params['utm_source'] AS varchar)) IN ('revcontent', 'rc') THEN 'revcontent'
            WHEN lower(try_cast(params['utm_source'] AS varchar)) IN ('google', 'googleads', 'google-ads') THEN 'googleads'
            WHEN params IS NOT NULL AND (contains(map_keys(params), 'gclid') OR contains(map_keys(params), 'gclsrc')) THEN 'googleads'
            WHEN params IS NOT NULL AND (contains(map_keys(params), 'tb_image') OR contains(map_keys(params), 'tb_headline')) THEN 'taboola'
            WHEN params IS NOT NULL AND (contains(map_keys(params), 'ob_creative') OR contains(map_keys(params), 'ob_item')) THEN 'outbrain'
            WHEN params IS NOT NULL AND (contains(map_keys(params), 'mg_id') OR contains(map_keys(params), 'mg_title')) THEN 'mgid'
            WHEN params IS NOT NULL AND (contains(map_keys(params), 'rc_id') OR contains(map_keys(params), 'rc_title')) THEN 'revcontent'
            ELSE coalesce(try_cast(params['utm_source'] AS varchar), 'unknown')
        END AS detected_network
    FROM trace.events
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
    CASE detected_network
        WHEN 'googleads' THEN coalesce(
            try_cast(params['utm_campaign'] AS varchar),
            try_cast(params['campaignid'] AS varchar),
            try_cast(params['campaign_id'] AS varchar))
        ELSE try_cast(params['utm_campaign'] AS varchar)
    END AS campaign_id,
    -- Normalized ad_id
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_item'] AS varchar)
        WHEN 'outbrain' THEN try_cast(params['ob_item'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_id'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_id'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['adgroupid'] AS varchar),
            try_cast(params['adgroup_id'] AS varchar),
            try_cast(params['feeditemid'] AS varchar),
            try_cast(params['feed_item_id'] AS varchar))
        ELSE coalesce(
            try_cast(params['ad_id'] AS varchar),
            try_cast(params['adid'] AS varchar),
            try_cast(params['item'] AS varchar))
    END AS ad_id,
    -- Normalized creative_id
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_image'] AS varchar)
        WHEN 'outbrain' THEN try_cast(params['ob_creative'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_id'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_id'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['adgroupid'] AS varchar),
            try_cast(params['adgroup_id'] AS varchar),
            try_cast(params['feeditemid'] AS varchar),
            try_cast(params['feed_item_id'] AS varchar),
            try_cast(params['utm_content'] AS varchar))
        ELSE coalesce(
            try_cast(params['creative'] AS varchar),
            try_cast(params['creative_id'] AS varchar),
            try_cast(params['asset'] AS varchar))
    END AS creative_id,
    -- Normalized publisher_id
    CASE detected_network
        WHEN 'taboola' THEN coalesce(
            try_cast(params['tb_publisher'] AS varchar),
            try_cast(params['tb_site'] AS varchar))
        WHEN 'outbrain' THEN coalesce(
            try_cast(params['ob_publisher'] AS varchar),
            try_cast(params['ob_site'] AS varchar))
        WHEN 'mgid' THEN coalesce(
            try_cast(params['mg_site'] AS varchar),
            try_cast(params['mg_publisher'] AS varchar))
        WHEN 'revcontent' THEN coalesce(
            try_cast(params['rc_site'] AS varchar),
            try_cast(params['rc_publisher'] AS varchar))
        WHEN 'googleads' THEN coalesce(
            try_cast(params['siteid'] AS varchar),
            try_cast(params['site_id'] AS varchar))
        ELSE coalesce(
            try_cast(params['publisher'] AS varchar),
            try_cast(params['pub'] AS varchar),
            try_cast(params['site'] AS varchar))
    END AS publisher_id,
    -- Normalized placement_id
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_placement'] AS varchar)
        WHEN 'outbrain' THEN try_cast(params['ob_placement'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_placement'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_placement'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['targetid'] AS varchar),
            try_cast(params['target_id'] AS varchar),
            try_cast(params['placement'] AS varchar))
        ELSE coalesce(
            try_cast(params['placement'] AS varchar),
            try_cast(params['position'] AS varchar))
    END AS placement_id,
    -- Normalized headline
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_headline'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_title'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_title'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['headline'] AS varchar),
            try_cast(params['keyword'] AS varchar),
            try_cast(params['placement'] AS varchar),
            try_cast(params['utm_term'] AS varchar),
            try_cast(params['target'] AS varchar))
        ELSE coalesce(
            try_cast(params['headline'] AS varchar),
            try_cast(params['title'] AS varchar),
            try_cast(params['head'] AS varchar))
    END AS headline,
    -- Normalized image_id
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_image'] AS varchar)
        WHEN 'outbrain' THEN try_cast(params['ob_creative'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_image'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_thumb'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['imageid'] AS varchar),
            try_cast(params['image_id'] AS varchar),
            try_cast(params['creative'] AS varchar))
        ELSE coalesce(
            try_cast(params['image'] AS varchar),
            try_cast(params['img'] AS varchar),
            try_cast(params['thumb'] AS varchar),
            try_cast(params['thumbnail'] AS varchar))
    END AS image_id,
    -- Item IDs
    CASE detected_network
        WHEN 'taboola' THEN try_cast(params['tb_item'] AS varchar)
        WHEN 'outbrain' THEN try_cast(params['ob_item'] AS varchar)
        WHEN 'mgid' THEN try_cast(params['mg_id'] AS varchar)
        WHEN 'revcontent' THEN try_cast(params['rc_id'] AS varchar)
        WHEN 'googleads' THEN coalesce(
            try_cast(params['feeditemid'] AS varchar),
            try_cast(params['feed_item_id'] AS varchar),
            try_cast(params['adgroupid'] AS varchar),
            try_cast(params['adgroup_id'] AS varchar))
        ELSE coalesce(
            try_cast(params['item'] AS varchar),
            try_cast(params['asset'] AS varchar))
    END AS item_id
FROM network_detection;

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
