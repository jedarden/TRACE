-- ============================================================================
-- Apache Iceberg Table Schema for TRACE Assets
-- ============================================================================
-- This file defines the Iceberg table schema for creative assets (headlines,
-- images, videos) across all ad networks.
--
-- Usage with Trino:
--   1. Configure Iceberg catalog in etc/catalog/iceberg.properties
--   2. Execute: source /path/to/assets_iceberg.sql
--
-- Usage with DuckDB:
--   INSTALL iceberg;
--   LOAD iceberg;
--   -- Then execute the CREATE TABLE statements below
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Primary Assets Table
-- ----------------------------------------------------------------------------
-- Dimension table for creative assets with performance metrics

CREATE TABLE IF NOT EXISTS trace.assets (
    -- Primary identifiers
    asset_id STRING NOT NULL,
    network STRING NOT NULL,

    -- Asset classification
    type STRING NOT NULL,  -- headline, image, video, thumbnail
    content STRING NOT NULL,  -- Text for headlines, URL for media

    -- Timestamps
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,

    -- Performance metrics
    total_views BIGINT,
    total_clicks BIGINT,
    total_sessions BIGINT,
    total_conversions BIGINT,

    -- Derived metrics
    ctr DOUBLE,  -- Click-through rate
    conversion_rate DOUBLE,

    -- Usage metrics
    campaign_count INT,
    active_campaigns INT,

    -- Quality indicators
    avg_quality_score DOUBLE,
    avg_position INT,  -- Average position in ad feed

    -- Content metadata
    metadata MAP<STRING, STRING>

)
PARTITIONED BY (network, type)
LOCATED AT 's3://my-trace-bucket/iceberg/assets';

-- ----------------------------------------------------------------------------
-- Table Properties
-- ----------------------------------------------------------------------------

ALTER TABLE trace.assets SET TPROPERTIES (
    'write.format.default' = 'parquet',
    'write.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',  -- 128MB files (assets are smaller)

    'commit.retry.num-retries' = '10',
    'commit.retry.min-wait-ms' = '100',
    'commit.retry.max-wait-ms' = '60000',

    'history.expire.min-snapshots-to-keep' = '10',
    'history.expire.max-snapshot-age-ms' = '2592000000'
);

-- ============================================================================
-- Helper Views for Asset Analytics
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Asset Performance Summary
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.asset_performance_summary AS
SELECT
    network,
    type,
    COUNT(*) AS total_assets,
    SUM(total_views) AS total_views,
    SUM(total_clicks) AS total_clicks,
    ROUND(100.0 * SUM(total_clicks) / NULLIF(SUM(total_views), 0), 2) AS overall_ctr,
    SUM(total_conversions) AS total_conversions,
    ROUND(100.0 * SUM(total_conversions) / NULLIF(SUM(total_sessions), 0), 2) AS overall_conversion_rate,
    AVG(avg_quality_score) AS avg_quality_score,
    COUNT(CASE WHEN first_seen >= CURRENT_DATE - INTERVAL '7' DAY THEN 1 END) AS new_assets_7d,
    COUNT(CASE WHEN last_seen >= CURRENT_DATE - INTERVAL '7' DAY THEN 1 END) AS active_assets_7d
FROM trace.assets
WHERE first_seen >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY 1, 2
ORDER BY 1, 2;

-- ----------------------------------------------------------------------------
-- View: Top Headlines
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.top_headlines AS
SELECT
    network,
    asset_id,
    content AS headline,
    total_views,
    total_clicks,
    ROUND(100.0 * total_clicks / NULLIF(total_views, 0), 2) AS ctr,
    total_conversions,
    campaign_count,
    avg_quality_score,
    first_seen,
    last_seen
FROM trace.assets
WHERE type = 'headline'
  AND last_seen >= CURRENT_DATE - INTERVAL '30' DAY
  AND total_views >= 100
ORDER BY total_clicks DESC;

-- ----------------------------------------------------------------------------
-- View: Creative Fatigue Detection
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.creative_fatigue AS
WITH asset_daily_performance AS (
    SELECT
        DATE_TRUNC('day', ad_events.ts) AS date,
        ad_events.headline,
        ad_events.network,
        COUNT(*) FILTER (WHERE ad_events.type = 'pageview') AS views,
        COUNT(*) FILTER (WHERE ad_events.type = 'click') AS clicks
    FROM trace.ad_events ad_events
    WHERE ad_events.headline IS NOT NULL
      AND ad_events.ts >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY 1, 2, 3
),
asset_metrics AS (
    SELECT
        headline,
        network,
        AVG(ctr) AS avg_ctr,
        STDDEV(ctr) AS ctr_stddev,
        COUNT(DISTINCT date) AS days_active
    FROM (
        SELECT
            date,
            headline,
            network,
            views,
            clicks,
            CASE
                WHEN views > 0 THEN 100.0 * clicks / views
                ELSE NULL
            END AS ctr
        FROM asset_daily_performance
    ) daily_ctr
    GROUP BY headline, network
)
SELECT
    a.headline,
    a.network,
    a.total_views,
    a.total_clicks,
    ROUND(100.0 * a.total_clicks / NULLIF(a.total_views, 0), 2) AS overall_ctr,
    ROUND(m.avg_ctr, 2) AS avg_daily_ctr,
    ROUND(m.ctr_stddev, 2) AS ctr_volatility,
    m.days_active,
    -- Fatigue indicators
    CASE
        WHEN m.ctr_stddev > 0.5 * m.avg_ctr THEN 'HIGH_VOLATILITY'
        WHEN a.avg_quality_score < 0.5 THEN 'LOW_QUALITY'
        WHEN m.days_active > 14 THEN 'POTENTIALLY_FATIGUED'
        ELSE 'HEALTHY'
    END AS fatigue_status
FROM trace.assets a
JOIN asset_metrics m
    ON a.content = m.headline
    AND a.network = m.network
WHERE a.type = 'headline'
  AND a.last_seen >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY m.ctr_stddev DESC NULLS LAST, overall_ctr DESC;

-- ----------------------------------------------------------------------------
-- View: Cross-Network Asset Comparison
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.cross_network_assets AS
WITH asset_groups AS (
    -- Group assets by content similarity
    SELECT
        content,
        -- Normalize content for comparison
        LOWER(TRIM(content)) AS normalized_content,
        type,
        network,
        asset_id,
        total_views,
        total_clicks,
        total_conversions
    FROM trace.assets
    WHERE type IN ('headline', 'image')
)
SELECT
    normalized_content AS content_group,
    type,
    -- Networks where this asset appears
    STRING_AGG(DISTINCT network, ', ') AS networks,
    COUNT(DISTINCT network) AS network_count,
    -- Aggregate performance
    SUM(total_views) AS total_views,
    SUM(total_clicks) AS total_clicks,
    ROUND(100.0 * SUM(total_clicks) / NULLIF(SUM(total_views), 0), 2) AS overall_ctr,
    SUM(total_conversions) AS total_conversions,
    -- Per-network breakdown
    ARRAY_AGG(
        JSON_BUILD_OBJECT(
            'network', network,
            'views', total_views,
            'clicks', total_clicks,
            'ctr', ROUND(100.0 * total_clicks / NULLIF(total_views, 0), 2)
        )
    ) AS network_performance
FROM asset_groups
WHERE total_views >= 100
GROUP BY normalized_content, type
HAVING COUNT(DISTINCT network) >= 2
ORDER BY total_clicks DESC;

-- ----------------------------------------------------------------------------
-- View: Asset Trend Analysis
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.asset_trends AS
WITH recent_performance AS (
    SELECT
        ad_events.headline,
        ad_events.network,
        DATE_TRUNC('day', ad_events.ts) AS date,
        COUNT(*) FILTER (WHERE ad_events.type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE ad_events.type = 'pageview') AS views
    FROM trace.ad_events ad_events
    WHERE ad_events.headline IS NOT NULL
      AND ad_events.ts >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY 1, 2, 3
),
trend_metrics AS (
    SELECT
        headline,
        network,
        -- Last 7 days
        SUM(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7' DAY) AS clicks_7d,
        SUM(views) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7' DAY) AS views_7d,
        -- Previous 7 days
        SUM(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '14' DAY AND date < CURRENT_DATE - INTERVAL '7' DAY) AS clicks_7d_prev,
        SUM(views) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '14' DAY AND date < CURRENT_DATE - INTERVAL '7' DAY) AS views_7d_prev,
        -- Overall
        SUM(clicks) AS clicks_total,
        SUM(views) AS views_total
    FROM recent_performance
    GROUP BY 1, 2
)
SELECT
    headline,
    network,
    clicks_7d,
    views_7d,
    ROUND(100.0 * clicks_7d / NULLIF(views_7d, 0), 2) AS ctr_7d,
    clicks_7d_prev,
    views_7d_prev,
    ROUND(100.0 * clicks_7d_prev / NULLIF(views_7d_prev, 0), 2) AS ctr_7d_prev,
    -- Trend calculation
    CASE
        WHEN views_7d_prev > 0 THEN
            ROUND(100.0 * (clicks_7d::FLOAT / NULLIF(views_7d, 0)) / NULLIF(clicks_7d_prev::FLOAT / NULLIF(views_7d_prev, 0), 0) - 100, 2)
        ELSE NULL
    END AS ctr_trend_pct,
    clicks_total,
    views_total,
    ROUND(100.0 * clicks_total / NULLIF(views_total, 0), 2) AS ctr_overall
FROM trend_metrics
WHERE views_7d >= 100
ORDER BY ctr_trend_pct DESC NULLS LAST;

-- ============================================================================
-- Migration Notes
-- ============================================================================
--
-- To populate this table from raw events:
--
-- 1. Extract unique assets from events
-- 2. Aggregate performance metrics
-- 3. Load into Iceberg table
--
-- Example SQL:
--
-- INSERT INTO trace.assets
-- WITH asset_events AS (
--     SELECT
--         -- Create unique asset ID
--         CONCAT(network, ':', type, ':', content) AS asset_id,
--         network,
--         CASE
--             WHEN params->>'tb_headline' IS NOT NULL THEN 'headline'
--             WHEN params->>'tb_image' IS NOT NULL THEN 'image'
--             ELSE 'unknown'
--         END AS type,
--         COALESCE(params->>'tb_headline', params->>'tb_image') AS content,
--         ts,
--         type AS event_type
--     FROM trace.ad_events
--     WHERE ts >= CURRENT_DATE - INTERVAL '7' DAY
-- )
-- SELECT
--     asset_id,
--     network,
--     type,
--     content,
--     MIN(ts) AS first_seen,
--     MAX(ts) AS last_seen,
--     COUNT(*) FILTER (WHERE event_type = 'pageview') AS total_views,
--     COUNT(*) FILTER (WHERE event_type = 'click') AS total_clicks,
--     COUNT(DISTINCT params->>'utm_campaign') AS campaign_count
-- FROM asset_events
-- WHERE content IS NOT NULL
-- GROUP BY 1, 2, 3, 4;
--
-- ============================================================================
