-- ============================================================================
-- Apache Iceberg Table Schema for TRACE Ad Events
-- ============================================================================
-- This file defines the Iceberg table schema for ad tracking events.
-- It includes normalized fields for cross-network ad campaign analysis.
--
-- Usage with Trino:
--   1. Configure Iceberg catalog in etc/catalog/iceberg.properties
--   2. Execute: source /path/to/ad_events_iceberg.sql
--
-- Usage with DuckDB:
--   INSTALL iceberg;
--   LOAD iceberg;
--   -- Then execute the CREATE TABLE statements below
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Primary Ad Events Table
-- ----------------------------------------------------------------------------
-- This is the main table for all ad-related events (pageview, click, scroll, dwell)
-- It includes both raw event fields and normalized ad campaign fields.

CREATE TABLE IF NOT EXISTS trace.ad_events (
    -- Primary timestamp (partitioning field)
    ts TIMESTAMP NOT NULL,

    -- Basic event fields
    ip STRING,
    ua STRING,
    url STRING NOT NULL,
    type STRING NOT NULL,  -- pageview, click, scroll, dwell

    -- Identity fields
    session_id STRING,
    user_id STRING,
    cookie_id STRING,  -- First-party cookie identity

    -- Normalized network detection
    network STRING,  -- taboola, outbrain, mgid, revcontent, unknown

    -- Campaign identifiers
    campaign_id STRING,  -- utm_campaign normalized
    campaign_name STRING,  -- Campaign name from network API (if available)

    -- Creative identifiers (normalized across networks)
    creative_id STRING,  -- Network-specific creative ID
    headline STRING,  -- Normalized headline/title
    image_id STRING,  -- Normalized image/thumbnail ID
    item_id STRING,  -- Content item ID

    -- Raw parameters (for flexibility)
    params MAP<STRING, STRING>

)
PARTITIONED BY DAYS(ts)
LOCATED AT 's3://my-trace-bucket/iceberg/ad_events';

-- ----------------------------------------------------------------------------
-- Table Properties for Performance
-- ----------------------------------------------------------------------------
-- These properties optimize the table for typical ad analytics queries

ALTER TABLE trace.ad_events SET TPROPERTIES (
    -- Write properties
    'write.format.default' = 'parquet',
    'write.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '536870912',  -- 512MB files

    -- Metadata properties
    'commit.retry.num-retries' = '10',
    'commit.retry.min-wait-ms' = '100',
    'commit.retry.max-wait-ms' = '60000',

    -- Snapshot retention (30 days for time travel)
    'history.expire.min-snapshots-to-keep' = '10',
    'history.expire.max-snapshot-age-ms' = '2592000000'  -- 30 days in milliseconds
);

-- ----------------------------------------------------------------------------
-- Secondary Table: Campaign Dimension (Optional)
-- ----------------------------------------------------------------------------
-- Dimension table for campaign metadata enriched from network APIs

CREATE TABLE IF NOT EXISTS trace.campaigns (
    campaign_id STRING NOT NULL,
    network STRING NOT NULL,
    campaign_name STRING,
    status STRING,  -- active, paused, deleted
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    budget_daily DOUBLE,
    budget_total DOUBLE,
    targeting_country STRING,
    targeting_platform STRING,  -- desktop, mobile, all
    -- Additional metadata as JSON
    metadata MAP<STRING, STRING>
)
PARTITIONED BY (network)
LOCATED AT 's3://my-trace-bucket/iceberg/campaigns';

-- ----------------------------------------------------------------------------
-- Secondary Table: Creatives Dimension (Optional)
-- ----------------------------------------------------------------------------
-- Dimension table for creative asset metadata

CREATE TABLE IF NOT EXISTS trace.creatives (
    creative_id STRING NOT NULL,
    network STRING NOT NULL,
    headline STRING,
    image_url STRING,
    thumbnail_url STRING,
    landing_page_url STRING,
    created_at TIMESTAMP,
    -- Additional metadata as JSON
    metadata MAP<STRING, STRING>
)
PARTITIONED BY (network)
LOCATED AT 's3://my-trace-bucket/iceberg/creatives';

-- ============================================================================
-- Helper Views for Common Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Daily Campaign Performance
-- ----------------------------------------------------------------------------
-- Aggregates daily metrics by campaign

CREATE OR REPLACE VIEW trace.daily_campaign_performance AS
SELECT
    DATE_TRUNC('day', ts) AS date,
    network,
    campaign_id,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
    COUNT(*) FILTER (WHERE type = 'dwell') AS dwells,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'scroll') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS scroll_rate_pct
FROM trace.ad_events
WHERE ts >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 5 DESC;

-- ----------------------------------------------------------------------------
-- View: Creative Performance
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.creative_performance AS
SELECT
    network,
    creative_id,
    headline,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT campaign_id) AS num_campaigns,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr_pct,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM trace.ad_events
WHERE ts >= CURRENT_DATE - INTERVAL '30' DAY
    AND creative_id IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 100
ORDER BY clicks DESC;

-- ----------------------------------------------------------------------------
-- View: Network Comparison
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.network_comparison AS
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
FROM trace.ad_events
WHERE ts >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ============================================================================
-- Migration Notes
-- ============================================================================
--
-- Migrating from existing Parquet data:
--
-- 1. Register existing Parquet files as Iceberg table:
--
--    CALL trace.system.register_parquet_table(
--      table => 'ad_events',
--      metadata_file => 's3://my-trace-bucket/trace-events/events/dt=2026-05-08/hour=14/metadata.json'
--    );
--
-- 2. Or, use the compactor to write directly to Iceberg format:
--    Update compactor to write to: s3://my-trace-bucket/iceberg/ad_events
--
-- 3. After migration, update all queries to use trace.ad_events instead of
--    read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
--
-- ============================================================================
