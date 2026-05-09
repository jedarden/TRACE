-- ============================================================================
-- Apache Iceberg Table Schema for TRACE Sessions
-- ============================================================================
-- This file defines the Iceberg table schema for sessionized user sessions.
-- Sessions are derived from raw events and represent user journeys.
--
-- Usage with Trino:
--   1. Configure Iceberg catalog in etc/catalog/iceberg.properties
--   2. Execute: source /path/to/sessions_iceberg.sql
--
-- Usage with DuckDB:
--   INSTALL iceberg;
--   LOAD iceberg;
--   -- Then execute the CREATE TABLE statements below
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Primary Sessions Table
-- ----------------------------------------------------------------------------
-- Aggregated session data with first-touch attribution

CREATE TABLE IF NOT EXISTS trace.sessions (
    -- Primary identifiers
    session_id STRING NOT NULL,
    user_id STRING,

    -- Time bounds
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,

    -- Event counts
    pageviews INT,
    clicks INT,
    scrolls INT,
    dwells INT,

    -- Entry and exit pages
    entry_url STRING,
    exit_url STRING,

    -- Attribution (first-touch)
    network STRING,
    campaign_id STRING,
    campaign_name STRING,
    creative_id STRING,
    headline STRING,

    -- Conversion tracking
    converted BOOLEAN,
    conversion_value DOUBLE,

    -- Device info (from first event)
    device_type STRING,
    device_os STRING,
    referrer STRING,

    -- Session quality metrics
    duration_seconds INT,
    bounce BOOLEAN,
    depth INT

)
PARTITIONED BY DAYS(started_at)
LOCATED AT 's3://my-trace-bucket/iceberg/sessions';

-- ----------------------------------------------------------------------------
-- Table Properties
-- ----------------------------------------------------------------------------

ALTER TABLE trace.sessions SET TPROPERTIES (
    'write.format.default' = 'parquet',
    'write.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '268435456',  -- 256MB files (sessions are smaller than events)

    'commit.retry.num-retries' = '10',
    'commit.retry.min-wait-ms' = '100',
    'commit.retry.max-wait-ms' = '60000',

    'history.expire.min-snapshots-to-keep' = '10',
    'history.expire.max-snapshot-age-ms' = '2592000000'
);

-- ============================================================================
-- Helper Views for Session Analytics
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Daily Session Summary
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.daily_session_summary AS
SELECT
    DATE_TRUNC('day', started_at) AS date,
    network,
    COUNT(*) AS sessions,
    SUM(pageviews) AS total_pageviews,
    SUM(clicks) AS total_clicks,
    SUM(scrolls) AS total_scroll_events,
    SUM(dwells) AS total_dwell_events,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS converted_sessions,
    ROUND(100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS conversion_rate_pct,
    AVG(duration_seconds) AS avg_duration_seconds,
    SUM(CASE WHEN bounce THEN 1 ELSE 0 END) AS bounces,
    ROUND(100.0 * SUM(CASE WHEN bounce THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS bounce_rate_pct,
    AVG(depth) AS avg_depth
FROM trace.sessions
WHERE started_at >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ----------------------------------------------------------------------------
-- View: Session Funnels
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.session_funnels AS
WITH funnel_steps AS (
    SELECT
        session_id,
        network,
        campaign_id,
        started_at,
        pageviews,
        clicks,
        converted,
        -- Define funnel stages
        CASE
            WHEN pageviews >= 1 THEN 'landing'
            ELSE NULL
        END AS stage_landing,
        CASE
            WHEN clicks >= 1 THEN 'engaged'
            ELSE NULL
        END AS stage_engaged,
        CASE
            WHEN depth >= 3 THEN 'browsing'
            ELSE NULL
        END AS stage_browsing,
        CASE
            WHEN converted THEN 'converted'
            ELSE NULL
        END AS stage_converted
    FROM trace.sessions
    WHERE started_at >= CURRENT_DATE - INTERVAL '30' DAY
)
SELECT
    DATE_TRUNC('day', started_at) AS date,
    network,
    campaign_id,
    COUNT(*) FILTER (WHERE stage_landing IS NOT NULL) AS landing_sessions,
    COUNT(*) FILTER (WHERE stage_engaged IS NOT NULL) AS engaged_sessions,
    COUNT(*) FILTER (WHERE stage_browsing IS NOT NULL) AS browsing_sessions,
    COUNT(*) FILTER (WHERE stage_converted IS NOT NULL) AS converted_sessions,
    ROUND(100.0 * COUNT(*) FILTER (WHERE stage_engaged IS NOT NULL) / NULLIF(COUNT(*) FILTER (WHERE stage_landing IS NOT NULL), 0), 2) AS landing_to_engagement_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE stage_browsing IS NOT NULL) / NULLIF(COUNT(*) FILTER (WHERE stage_engaged IS NOT NULL), 0), 2) AS engagement_to_browsing_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE stage_converted IS NOT NULL) / NULLIF(COUNT(*) FILTER (WHERE stage_browsing IS NOT NULL), 0), 2) AS browsing_to_conversion_pct
FROM funnel_steps
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2;

-- ----------------------------------------------------------------------------
-- View: Session Quality Analysis
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.session_quality_analysis AS
SELECT
    network,
    campaign_id,
    -- Duration buckets
    CASE
        WHEN duration_seconds < 10 THEN '0-10s'
        WHEN duration_seconds < 30 THEN '10-30s'
        WHEN duration_seconds < 60 THEN '30-60s'
        WHEN duration_seconds < 180 THEN '1-3m'
        WHEN duration_seconds < 300 THEN '3-5m'
        ELSE '5m+'
    END AS duration_bucket,
    -- Metrics
    COUNT(*) AS sessions,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS conversions,
    ROUND(100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS conversion_rate_pct,
    AVG(depth) AS avg_depth,
    SUM(CASE WHEN bounce THEN 1 ELSE 0 END) AS bounces,
    ROUND(100.0 * SUM(CASE WHEN bounce THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS bounce_rate_pct
FROM trace.sessions
WHERE started_at >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2, 3
ORDER BY 2, 3;

-- ============================================================================
-- Migration Notes
-- ============================================================================
--
-- To populate this table from raw events:
--
-- 1. Run the session stitcher in analytics service
-- 2. Export sessions to S3 in Iceberg format
-- 3. Or use this SQL to aggregate from events table:
--
-- INSERT INTO trace.sessions
-- SELECT
--     session_id,
--     user_id,
--     MIN(ts) AS started_at,
--     MAX(ts) AS ended_at,
--     COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
--     COUNT(*) FILTER (WHERE type = 'click') AS clicks,
--     COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
--     COUNT(*) FILTER (WHERE type = 'dwell') AS dwells,
--     MIN(url) FILTER (WHERE row_number = 1) AS entry_url,
--     MAX(url) FILTER (WHERE row_number = last) AS exit_url,
--     -- First-touch attribution
--     MIN(network) AS network,
--     MIN(campaign_id) AS campaign_id,
--     MIN(headline) AS headline,
--     -- Conversion (define your conversion events)
--     MAX(CASE WHEN type = 'conversion' THEN TRUE ELSE FALSE END) AS converted,
--     -- Session metrics
--     EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::INT AS duration_seconds,
--     COUNT(*) = 1 AS bounce,
--     COUNT(DISTINCT url) AS depth
-- FROM trace.ad_events
-- WHERE session_id IS NOT NULL
--   AND ts >= CURRENT_DATE - INTERVAL '7' DAY
-- GROUP BY session_id, user_id;
--
-- ============================================================================
