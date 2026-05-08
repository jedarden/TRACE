-- ============================================================================
-- Iceberg Helper Views for Schema Evolution and Backward Compatibility
-- ============================================================================
-- These views provide stable interfaces that handle schema evolution gracefully.
-- Use these views in applications instead of querying base tables directly.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: ad_events_compatible
-- ----------------------------------------------------------------------------
-- Provides a backward-compatible view that handles all schema versions.
-- New columns are exposed as NULL for older data.

CREATE OR REPLACE VIEW trace.ad_events_compatible AS
SELECT
    -- Core fields (all versions)
    ts,
    ip,
    ua,
    url,
    type,
    session_id,
    user_id,
    cookie_id,
    network,
    campaign_id,
    campaign_name,
    creative_id,
    headline,
    image_id,
    item_id,
    params,

    -- V002 fields (NULL for data before migration)
    COALESCE(referrer, '') AS referrer,
    COALESCE(referrer_network, 'unknown') AS referrer_network,
    attribution_campaign_id,
    attribution_creative_id,
    COALESCE(attribution_touches, 0) AS attribution_touches,
    COALESCE(attribution_days_to_convert, 0) AS attribution_days_to_convert,
    COALESCE(device_type, 'unknown') AS device_type,
    COALESCE(device_os, 'unknown') AS device_os,
    COALESCE(device_browser, 'unknown') AS device_browser,

    -- V003 engagement metrics (NULL for non-scroll/dwell events)
    scroll_depth_pct,
    scroll_time_ms,
    dwell_time_ms,
    dwell_visible_pct,
    viewport_width,
    viewport_height,

    -- V004 quality scores (default to valid for unscored data)
    COALESCE(quality_score, 1.0) AS quality_score,
    COALESCE(bot_probability, 0.0) AS bot_probability,
    COALESCE(fraud_score, 0.0) AS fraud_score,
    COALESCE(is_valid, true) AS is_valid,
    COALESCE(is_verified, true) AS is_verified,
    validation_reason,
    enriched_at,
    enrichment_version

FROM trace.ad_events;

-- ----------------------------------------------------------------------------
-- View: ad_events_quality_filtered
-- ----------------------------------------------------------------------------
-- Returns only high-quality traffic, filtering out likely bots and fraud.

CREATE OR REPLACE VIEW trace.ad_events_quality_filtered AS
SELECT
    ts, ip, ua, url, type,
    session_id, user_id, cookie_id,
    network, campaign_id, campaign_name,
    creative_id, headline, image_id, item_id,
    params, referrer, device_type
FROM trace.ad_events_compatible
WHERE COALESCE(is_valid, true) = true
    AND COALESCE(is_verified, true) = true
    AND COALESCE(quality_score, 1.0) >= 0.5
    AND COALESCE(bot_probability, 0.0) < 0.5
    AND COALESCE(fraud_score, 0.0) < 0.5;

-- ----------------------------------------------------------------------------
-- View: ad_events_engagement
-- ----------------------------------------------------------------------------
-- Focused view for engagement analysis with scroll and dwell metrics.

CREATE OR REPLACE VIEW trace.ad_events_engagement AS
SELECT
    ts,
    network,
    campaign_id,
    creative_id,
    headline,
    type,
    device_type,
    device_os,

    -- Engagement metrics
    scroll_depth_pct,
    scroll_time_ms,
    dwell_time_ms,
    dwell_visible_pct,
    viewport_width,
    viewport_height,

    -- Quality adjusted
    COALESCE(quality_score, 1.0) AS quality_score,

    -- Attribution
    attribution_touches,
    attribution_days_to_convert

FROM trace.ad_events
WHERE type IN ('scroll', 'dwell', 'pageview');

-- ----------------------------------------------------------------------------
-- View: ad_events_attribution
-- ----------------------------------------------------------------------------
-- Attribution analysis view with conversion tracking.

CREATE OR REPLACE VIEW trace.ad_events_attribution AS
SELECT
    DATE_TRUNC('day', ts) AS date,
    network,
    campaign_id,
    campaign_name,
    creative_id,
    headline,

    -- Funnel metrics
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
    COUNT(*) FILTER (WHERE type = 'dwell') AS dwells,

    -- Attribution metrics
    AVG(attribution_touches) AS avg_touches_to_convert,
    AVG(attribution_days_to_convert) AS avg_days_to_convert,

    -- Device breakdown
    COUNT(DISTINCT device_type) AS device_types,
    COUNT(DISTINCT device_os) AS os_types,

    -- Quality metrics
    AVG(COALESCE(quality_score, 1.0)) AS avg_quality_score,
    SUM(CASE WHEN COALESCE(bot_probability, 0.0) > 0.5 THEN 1 ELSE 0 END) AS bot_count,

    MIN(ts) AS first_event,
    MAX(ts) AS last_event

FROM trace.ad_events
WHERE ts >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY date DESC, clicks DESC;

-- ----------------------------------------------------------------------------
-- View: schema_compatibility_check
-- ----------------------------------------------------------------------------
-- Diagnostic view to check which schema version the data supports.

CREATE OR REPLACE VIEW trace.schema_compatibility_check AS
WITH column_counts AS (
    SELECT
        COUNT(*) AS total_columns,
        COUNT(referrer) AS has_v002_columns,
        COUNT(scroll_depth_pct) AS has_v003_columns,
        COUNT(quality_score) AS has_v004_columns
    FROM trace.ad_events
    WHERE ts >= CURRENT_DATE - INTERVAL '1' DAY
)
SELECT
    'V001 (base)' AS schema_level,
    CASE WHEN has_v002_columns > 0 THEN 'YES - Compatible' ELSE 'YES - Native' END AS status
FROM column_counts
UNION ALL
SELECT
    'V002 (referrer/attribution)' AS schema_level,
    CASE WHEN has_v002_columns > 0 THEN 'YES - Native' ELSE 'NO - Missing columns' END AS status
FROM column_counts
UNION ALL
SELECT
    'V003 (engagement)' AS schema_level,
    CASE WHEN has_v003_columns > 0 THEN 'YES - Native' ELSE 'NO - Missing columns' END AS status
FROM column_counts
UNION ALL
SELECT
    'V004 (quality)' AS schema_level,
    CASE WHEN has_v004_columns > 0 THEN 'YES - Native' ELSE 'NO - Missing columns' END AS status
FROM column_counts;

-- ----------------------------------------------------------------------------
-- Function: get_schema_version()
-- ----------------------------------------------------------------------------
-- Returns the current schema version of the ad_events table.

CREATE OR REPLACE FUNCTION trace.get_schema_version()
RETURNS TABLE(version INT, description STRING, applied_at TIMESTAMP)
AS $$
    SELECT version, description, applied_at
    FROM trace.schema_migrations
    ORDER BY version DESC
    LIMIT 1
$$;

-- ----------------------------------------------------------------------------
-- Function: is_column_available(column_name STRING)
-- ----------------------------------------------------------------------------
-- Check if a column exists in the current schema (useful for conditional queries)

CREATE OR REPLACE FUNCTION trace.is_column_available(col_name STRING)
RETURNS BOOLEAN
AS $$
    SELECT COUNT(*) > 0
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'trace'
        AND TABLE_NAME = 'ad_events'
        AND COLUMN_NAME = col_name
$$;

-- ----------------------------------------------------------------------------
-- Diagnostic Views
-- ----------------------------------------------------------------------------

-- Check which migrations have been applied
CREATE OR REPLACE VIEW trace.migration_status AS
SELECT
    version,
    description,
    applied_at,
    applied_by,
    CASE
        WHEN version = (SELECT MAX(version) FROM trace.schema_migrations)
            THEN 'CURRENT'
        ELSE 'SUPERSEDED'
    END AS status
FROM trace.schema_migrations
ORDER BY version DESC;

-- Column inventory with schema version annotations
CREATE OR REPLACE VIEW trace.column_inventory AS
SELECT
    COLUMN_NAME,
    ORDINAL_POSITION,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    DATA_TYPE,
    CASE
        WHEN COLUMN_NAME IN ('ts', 'ip', 'ua', 'url', 'type', 'session_id', 'user_id', 'cookie_id', 'network', 'campaign_id', 'campaign_name', 'creative_id', 'headline', 'image_id', 'item_id', 'params')
            THEN 'V001'
        WHEN COLUMN_NAME IN ('referrer', 'referrer_network', 'attribution_campaign_id', 'attribution_creative_id', 'attribution_touches', 'attribution_days_to_convert', 'device_type', 'device_os', 'device_browser')
            THEN 'V002'
        WHEN COLUMN_NAME IN ('scroll_depth_pct', 'scroll_time_ms', 'dwell_time_ms', 'dwell_visible_pct', 'viewport_width', 'viewport_height')
            THEN 'V003'
        WHEN COLUMN_NAME IN ('quality_score', 'bot_probability', 'fraud_score', 'is_valid', 'is_verified', 'validation_reason', 'enriched_at', 'enrichment_version')
            THEN 'V004'
        ELSE 'UNKNOWN'
    END AS added_in_version
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'trace'
    AND TABLE_NAME = 'ad_events'
ORDER BY ORDINAL_POSITION;
