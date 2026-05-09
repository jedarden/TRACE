-- ============================================================================
-- TRACE Iceberg Schema Migration Framework
-- ============================================================================
-- This file provides a framework for managing schema migrations on Iceberg
-- tables. It includes migration tracking, rollback procedures, and validation.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Migration Tracking Table
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS trace.schema_migrations (
    version INT NOT NULL,
    description STRING NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    applied_by STRING NOT NULL,
    checksum STRING,
    rollback_sql STRING
)
PARTITIONED BY (version)
LOCATED AT 's3://my-trace-bucket/iceberg/schema_migrations';

-- ----------------------------------------------------------------------------
-- Migration Functions
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Function: apply_migration()
-- ----------------------------------------------------------------------------
-- Apply a schema migration with tracking and rollback support

CREATE OR REPLACE FUNCTION trace.apply_migration(
    migration_version INT,
    migration_description STRING,
    migration_sql STRING,
    rollback_sql STRING
)
RETURNS BOOLEAN
AS $$
    DECLARE
        current_version INT;
        migration_count INT;
    BEGIN
        -- Check if migration is already applied
        SELECT COUNT(*) INTO migration_count
        FROM trace.schema_migrations
        WHERE version = migration_version;

        IF migration_count > 0 THEN
            RAISE INFO 'Migration % already applied', migration_version;
            RETURN FALSE;
        END IF;

        -- Get current version
        SELECT COALESCE(MAX(version), 0) INTO current_version
        FROM trace.schema_migrations;

        -- Validate version is sequential
        IF migration_version != current_version + 1 THEN
            RAISE EXCEPTION 'Migration version % must follow current version %',
                migration_version, current_version;
        END IF;

        -- Execute migration
        EXECUTE migration_sql;

        -- Record migration
        INSERT INTO trace.schema_migrations (version, description, applied_at, applied_by, checksum, rollback_sql)
        VALUES (
            migration_version,
            migration_description,
            CURRENT_TIMESTAMP,
            CURRENT_USER,
            MD5(migration_sql),
            rollback_sql
        );

        RAISE INFO 'Migration % applied successfully: %', migration_version, migration_description;
        RETURN TRUE;
    END;
$$;

-- ----------------------------------------------------------------------------
-- Function: rollback_migration()
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION trace.rollback_migration(migration_version INT)
RETURNS BOOLEAN
AS $$
    DECLARE
        rollback_sql TEXT;
        migration_exists INT;
    BEGIN
        -- Check if migration exists
        SELECT COUNT(*) INTO migration_exists
        FROM trace.schema_migrations
        WHERE version = migration_version;

        IF migration_exists = 0 THEN
            RAISE EXCEPTION 'Migration % not found', migration_version;
        END IF;

        -- Get rollback SQL
        SELECT rollback_sql INTO rollback_sql
        FROM trace.schema_migrations
        WHERE version = migration_version;

        IF rollback_sql IS NULL THEN
            RAISE EXCEPTION 'Migration % does not have a rollback script', migration_version;
        END IF;

        -- Execute rollback
        EXECUTE rollback_sql;

        -- Remove migration record
        DELETE FROM trace.schema_migrations
        WHERE version = migration_version;

        RAISE INFO 'Migration % rolled back successfully', migration_version;
        RETURN TRUE;
    END;
$$;

-- ----------------------------------------------------------------------------
-- Function: get_current_version()
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION trace.get_current_version()
RETURNS TABLE(version INT, description STRING, applied_at TIMESTAMP)
AS $$
    SELECT version, description, applied_at
    FROM trace.schema_migrations
    ORDER BY version DESC
    LIMIT 1;
$$;

-- ============================================================================
-- Defined Migrations
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Migration V002: Add referrer and attribution fields
-- ----------------------------------------------------------------------------

SELECT trace.apply_migration(
    2,
    'Add referrer tracking and attribution fields',
    $$ALTER TABLE trace.ad_events ADD COLUMN referrer STRING;
      ALTER TABLE trace.ad_events ADD COLUMN referrer_network STRING;
      ALTER TABLE trace.ad_events ADD COLUMN attribution_campaign_id STRING;
      ALTER TABLE trace.ad_events ADD COLUMN attribution_creative_id STRING;
      ALTER TABLE trace.ad_events ADD COLUMN attribution_touches INT;
      ALTER TABLE trace.ad_events ADD COLUMN attribution_days_to_convert INT;$$,
    $$-- Rollback V002: Note that Iceberg doesn't support DROP COLUMN in all engines
      -- Instead, we use time travel to query pre-V002 data:
      -- SELECT * FROM trace.ad_events FOR VERSION AS OF <pre_v002_snapshot_id>$$
);

-- ----------------------------------------------------------------------------
-- Migration V003: Add device detection
-- ----------------------------------------------------------------------------

SELECT trace.apply_migration(
    3,
    'Add device detection fields',
    $$ALTER TABLE trace.ad_events ADD COLUMN device_type STRING;
      ALTER TABLE trace.ad_events ADD COLUMN device_os STRING;
      ALTER TABLE trace.ad_events ADD COLUMN device_browser STRING;$$,
    $$-- Rollback V003: Use time travel or ignore columns in queries
      -- SELECT * FROM trace.ad_events FOR VERSION AS OF <pre_v003_snapshot_id>$$
);

-- ----------------------------------------------------------------------------
-- Migration V004: Add engagement metrics
-- ----------------------------------------------------------------------------

SELECT trace.apply_migration(
    4,
    'Add engagement metrics (scroll, dwell)',
    $$ALTER TABLE trace.ad_events ADD COLUMN scroll_depth_pct INT;
      ALTER TABLE trace.ad_events ADD COLUMN scroll_time_ms INT;
      ALTER TABLE trace.ad_events ADD COLUMN dwell_time_ms INT;
      ALTER TABLE trace.ad_events ADD COLUMN dwell_visible_pct INT;
      ALTER TABLE trace.ad_events ADD COLUMN viewport_width INT;
      ALTER TABLE trace.ad_events ADD COLUMN viewport_height INT;$$,
    $$-- Rollback V004: Use time travel or ignore columns in queries
      -- SELECT * FROM trace.ad_events FOR VERSION AS OF <pre_v004_snapshot_id>$$
);

-- ----------------------------------------------------------------------------
-- Migration V005: Add quality scoring
-- ----------------------------------------------------------------------------

SELECT trace.apply_migration(
    5,
    'Add quality scoring and validation fields',
    $$ALTER TABLE trace.ad_events ADD COLUMN quality_score DOUBLE;
      ALTER TABLE trace.ad_events ADD COLUMN bot_probability DOUBLE;
      ALTER TABLE trace.ad_events ADD COLUMN fraud_score DOUBLE;
      ALTER TABLE trace.ad_events ADD COLUMN is_valid BOOLEAN;
      ALTER TABLE trace.ad_events ADD COLUMN is_verified BOOLEAN;
      ALTER TABLE trace.ad_events ADD COLUMN validation_reason STRING;
      ALTER TABLE trace.ad_events ADD COLUMN enriched_at TIMESTAMP;
      ALTER TABLE trace.ad_events ADD COLUMN enrichment_version STRING;$$,
    $$-- Rollback V005: Use time travel or ignore columns in queries
      -- SELECT * FROM trace.ad_events FOR VERSION AS OF <pre_v005_snapshot_id>$$
);

-- ============================================================================
-- Migration Validation Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query: Check migration status
-- ----------------------------------------------------------------------------

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

-- ----------------------------------------------------------------------------
-- Query: Get migration history
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.migration_history AS
SELECT
    version,
    description,
    applied_at,
    applied_by,
    LEAD(version) OVER (ORDER BY version) - version AS gap_to_next,
    CASE
        WHEN rollback_sql IS NULL THEN 'NO ROLLBACK'
        ELSE 'ROLLBACK AVAILABLE'
    END AS rollback_status
FROM trace.schema_migrations
ORDER BY version;

-- ----------------------------------------------------------------------------
-- Query: Validate schema compatibility
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW trace.schema_compatibility AS
WITH version_columns AS (
    SELECT
        'V001' AS version,
        ARRAY['ts', 'ip', 'ua', 'url', 'type', 'session_id', 'user_id',
              'cookie_id', 'network', 'campaign_id', 'campaign_name',
              'creative_id', 'headline', 'image_id', 'item_id', 'params'] AS columns
    UNION ALL
    SELECT 'V002', ARRAY['referrer', 'referrer_network', 'attribution_campaign_id',
                          'attribution_creative_id', 'attribution_touches', 'attribution_days_to_convert']
    UNION ALL
    SELECT 'V003', ARRAY['device_type', 'device_os', 'device_browser']
    UNION ALL
    SELECT 'V004', ARRAY['scroll_depth_pct', 'scroll_time_ms', 'dwell_time_ms',
                          'dwell_visible_pct', 'viewport_width', 'viewport_height']
    UNION ALL
    SELECT 'V005', ARRAY['quality_score', 'bot_probability', 'fraud_score',
                          'is_valid', 'is_verified', 'validation_reason',
                          'enriched_at', 'enrichment_version']
),
actual_columns AS (
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'trace'
      AND TABLE_NAME = 'ad_events'
)
SELECT
    v.version,
    ARRAY_LENGTH(v.columns, 1) AS column_count,
    COUNT(*) FILTER (WHERE ac.COLUMN_NAME = ANY(v.columns)) AS columns_present,
    CASE
        WHEN COUNT(*) FILTER (WHERE ac.COLUMN_NAME = ANY(v.columns)) = ARRAY_LENGTH(v.columns, 1)
            THEN 'YES'
        ELSE 'PARTIAL'
    END AS status
FROM version_columns v
LEFT JOIN actual_columns ac ON TRUE
GROUP BY v.version, v.columns
ORDER BY v.version;

-- ============================================================================
-- Utility Procedures
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Procedure: Recreate compatibility views after migration
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE trace.rebuild_compatibility_views()
AS $$
BEGIN
    -- Drop existing views
    DROP VIEW IF EXISTS trace.ad_events_compatible;

    -- Recreate ad_events_compatible view with all schema versions
    CREATE OR REPLACE VIEW trace.ad_events_compatible AS
    SELECT
        -- Core fields (V001)
        ts, ip, ua, url, type,
        session_id, user_id, cookie_id,
        network, campaign_id, campaign_name,
        creative_id, headline, image_id, item_id, params,

        -- V002 fields (NULL for older data)
        COALESCE(referrer, '') AS referrer,
        COALESCE(referrer_network, 'unknown') AS referrer_network,
        attribution_campaign_id,
        attribution_creative_id,
        COALESCE(attribution_touches, 0) AS attribution_touches,
        COALESCE(attribution_days_to_convert, 0) AS attribution_days_to_convert,

        -- V003 fields (NULL for older data)
        COALESCE(device_type, 'unknown') AS device_type,
        COALESCE(device_os, 'unknown') AS device_os,
        COALESCE(device_browser, 'unknown') AS device_browser,

        -- V004 fields (NULL for older data)
        scroll_depth_pct,
        scroll_time_ms,
        dwell_time_ms,
        dwell_visible_pct,
        viewport_width,
        viewport_height,

        -- V005 fields (defaults for older data)
        COALESCE(quality_score, 1.0) AS quality_score,
        COALESCE(bot_probability, 0.0) AS bot_probability,
        COALESCE(fraud_score, 0.0) AS fraud_score,
        COALESCE(is_valid, TRUE) AS is_valid,
        COALESCE(is_verified, TRUE) AS is_verified,
        validation_reason,
        enriched_at,
        enrichment_version
    FROM trace.ad_events;
END;
$$;

-- ============================================================================
-- Usage Examples
-- ============================================================================
--
-- Check current schema version:
--   SELECT * FROM trace.get_current_version();
--
-- View migration history:
--   SELECT * FROM trace.migration_history;
--
-- Apply a new migration:
--   SELECT trace.apply_migration(
--       6,
--       'Add new feature column',
--       'ALTER TABLE trace.ad_events ADD COLUMN new_feature STRING;',
--       '-- No rollback available'
--   );
--
-- Rollback a migration (if rollback SQL exists):
--   SELECT trace.rollback_migration(5);
--
-- Rebuild compatibility views:
--   CALL trace.rebuild_compatibility_views();
--
-- Check schema compatibility:
--   SELECT * FROM trace.schema_compatibility;
--
-- ============================================================================
