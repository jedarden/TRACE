-- ============================================================================
-- Migration V002: Add Referrer and Attribution Fields
-- ============================================================================
-- This migration adds referrer tracking and attribution fields for ad events.
--
-- Backward Compatibility: SAFE
-- - New columns are nullable
-- - No existing queries are affected
-- - Old readers simply see NULL for new columns
-- ============================================================================

-- Add referrer tracking to ad_events
ALTER TABLE trace.ad_events
ADD COLUMN referrer STRING DEFAULT NULL,
ADD COLUMN referrer_network STRING DEFAULT NULL;  -- google, facebook, twitter, direct, etc.

-- Add attribution fields for conversion tracking
ALTER TABLE trace.ad_events
ADD COLUMN attribution_campaign_id STRING DEFAULT NULL,  -- Original campaign that led to conversion
ADD COLUMN attribution_creative_id STRING DEFAULT NULL,  -- Original creative that led to conversion
ADD COLUMN attribution_touches INT DEFAULT NULL,  -- Number of ad touches before conversion
ADD COLUMN attribution_days_to_convert INT DEFAULT NULL;  -- Days from first touch to conversion

-- Add device context for better targeting
ALTER TABLE trace.ad_events
ADD COLUMN device_type STRING DEFAULT NULL,  -- desktop, mobile, tablet
ADD COLUMN device_os STRING DEFAULT NULL,  -- ios, android, windows, macos, linux
ADD COLUMN device_browser STRING DEFAULT NULL;  -- chrome, firefox, safari, edge

-- Record migration
INSERT INTO trace.schema_migrations (version, description, applied_at, applied_by, checksum)
VALUES (
    2,
    'Add referrer tracking and attribution fields',
    CURRENT_TIMESTAMP,
    SESSION_USER(),
    'sha256:add_referrer_attribution_v1'
);

-- Verify columns added
SELECT
    COLUMN_NAME,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM trace.ad_events.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ad_events'
    AND COLUMN_NAME IN (
        'referrer', 'referrer_network',
        'attribution_campaign_id', 'attribution_creative_id',
        'attribution_touches', 'attribution_days_to_convert',
        'device_type', 'device_os', 'device_browser'
    )
ORDER BY ORDINAL_POSITION;

SELECT 'Migration V002 applied successfully - Added 9 new columns' AS status;
