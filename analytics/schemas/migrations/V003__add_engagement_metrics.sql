-- ============================================================================
-- Migration V003: Add Engagement Metrics
-- ============================================================================
-- This migration adds detailed engagement metrics for scroll and dwell events.
--
-- Backward Compatibility: SAFE
-- - New columns are nullable
-- - Only relevant for specific event types (scroll, dwell)
-- - Existing queries unaffected
-- ============================================================================

-- Add scroll-specific metrics
ALTER TABLE trace.ad_events
ADD COLUMN scroll_depth_pct INT DEFAULT NULL,  -- Percentage of page scrolled (0-100)
ADD COLUMN scroll_time_ms INT DEFAULT NULL;  -- Time spent scrolling in milliseconds

-- Add dwell-specific metrics
ALTER TABLE trace.ad_events
ADD COLUMN dwell_time_ms INT DEFAULT NULL,  -- Total dwell time in milliseconds
ADD COLUMN dwell_visible_pct INT DEFAULT NULL,  -- Percentage of time ad was visible (0-100)

-- Add viewport metrics for all events
ALTER TABLE trace.ad_events
ADD COLUMN viewport_width INT DEFAULT NULL,  -- Viewport width in pixels
ADD COLUMN viewport_height INT DEFAULT NULL;  -- Viewport height in pixels

-- Record migration
INSERT INTO trace.schema_migrations (version, description, applied_at, applied_by, checksum)
VALUES (
    3,
    'Add engagement metrics for scroll and dwell events',
    CURRENT_TIMESTAMP,
    SESSION_USER(),
    'sha256:add_engagement_metrics_v1'
);

-- Verify columns added
SELECT
    COLUMN_NAME,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM trace.ad_events.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ad_events'
    AND COLUMN_NAME IN (
        'scroll_depth_pct', 'scroll_time_ms',
        'dwell_time_ms', 'dwell_visible_pct',
        'viewport_width', 'viewport_height'
    )
ORDER BY ORDINAL_POSITION;

SELECT 'Migration V003 applied successfully - Added 6 engagement columns' AS status;
