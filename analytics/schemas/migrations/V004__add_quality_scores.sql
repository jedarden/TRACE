-- ============================================================================
-- Migration V004: Add Quality Scores and Fraud Detection
-- ============================================================================
-- This migration adds quality scoring fields for ad traffic analysis.
--
-- Backward Compatibility: SAFE
-- - New columns are nullable with default NULL
-- - Scores can be computed retroactively
-- - Existing queries see NULL until scoring is run
-- ============================================================================

-- Add quality scores
ALTER TABLE trace.ad_events
ADD COLUMN quality_score DOUBLE DEFAULT NULL,  -- Overall quality score (0.0-1.0)
ADD COLUMN bot_probability DOUBLE DEFAULT NULL,  -- Probability of bot traffic (0.0-1.0)
ADD COLUMN fraud_score DOUBLE DEFAULT NULL;  -- Fraud probability score (0.0-1.0)

-- Add validation flags
ALTER TABLE trace.ad_events
ADD COLUMN is_valid BOOLEAN DEFAULT NULL,  -- Passed basic validation
ADD COLUMN is_verified BOOLEAN DEFAULT NULL,  -- Passed deep verification
ADD COLUMN validation_reason STRING DEFAULT NULL;  -- Reason if validation failed

-- Add enrichment metadata
ALTER TABLE trace.ad_events
ADD COLUMN enriched_at TIMESTAMP DEFAULT NULL,  -- When enrichment was applied
ADD COLUMN enrichment_version STRING DEFAULT NULL;  -- Version of enrichment pipeline

-- Record migration
INSERT INTO trace.schema_migrations (version, description, applied_at, applied_by, checksum)
VALUES (
    4,
    'Add quality scores and fraud detection fields',
    CURRENT_TIMESTAMP,
    SESSION_USER(),
    'sha256:add_quality_scores_v1'
);

-- Verify columns added
SELECT
    COLUMN_NAME,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM trace.ad_events.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ad_events'
    AND COLUMN_NAME IN (
        'quality_score', 'bot_probability', 'fraud_score',
        'is_valid', 'is_verified', 'validation_reason',
        'enriched_at', 'enrichment_version'
    )
ORDER BY ORDINAL_POSITION;

SELECT 'Migration V004 applied successfully - Added 8 quality/fraud columns' AS status;
