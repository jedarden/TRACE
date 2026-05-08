-- ============================================================================
-- Migration V001: Initial Schema Setup
-- ============================================================================
-- Creates the schema version tracking table
-- ============================================================================

-- Create schema migrations tracking table
CREATE TABLE IF NOT EXISTS trace.schema_migrations (
    version INT NOT NULL,
    description STRING NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    applied_by STRING,
    checksum STRING,
    PRIMARY KEY (version)
);

-- Record this migration
INSERT INTO trace.schema_migrations (version, description, applied_at, applied_by, checksum)
VALUES (
    1,
    'Initial schema setup with ad_events, campaigns, and creatives tables',
    CURRENT_TIMESTAMP,
    SESSION_USER(),
    'sha256:initial_schema_v1'
);

-- Verify tables exist
SELECT 'Migration V001 applied successfully' AS status;
