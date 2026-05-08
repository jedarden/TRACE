# Iceberg Schema Evolution and Backward Compatibility Guide

## Core Principles

### Safe Schema Changes (Always Backward Compatible)

1. **ADD COLUMN** with nullable or default value
   ```sql
   ALTER TABLE trace.ad_events ADD COLUMN referrer STRING DEFAULT NULL;
   ```

2. **ADD COLUMN** at any position (Iceberg handles this)
   ```sql
   ALTER TABLE trace.ad_events ADD COLUMN device_type STRING DEFAULT NULL;
   ```

3. **Column promotion** (widening types)
   ```sql
   ALTER TABLE trace.ad_events ALTER COLUMN campaign_id SET DATA TYPE STRING;
   ```

### Unsafe Changes (Break Backward Compatibility)

1. **DROP COLUMN** - Old readers expecting the column will fail
2. **RENAME COLUMN** - Breaks queries referencing the old name
3. **Narrowing types** - `bigint -> int` may truncate data
4. **Changing NULL constraints** - `NULL -> NOT NULL` breaks old data
5. **Changing partitioning** - Requires rewrite of existing data

## Recommended Practices

### Instead of DROP COLUMN: Deprecation Pattern

```sql
-- DON'T: ALTER TABLE trace.ad_events DROP COLUMN old_field;

-- DO: Mark as deprecated in documentation
-- Add comment to table:
COMMENT ON COLUMN trace.ad_events.old_field IS 'DEPRECATED: Use new_field instead. Will be removed in V6.0';
```

### Instead of RENAME: Alias Pattern

```sql
-- DON'T: ALTER TABLE trace.ad_events RENAME COLUMN ua TO user_agent;

-- DO: Create a view with the new name
CREATE OR REPLACE VIEW trace.ad_events_v2 AS
SELECT
    ts, ip, url, type,
    ua AS user_agent,  -- Alias for backward compatibility
    session_id, user_id, cookie_id,
    network, campaign_id, campaign_name,
    creative_id, headline, image_id, item_id,
    params
FROM trace.ad_events;
```

### Column Evolution Pattern

When a column needs to evolve, use a versioning strategy:

```sql
-- Phase 1: Add new column alongside old
ALTER TABLE trace.ad_events ADD COLUMN campaign_id_v2 STRING DEFAULT NULL;

-- Phase 2: Backfill data (run as separate job)
UPDATE trace.ad_events SET campaign_id_v2 = campaign_id WHERE ts < '2026-05-01';

-- Phase 3: Switch readers to new column
-- (Update application code gradually)

-- Phase 4: Deprecate old column (after verified)
COMMENT ON COLUMN trace.ad_events.campaign_id IS 'DEPRECATED: Use campaign_id_v2. Removal in V7.0';
```

## Query Patterns for Backward Compatibility

### Handling Missing Columns in Views

```sql
-- Create views that handle multiple schema versions
CREATE OR REPLACE VIEW trace.ad_events_unified AS
SELECT
    ts, ip, ua, url, type,
    session_id, user_id, cookie_id,

    -- Use COALESCE for versioned columns
    COALESCE(campaign_id_v2, campaign_id) AS campaign_id,

    -- Handle optional engagement metrics (V003+)
    scroll_depth_pct,
    dwell_time_ms,

    -- Handle optional quality scores (V004+)
    COALESCE(quality_score, 1.0) AS quality_score,  -- Default to valid if not scored

    network, campaign_name, creative_id, headline, image_id, item_id,
    params
FROM trace.ad_events;
```

### Time Travel Queries with Schema Evolution

```sql
-- Query data as it was, handling schema differences
SELECT
    ts,
    type,
    campaign_id,
    -- V002+ columns will be NULL for older snapshots
    referrer
FROM trace.ad_events
FOR VERSION AS OF 1234567890  -- Snapshot from before V002
WHERE ts >= '2026-04-01';

-- Iceberg returns NULL for columns that didn't exist at that snapshot
```

## Testing Schema Evolution

### Before Applying Migrations

```sql
-- 1. Test migration on staging table
CREATE TABLE trace.ad_events_staging AS SELECT * FROM trace.ad_events LIMIT 0;

-- 2. Apply migration to staging
ALTER TABLE trace.ad_events_staging ADD COLUMN new_field STRING DEFAULT NULL;

-- 3. Verify queries still work
SELECT COUNT(*) FROM trace.ad_events_staging;

-- 4. Test with real data sample
INSERT INTO trace.ad_events_staging
SELECT *, NULL AS new_field
FROM trace.ad_events
LIMIT 1000;

-- 5. Verify queries work with both NULL and populated values
SELECT COUNT(*) FROM trace.ad_events_staging WHERE new_field IS NULL;
SELECT COUNT(*) FROM trace.ad_events_staging WHERE new_field IS NOT NULL;
```

### Validation Queries

```sql
-- Check for NULL values in new columns (expected for old data)
SELECT
    COUNT(*) AS total_rows,
    COUNT(referrer) AS rows_with_referrer,
    COUNT(scroll_depth_pct) AS rows_with_scroll_data,
    COUNT(quality_score) AS rows_with_quality_score
FROM trace.ad_events
WHERE ts >= CURRENT_DATE - INTERVAL '7' DAY;

-- Verify column defaults are working
SELECT
    COLUMN_NAME,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'ad_events'
ORDER BY ORDINAL_POSITION;
```

## Rollback Strategy

### Rollback Plan for Each Migration

```sql
-- Migration V002 rollback (if needed)
-- Note: Iceberg doesn't support DROP COLUMN in all engines
-- Instead, rely on snapshot time travel

-- 1. Identify snapshot before migration
SELECT snapshot_id, committed_at
FROM trace.ad_events.snapshots
WHERE summary['migration'] = 'V002'
ORDER BY committed_at DESC
LIMIT 1;

-- 2. Query from previous snapshot
SELECT * FROM trace.ad_events FOR VERSION AS OF <previous_snapshot_id>;

-- 3. Create rollback view if needed
CREATE OR REPLACE VIEW trace.ad_events_pre_v002 AS
SELECT * FROM trace.ad_events FOR VERSION AS OF <previous_snapshot_id>;
```

## Monitoring Schema Evolution

### Track Column Usage

```sql
-- Monitor which columns are actually used
CREATE TABLE trace.column_usage_log (
    query_timestamp TIMESTAMP,
    table_name STRING,
    columns_used ARRAY<STRING>,
    schema_version INT
);

-- Query to find unused columns (potential deprecation candidates)
SELECT
    COLUMN_NAME,
    COUNT(*) AS reference_count
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'trace'
    AND VIEW_DEFINITION LIKE CONCAT('%', COLUMN_NAME, '%')
GROUP BY COLUMN_NAME
ORDER BY reference_count ASC;
```

### Schema Compatibility Matrix

| Schema Version | Columns Added | Compatible Readers | Migration Required |
|----------------|---------------|-------------------|-------------------|
| V1 (base) | Initial schema | V1+ | No |
| V2 | referrer, attribution, device | V2+ | Optional for referrer data |
| V3 | engagement metrics | V3+ | Optional for engagement |
| V4 | quality scores | V4+ | Optional for quality filtering |

## Emergency Procedures

### If Migration Fails Mid-Transaction

Iceberg transactions are atomic - either all changes apply or none do. If a migration fails:

1. **Check table status**
   ```sql
   SELECT * FROM trace.ad_events.snapshots ORDER BY committed_at DESC LIMIT 5;
   ```

2. **Verify current schema**
   ```sql
   SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ad_events';
   ```

3. **Roll back to previous snapshot if needed**
   ```sql
   CALL trace.system.rollback_to_snapshot(
       table => 'ad_events',
       snapshot_id => <previous_snapshot_id>
   );
   ```

### If Readers Break After Migration

1. **Use time travel for immediate fix**
   ```sql
   CREATE OR REPLACE VIEW trace.ad_events_safe AS
   SELECT * FROM trace.ad_events FOR VERSION AS OF <known_good_snapshot>;
   ```

2. **Investigate breaking change**
   - Check which queries are failing
   - Identify incompatible schema change
   - Create compatibility view

3. **Fix and redeploy readers**
   - Update queries to handle new schema
   - Test thoroughly
   - Roll out new readers
   - Remove compatibility view
