# Iceberg Schema Migrations

This directory contains schema migration scripts for TRACE Iceberg tables.

## Migration Philosophy

Iceberg supports schema evolution, but careful planning is needed for backward compatibility:

1. **Additive changes only** - Adding columns is always safe
2. **Column promotion** - `int -> bigint` is safe, narrowing is not
3. **Never drop columns** - Mark as deprecated instead
4. **Never rename columns** - Use aliases in views
5. **Always add defaults** - New columns must have defaults for old data

## Migration Scripts

Scripts are numbered sequentially: `V001__description.sql`

```sql
-- Example migration
ALTER TABLE trace.ad_events
ADD COLUMN referrer STRING DEFAULT NULL;
```

## Running Migrations

```bash
# Check migration status
python analytics/schemas/migrations/migrate.py --status

# Preview pending migrations
python analytics/schemas/migrations/migrate.py --dry-run

# Apply all pending migrations
python analytics/schemas/migrations/migrate.py --apply-all

# Apply specific migration
python analytics/schemas/migrations/migrate.py --apply V002

# Rollback info
python analytics/schemas/migrations/migrate.py --rollback V002
```

## Schema Version Table

Track applied migrations:

```sql
CREATE TABLE IF NOT EXISTS trace.schema_migrations (
    version INT NOT NULL,
    description STRING NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    applied_by STRING,
    checksum STRING,
    PRIMARY KEY (version)
);
```

## Available Migrations

| Version | Description | Columns Added | Safe to Apply |
|---------|-------------|---------------|---------------|
| V001 | Initial schema setup | Schema tracking table | Yes |
| V002 | Referrer & attribution | 9 columns (referrer, attribution, device) | Yes |
| V003 | Engagement metrics | 6 columns (scroll, dwell, viewport) | Yes |
| V004 | Quality scores | 8 columns (quality, fraud, validation) | Yes |
