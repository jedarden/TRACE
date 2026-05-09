# DuckDB Analytics Query Layer over Iceberg Tables

## Summary

Implemented a complete DuckDB analytics query layer that supports querying both Iceberg tables and legacy Parquet files. The implementation provides automatic backend selection based on configuration, making it seamless to migrate from Parquet to Iceberg without changing query code.

## Changes Made

### 1. Fixed DuckDB Iceberg Extension Syntax (`analytics/src/duckdb.rs`)

**Problem**: The original `setup_iceberg_views()` method used incorrect syntax for DuckDB's `iceberg_scan()` function.

**Solution**: Updated to use the correct syntax:
```sql
CREATE OR REPLACE VIEW iceberg_ad_events AS
SELECT * FROM iceberg_scan('s3://bucket/iceberg/ad_events', catalog_uri => 'http://catalog:8181');
```

**Changes**:
- Fixed `iceberg_scan()` function call with proper named parameter syntax
- Added `setup_parquet_views()` method for backward compatibility
- Modified `DuckDBClient::new()` to automatically set up appropriate views based on configuration
- Added `campaigns_table_sql()` and `creatives_table_sql()` helper methods for dimension table queries

### 2. Iceberg-Aware Query Rendering (`analytics/src/queries.rs`)

**Problem**: Query templates needed to support both Parquet file paths and Iceberg table references.

**Solution**: Added `render_template_with_client()` function that:
- Uses `{{events_table}}` placeholder that gets replaced with appropriate view name
- Automatically detects if Iceberg is enabled and uses correct table references
- Maintains backward compatibility with legacy `{{s3_path}}` templates

**Changes**:
- Added `render_template_with_client()` function for Iceberg-aware rendering
- Updated all report definitions to mark `supports_iceberg: true`
- Kept legacy `render_template()` for backward compatibility

### 3. Updated Report Execution (`analytics/src/reporter.rs`)

**Problem**: Report runner needed to pass configuration for Iceberg-aware rendering.

**Solution**: Updated `run_report()` to:
- Accept `config: &Config` parameter
- Use Iceberg-aware rendering when enabled
- Log which backend (Iceberg or Parquet) is being used

### 4. Added Comprehensive Unit Tests

**Added tests for**:
- `duckdb.rs`: JSON/CSV escaping, query result formatting
- `config.rs`: Iceberg enablement detection, S3 path generation
- `queries.rs`: Report listing, template rendering, category validation

## Configuration

### Environment Variables

To enable Iceberg support, set the following environment variables:

```bash
# Required for Iceberg
export ICEBERG_CATALOG_URI=http://iceberg-catalog:8181
export ICEBERG_WAREHOUSE=s3://my-trace-bucket/iceberg

# Standard S3 configuration (works for both Parquet and Iceberg)
export TRACE_S3_BUCKET=my-trace-bucket
export TRACE_S3_REGION=us-east-1
export TRACE_S3_PREFIX=trace-events
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Usage

The analytics service automatically detects whether to use Iceberg or Parquet:

```bash
# Run any report - automatically uses Iceberg if configured
trace-analytics run daily_summary --format json

# List all available reports
trace-analytics list

# Run scheduled reports in daemon mode
trace-analytics schedule --interval 86400
```

## Architecture

### View Setup

**When Iceberg is enabled**:
1. `iceberg_ad_events` view → queries Iceberg `ad_events` table
2. `iceberg_campaigns` view → queries Iceberg `campaigns` dimension table
3. `iceberg_creatives` view → queries Iceberg `creatives` dimension table

**When Iceberg is disabled** (legacy mode):
1. `parquet_events` view → reads from S3 Parquet files
2. `parquet_events_compacted` view → reads from compacted Parquet files
3. Dimension data is extracted from events via subqueries

### Query Flow

```
┌─────────────────┐
│   Report SQL    │
│  (template)     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ render_template_with_    │
│ client()                 │
│ - Detects backend type   │
│ - Substitutes table refs│
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ DuckDBClient            │
│ - Uses appropriate view │
│ - Executes query        │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Results (JSON/CSV)      │
└─────────────────────────┘
```

## DuckDB Iceberg Extension Notes

### Extension Installation

The DuckDB Iceberg extension is automatically installed and loaded when `ICEBERG_CATALOG_URI` is configured:

```sql
INSTALL iceberg;
LOAD iceberg;
```

### Catalog Connection

DuckDB connects to the Iceberg REST catalog using the `catalog_uri` parameter:

```sql
SELECT * FROM iceberg_scan(
    's3://bucket/iceberg/ad_events',
    catalog_uri => 'http://catalog:8181'
);
```

### Partition Pruning

Iceberg tables support automatic partition pruning. DuckDB leverages this for efficient time-based queries:

```sql
-- This query automatically prunes partitions
SELECT * FROM iceberg_ad_events
WHERE ts >= '2026-05-01' AND ts < '2026-05-08';
```

## Migration Path

### From Parquet to Iceberg

1. **Deploy Iceberg catalog** alongside existing Parquet storage
2. **Migrate historical data** using the compactor or migration scripts
3. **Set environment variables** to enable Iceberg
4. **Run reports** - they automatically use Iceberg tables
5. **Retire Parquet paths** once migration is verified

### No Code Changes Required

Existing query templates work with both backends:
- `{{s3_path}}` - Legacy placeholder for Parquet paths
- `{{events_table}}` - New placeholder for view-based queries

## Performance Considerations

### Iceberg Benefits

1. **Partition pruning** - Only relevant partitions are scanned
2. **Schema evolution** - Add columns without rewriting data
3. **Time travel** - Query data as it was at any point
4. **Hidden partitioning** - Query without knowing partition structure

### Query Optimization

- Use date filters on `ts` column for partition pruning
- Filter on `network`, `campaign_id` for better performance
- Use materialized views for complex aggregations

## Testing

Run unit tests with:

```bash
cargo test -p trace-analytics
```

Test coverage includes:
- Configuration parsing and validation
- Template rendering with various parameter combinations
- Query result formatting (JSON/CSV)
- Iceberg enablement detection
- S3 path generation

## Future Enhancements

1. **Materialized views** - Cache frequently accessed aggregations
2. **Query result caching** - Cache results for identical queries
3. **Metrics collection** - Track query performance and resource usage
4. **Async query execution** - Support for long-running queries
5. **Dynamic schema discovery** - Auto-detect available columns and tables
