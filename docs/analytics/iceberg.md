# Apache Iceberg Integration for TRACE

## Overview

Apache Iceberg provides an open table format for huge analytic datasets. For TRACE, Iceberg offers:

- **Schema evolution** - Add columns without rewriting data
- **Partition evolution** - Change partitioning without data migration
- **Time travel** - Query data as it was at any point in time
- **Hidden partitioning** - Query without knowing partition structure
- **ACID transactions** - Isolated concurrent writes

## When to Use Iceberg

### Use Iceberg if:
- Daily event volume > 10M events
- Multiple concurrent readers/writers
- Need frequent schema changes
- Require snapshot isolation for queries

### Stick with Parquet-only if:
- Daily event volume < 1M events
- Single writer workflow
- Simple query patterns
- Minimal infrastructure footprint

## Architecture

```
┌──────────────────────┐
│  TRACE Events        │
│  (Parquet on S3)     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Iceberg Catalog     │
│  (REST / Hive / Glue)│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Query Engines       │
│  Trino / DuckDB /    │
│  Spark / Athena      │
└──────────────────────┘
```

## Catalog Options

### 1. REST Catalog (Recommended)

Lightweight, cloud-agnostic catalog service.

**Deployment:**
```bash
docker run -d \
  -p 8181:8181 \
  -v iceberg-catalog-data:/data \
  tabulario/iceberg-rest:latest \
  catalog \
  --warehouse=s3://my-trace-bucket/iceberg \
  --config=rest-catalog-config.json
```

**Configuration:**
```yaml
warehouse: s3://my-trace-bucket/iceberg
catalog:
  type: rest
  uri: http://iceberg-catalog:8181
```

### 2. AWS Glue Catalog

Native AWS integration, works with Athena.

**Configuration:**
```yaml
catalog:
  type: glue
  uri: https://glue.us-east-1.amazonaws.com
  warehouse: s3://my-trace-bucket/iceberg
```

### 3. Hive Metastore

Traditional option, requires additional infrastructure.

**Configuration:**
```yaml
catalog:
  type: hive
  uri: thrift://hive-metastore:9083
  warehouse: s3://my-trace-bucket/iceberg
```

## Table Schema

### Events Table

```sql
CREATE TABLE trace.events (
  ts TIMESTAMP NOT NULL,
  ip STRING,
  ua STRING,
  url STRING NOT NULL,
  params MAP<STRING, STRING> NOT NULL,
  type STRING NOT NULL
)
PARTITIONED BY DAYS(ts)
LOCATED AT 's3://my-trace-bucket/iceberg/events';
```

### Partitioning Strategy

**Recommended:** Daily partitioning by `ts` timestamp
- Good balance between query performance and metadata size
- Aligns with natural data lifecycle

**Alternative:** Hourly partitioning (for very high volume)
```
PARTITIONED BY HOURS(ts)
```

## Migrating Existing Parquet Data

### Option 1: Register Existing Parquet (Fastest)

```sql
-- Register existing partitioned Parquet as Iceberg table
CALL trace.system.register_parquet_table(
  table => 'events',
  metadata_file => 's3://my-trace-bucket/trace-events/events/dt=2026-05-08/hour=14/metadata.json'
);
```

### Option 2: Rewrite to Iceberg Format (Recommended)

Run a migration job:

```bash
# From the compactor container
cargo run --bin migrate-to-iceberg \
  -- --source s3://my-trace-bucket/trace-events \
     --target s3://my-trace-bucket/iceberg/events \
     --catalog-uri http://iceberg-catalog:8181
```

## Querying with Trino

### Setup Trino with Iceberg

```bash
docker run -d \
  -p 8080:8080 \
  -v trino-catalog:/etc/trino/catalog \
  trinodb/trino:latest
```

**`catalog/iceberg.properties`:**
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-catalog:8181
iceberg.rest-catalog.warehouse=s3://my-trace-bucket/iceberg
```

### Sample Queries

```sql
-- CTR by campaign (auto-partition pruning)
SELECT
  params['utm_campaign'] AS campaign,
  COUNT(*) FILTER (WHERE type = 'pageview') AS views,
  COUNT(*) FILTER (WHERE type = 'click') AS clicks,
  (clicks::FLOAT / NULLIF(views, 0)) AS ctr
FROM trace.events
WHERE ts >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 4 DESC;

-- Time series of events
SELECT
  DATE_TRUNC('day', ts) AS day,
  type,
  COUNT(*) AS events
FROM trace.events
WHERE ts >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY 1, 2;

-- Asset performance by headline
SELECT
  params['tb_headline'] AS headline,
  COUNT(*) FILTER (WHERE type = 'click') AS clicks,
  COUNT(DISTINCT params['utm_campaign']) AS campaigns_used
FROM trace.events
WHERE params['tb_headline'] IS NOT NULL
  AND ts >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;
```

## Querying with DuckDB + Iceberg

DuckDB can read Iceberg metadata directly:

```sql
-- Load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Query via Iceberg REST catalog
SELECT * FROM iceberg_scan(
  's3://my-trace-bucket/iceberg/events',
  catalog_uri => 'http://iceberg-catalog:8181'
)
WHERE ts >= '2026-05-01'
LIMIT 100;
```

## Maintenance

### Snapshots

Iceberg maintains snapshots for time travel:

```sql
-- List snapshots
SELECT * FROM trace.events.snapshots;

-- Query as of specific snapshot
SELECT * FROM trace.events
FOR VERSION AS OF 1234567890
WHERE type = 'click';
```

### Expiring Old Snapshots

```sql
CALL trace.system.expire_snapshots(
  table => 'events',
  older_than => TIMESTAMP '2026-04-01 00:00:00'
);
```

### Rewrite Data Files

For optimizing small files (similar to compactor):

```sql
CALL trace.system.rewrite_data_files(
  table => 'events',
  options => MAP{
    'target-file-size-bytes', '536870912',
    'min-input-files', '10'
  }
);
```

## Compactor Integration

The TRACE compactor can write directly to Iceberg tables:

```rust
// After compaction, register with Iceberg
async fn register_with_iceberg(
    s3: &S3Client,
    iceberg_catalog: &IcebergCatalog,
    date: NaiveDate,
) -> Result<()> {
    iceberg_catalog
        .append_data(
            "trace.events",
            format!("s3://bucket/events-compacted/dt={}", date),
        )
        .await?;

    Ok(())
}
```

## Configuration

### Environment Variables

```bash
# Iceberg Catalog
ICEBERG_CATALOG_TYPE=rest
ICEBERG_CATALOG_URI=http://iceberg-catalog:8181
ICEBERG_WAREHOUSE=s3://my-trace-bucket/iceberg

# Or for Glue
ICEBERG_CATALOG_TYPE=glue
ICEBERG_CATALOG_REGION=us-east-1
ICEBERG_WAREHOUSE=s3://my-trace-bucket/iceberg
```

## Migration Path

1. **Phase 1:** Run with Parquet-only (current state)
2. **Phase 2:** Deploy Iceberg catalog alongside
3. **Phase 3:** Migrate historical data via compactor
4. **Phase 4:** Point all queries to Iceberg tables
5. **Phase 5:** Retire raw Parquet paths (keep as backup)
