# Iceberg Partition Pruning Optimization Guide

## Overview

Partition pruning is a critical optimization technique that allows query engines to skip reading irrelevant partitions based on query predicates. Iceberg's hidden partitioning makes this transparent to users while maintaining performance.

## How TRACE tables are actually laid out and read (verified 2026-09-16)

The pipeline writes **Hive-style partition directories of ZSTD Parquet** —
`started_at_day=YYYY-MM-DD/`, `ts_day=YYYY-MM-DD/`,
`network=<n>/type=<t>/` — with partition values in the path. A directory tree
of Parquet files is not itself an Iceberg table: `iceberg_scan` requires the
table metadata registered in the REST catalog. The analytics service uses a
catalog table with `iceberg_scan` when its Iceberg catalog is enabled; in
Parquet mode it reads the compacted data with
`read_parquet('<glob>', hive_partitioning = true)`, which prunes at exactly
one level:

- **Filtering the Hive partition column** (`started_at_day`, `ts_day`,
  `network`, …) prunes whole directories. Verified below.
- **Filtering an in-file column only** (`started_at`, `ts`) does **not** skip
  files on the DuckDB read path, even when every file's Parquet min/max
  excludes the range — measured `Total Files Read` equals the unfiltered
  baseline. Range filters on the timestamp are still correct and still filter
  rows; they just do not save I/O here. An engine reading a *true* Iceberg
  table (Trino with the `day(started_at)` partition transform) would prune
  from the range predicate alone.

So the practical rule against the as-built layout: **filter the partition
column in addition to the timestamp.** The verified sessions examples below
show both shapes.

## TRACE Partitioning Strategy

### Ad Events Table

```sql
-- Partitioned by day of timestamp
PARTITIONED BY DAYS(ts)
-- Physical partition format: ts_day=YYYY-MM-DD
```

### Sessions Table

```sql
-- Partitioned by day of session start
PARTITIONED BY DAYS(started_at)
-- Physical partition format: started_at_day=YYYY-MM-DD
```

### Assets Table

```sql
-- Partitioned by network and asset type
PARTITIONED BY (network, type)
-- Physical partition format: network=taboola/type=headline/
```

## Partition Pruning Examples

### Event Time Queries

```sql
-- Parquet mode: hive_partitioning exposes the physical ts_day directory key.
SELECT COUNT(*) AS clicks
FROM read_parquet(
    's3://my-trace-bucket/trace-events/iceberg/ad_events/data/**/*.parquet',
    hive_partitioning = true
)
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-08'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';
-- Only reads the ts_day=2026-05-01 through ts_day=2026-05-07 directories.

-- True Iceberg mode: its hidden day(ts) transform prunes from ts directly.
SELECT COUNT(*) AS clicks
FROM trace.ad_events
WHERE ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';
```

### Sessions Table — verified against the live layout (2026-09-16)

Measured on the as-built sessions Parquet view (10 day partitions
`started_at_day=2026-09-01…2026-09-10`, one ZSTD Parquet file per day,
240 rows/day; 138 MB / 20 M-row variant confirmed the same file counts),
DuckDB 1.5.2, `read_parquet('…/iceberg/sessions/data/**/*.parquet',
hive_partitioning = true)`. "Files read" is `TABLE_SCAN → Total Files Read`
from `EXPLAIN ANALYZE`; with one file per day it equals partitions scanned.

| Query | Files read | Rows out |
|---|---|---|
| `started_at >= '2026-09-03' AND started_at < '2026-09-06'` | **10 / 10** ✗ no pruning | 720 |
| `DATE_TRUNC('day', started_at) = DATE '2026-09-03'` | 10 / 10 ✗ | 240 |
| `CAST(started_at AS DATE) = DATE '2026-09-03'` | 10 / 10 ✗ | 240 |
| `started_at_day = DATE '2026-09-03'` | **1 / 10** ✓ | 240 |
| `started_at_day >= DATE '2026-09-03' AND started_at_day < DATE '2026-09-06'` | **3 / 10** ✓ | 720 |
| `started_at` range **AND** `started_at_day` range | **3 / 10** ✓ | 720 |
| no filter (baseline) | 10 / 10 | 2400 |

Readings:

- The timestamp-only range is **correct** (720 = exactly the rows in
  `[09-03, 09-06)`) but scans every file: DuckDB opens each file's footer and
  reads it, even though each file's `started_at` min/max is confined to a
  single day (`parquet_metadata` confirms `[day 00:00:00 .. day 23:54:00]`).
  The DATE_TRUNC and CAST forms behave the same at the file level while also
  evaluating an expression per row — still avoid them.
- Filtering the **partition column** (`started_at_day`, exposed by
  `hive_partitioning = true` and auto-cast to DATE) prunes exactly: equality
  → 1 file, 3-day range → 3 files.
- The belt-and-braces pattern (timestamp range for row filtering **plus**
  partition-column range for file pruning) is the recommended sessions query:

```sql
-- ✅ GOOD against parquet_sessions: partition column prunes files,
--    the timestamp range keeps row filtering independent of the partitioning
SELECT COUNT(*) AS sessions
FROM parquet_sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06'
  AND started_at >= TIMESTAMP '2026-09-03 00:00:00'
  AND started_at <  TIMESTAMP '2026-09-06 00:00:00';

-- ❌ BAD against parquet_sessions: timestamp-only range reads every
--    partition on the DuckDB read path (10/10 files measured). Acceptable
--    only against a true Iceberg table under Trino, where the day(started_at)
--    transform prunes from this predicate.
SELECT COUNT(*) AS sessions
FROM parquet_sessions
WHERE started_at >= '2026-09-03' AND started_at < '2026-09-06';
```

### Network-Based Queries on Assets Table

```sql
-- ✅ GOOD: Uses partition pruning on network
SELECT * FROM trace.assets
WHERE network = 'taboola';
-- Only scans: network=taboola/*

-- ✅ GOOD: Uses partition pruning on both network and type
SELECT * FROM trace.assets
WHERE network = 'taboola' AND type = 'headline';
-- Only scans: network=taboola/type=headline/

-- ❌ BAD: Network filter with OR may not prune efficiently
SELECT * FROM trace.assets
WHERE network = 'taboola' OR network = 'outbrain';
-- May scan multiple partitions
```

### Combining Multiple Predicates

```sql
-- ✅ GOOD: Time + network filtering
SELECT
    campaign_id,
    COUNT(*) AS clicks
FROM read_parquet(
    's3://my-trace-bucket/trace-events/iceberg/ad_events/data/**/*.parquet',
    hive_partitioning = true
)
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-08'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00'
  AND network = 'taboola'
GROUP BY campaign_id;
-- Prunes by ts_day, then filters by network.
```

## Query Patterns for Optimal Pruning

### 1. Always Use Date Ranges for Time Filters

```sql
-- True Iceberg table: timestamp ranges prune the hidden day(ts) transform.
SELECT *
FROM trace.ad_events
WHERE ts >= '2026-05-01'
  AND ts < '2026-05-08';

-- Current Parquet layout: include ts_day to skip files.
SELECT *
FROM read_parquet(
    's3://my-trace-bucket/trace-events/iceberg/ad_events/data/**/*.parquet',
    hive_partitioning = true
)
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-08'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';

-- On Iceberg, use a closed-open timestamp range rather than DATE_TRUNC.
SELECT *
FROM trace.ad_events
WHERE ts >= DATE_TRUNC('day', CURRENT_DATE + INTERVAL '-7 days')
  AND ts < DATE_TRUNC('day', CURRENT_DATE);

-- ❌ AVOID: DATE equality on timestamp
SELECT *
FROM trace.ad_events
WHERE DATE(ts) = '2026-05-01';
```

### 2. Filter on Partition Columns Directly

```sql
-- For assets table with network partitioning
-- ✅ GOOD: Direct network filter
SELECT * FROM trace.assets
WHERE network = 'taboola';

-- ✅ GOOD: Network IN list (still prunes)
SELECT * FROM trace.assets
WHERE network IN ('taboola', 'outbrain');
```

### 3. Use Subqueries to Push Down Predicates

```sql
-- True Iceberg table: the timestamp predicate prunes the hidden transform.
WITH recent_campaigns AS (
    SELECT DISTINCT campaign_id
    FROM trace.ad_events
    WHERE ts >= CURRENT_DATE + INTERVAL '-7 days'
      AND network = 'taboola'
)
SELECT
    c.campaign_id,
    c.campaign_name,
    COUNT(*) AS total_clicks
FROM recent_campaigns c
JOIN trace.ad_events e
    ON c.campaign_id = e.campaign_id
WHERE e.ts >= CURRENT_DATE + INTERVAL '-7 days'
  AND e.network = 'taboola'
GROUP BY 1, 2;
```

## Monitoring Partition Pruning

### Check Query Plan (Trino)

```sql
-- Show which partitions will be scanned
EXPLAIN
SELECT COUNT(*) FROM trace.ad_events
WHERE ts >= '2026-05-01' AND ts < '2026-05-08';

-- Look for:
-- - "Filter by partition values" in the plan
-- - Number of partitions scanned vs total
```

### Check Query Plan (DuckDB)

Verified against DuckDB 1.5.2 (2026-09-16):

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM parquet_sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06';

-- Look for the TABLE_SCAN node:
-- - "Total Files Read: 3"  (should match days/partitions in range;
--   with one file per day partition, files read == partitions scanned)
-- - "Filters:" — the predicates pushed into the scan
-- - "Filename(s):" — the glob the scan started from
```

`iceberg_scan` (and the `iceberg_*` views that wrap it) require a populated
Iceberg REST catalog. A bare Parquet directory without catalog metadata fails
with a missing-version error; use the registered Parquet views and their Hive
partition columns when the catalog is disabled.

## Partition Evolution

Iceberg allows changing partitioning without rewriting data:

```sql
-- Add hour partitioning (from daily to hourly)
ALTER TABLE trace.ad_events
SET PARTITION SPEC (
    ts,
    bucket(16, network)  -- Add network bucketing
);
```

## Best Practices

1. **Always filter on timestamp** for time-series queries
2. **Use closed-open intervals** for time ranges (`>=` start AND `<` end)
3. **On Parquet, also filter the physical key** (`ts_day` or `started_at_day`)
4. **On Iceberg, use the timestamp range** to prune hidden day transforms
5. **Avoid functions on partition columns** in WHERE clauses
6. **Monitor query plans** to verify partition pruning is working

## Performance Impact

| Scenario | Without Pruning | With Pruning | Improvement |
|----------|----------------|--------------|-------------|
| 7-day query on 1-year data | Scans 365 partitions | Scans 7 partitions | 52x faster |
| Single network query | Scans all network partitions | Scans 1 partition | 5x faster (5 networks) |
| Recent data query (7 days) | Scans entire table | Scans 7 partitions | 100x+ faster |

## Tools for Partition Analysis

```sql
-- Show partition sizes (Trino)
SELECT
    partition,
    COUNT(*) AS file_count,
    SUM(size) AS total_bytes
FROM iceberg.metadata.table_partitions
WHERE table_name = 'ad_events'
GROUP BY partition
ORDER BY partition DESC;

-- Show partition distribution (DuckDB)
-- (Requires custom query against Iceberg metadata)
```

## Troubleshooting

### Issue: Query scans all partitions despite date filter

**Cause**: Using `DATE(ts) = '2026-05-01'` instead of range

**Fix**:
```sql
-- Instead of:
WHERE DATE(ts) = '2026-05-01'

-- Use:
WHERE ts >= '2026-05-01' AND ts < '2026-05-02'
```

### Issue: OR conditions prevent pruning

**Cause**: `WHERE network = 'taboola' OR network = 'outbrain'`

**Fix**: Use UNION ALL or IN clause
```sql
-- Use IN:
WHERE network IN ('taboola', 'outbrain')

-- Or UNION ALL for better control:
SELECT * FROM trace.ad_events WHERE network = 'taboola'
UNION ALL
SELECT * FROM trace.ad_events WHERE network = 'outbrain'
```

## Additional Resources

- [Iceberg Partitioning Docs](https://iceberg.apache.org/spec/#partitioning)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)
