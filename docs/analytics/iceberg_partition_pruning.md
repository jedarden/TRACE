# Iceberg Partition Pruning Optimization Guide

## Overview

Partition pruning is a critical optimization technique that allows query engines to skip reading irrelevant partitions based on query predicates. Iceberg's hidden partitioning makes this transparent to users while maintaining performance.

## How TRACE tables are actually laid out and read (verified 2026-09-16)

The DDL in this guide describes the *target* Iceberg state. The pipeline as
built (session materializer, compactor) writes **Hive-style partition
directories of ZSTD Parquet** — `started_at_day=YYYY-MM-DD/`,
`ts_day=YYYY-MM-DD/`, `network=<n>/type=<t>/` — one file per partition per
run. **No component writes Iceberg `metadata/*.metadata.json` today**, so
`iceberg_scan` on these paths fails outright (verified on DuckDB 1.5.2, even
with `unsafe_enable_version_guessing = true`): there is no version hint to
find. `iceberg_scan` becomes usable once a REST catalog populated with real
table metadata is deployed (`ICEBERG_CATALOG_URI` is plumbed but unpopulated).

Until then the DuckDB analytics layer reads the tables as
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

### Time-Based Queries (Automatic Partition Pruning)

```sql
-- ✅ GOOD: Uses partition pruning
SELECT COUNT(*) AS clicks
FROM trace.ad_events
WHERE ts >= '2026-05-01' AND ts < '2026-05-08';
-- Only scans partitions: ts_day=2026-05-01 through ts_day=2026-05-07

-- ❌ BAD: No partition pruning (date filter on extracted value)
SELECT COUNT(*) AS clicks
FROM trace.ad_events
WHERE DATE_TRUNC('day', ts) = '2026-05-01';
-- Scans all partitions, then filters

-- ✅ GOOD: Equivalent with partition pruning
SELECT COUNT(*) AS clicks
FROM trace.ad_events
WHERE ts >= '2026-05-01' AND ts < '2026-05-02';
```

### Sessions Table — verified against the live layout (2026-09-16)

Measured on the as-built `trace.sessions` layout (10 day partitions
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
-- ✅ GOOD against trace.sessions as built: partition column prunes files,
--    the timestamp range keeps row filtering independent of the partitioning
SELECT COUNT(*) AS sessions
FROM trace.sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06'
  AND started_at >= TIMESTAMP '2026-09-03 00:00:00'
  AND started_at <  TIMESTAMP '2026-09-06 00:00:00';

-- ❌ BAD against trace.sessions as built: timestamp-only range reads every
--    partition on the DuckDB read path (10/10 files measured). Acceptable
--    only against a true Iceberg table under Trino, where the day(started_at)
--    transform prunes from this predicate.
SELECT COUNT(*) AS sessions
FROM trace.sessions
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
FROM trace.ad_events
WHERE ts >= '2026-05-01'
  AND ts < '2026-05-08'
  AND network = 'taboola'
GROUP BY campaign_id;
-- Prunes by time partition first, then filters by network
```

## Query Patterns for Optimal Pruning

### 1. Always Use Date Ranges for Time Filters

```sql
-- ✅ GOOD: Closed-open interval
SELECT *
FROM trace.ad_events
WHERE ts >= '2026-05-01'
  AND ts < '2026-05-08';

-- ✅ GOOD: Using DATE_TRUNC with range
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
-- ✅ GOOD: Predicate pushed down
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
SELECT COUNT(*) FROM trace.sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06';

-- Look for the TABLE_SCAN node:
-- - "Total Files Read: 3"  (should match days/partitions in range;
--   with one file per day partition, files read == partitions scanned)
-- - "Filters:" — the predicates pushed into the scan
-- - "Filename(s):" — the glob the scan started from
```

`iceberg_scan` (and the `iceberg_*` views that wrap it) require a populated
Iceberg REST catalog; against the pipeline's current Parquet-only layout they
fail with a missing-version error — see "How TRACE tables are actually laid
out and read" above.

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
3. **Filter on partition columns** when possible
4. **Avoid functions on partition columns** in WHERE clauses
5. **Monitor query plans** to verify partition pruning is working
6. **Consider query patterns** when designing partitioning

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
