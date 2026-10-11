# Iceberg Partition Pruning Optimization Guide

## Overview

Partition pruning is a critical optimization technique that allows query engines to skip reading irrelevant partitions based on query predicates. Iceberg's hidden partitioning makes this transparent to users while maintaining performance.

## How TRACE tables are intended to be laid out and read (live status checked 2026-09-30)

The repository's materializer writes **Hive-style partition directories of
ZSTD Parquet** —
`started_at_day=YYYY-MM-DD/`, `ts_day=YYYY-MM-DD/`,
`network=<n>/type=<t>/` — with partition values in the path. A directory tree
of Parquet files is not itself an Iceberg table: `iceberg_scan` requires the
table metadata registered in the REST catalog. The analytics service uses a
catalog table with `iceberg_scan` when its Iceberg catalog is enabled; in
Parquet mode it reads the compacted data with
`read_parquet('<glob>', hive_partitioning = true)`, which prunes at exactly
one level:

- **Filtering the Hive partition column** (`started_at_day`, `ts_day`,
  `network`, …) prunes whole directories. The sessions result below is a
  local Parquet-fixture validation, not a live-table measurement.
- **Filtering an in-file column only** (`started_at`, `ts`) does **not** skip
  files on the DuckDB read path, even when every file's Parquet min/max
  excludes the range — measured `Total Files Read` equals the unfiltered
  baseline. Range filters on the timestamp are still correct and still filter
  rows; they just do not save I/O here. An engine reading a *true* Iceberg
  table (Trino with the `day(started_at)` partition transform) would prune
  from the range predicate alone.

So the practical rule against the as-built layout is: **filter the partition
column in addition to the timestamp.** The sessions examples below show both
shapes and identify which results came from the local fixture.

In Parquet mode, the analytics service registers `parquet_ad_events` over
`s3://<TRACE_S3_BUCKET>/<TRACE_S3_PREFIX>/iceberg/ad_events/data/**/*.parquet`
and `parquet_sessions` over
`s3://<TRACE_S3_BUCKET>/<TRACE_S3_PREFIX>/iceberg/sessions/data/**/*.parquet`,
both with `hive_partitioning = true`. Their wildcard projections retain the
directory keys `ts_day` and `started_at_day` for pruning. In catalog mode, the
service registers `iceberg_ad_events` and `iceberg_sessions`; those views read
the Iceberg tables and use timestamp predicates to prune hidden day
transforms. The opt-in mixed-generation `parquet_events` and
`parquet_events_compacted` compatibility views derive `dt` from `ts`, so they
are unpruned even though their `dt` value can be filtered.

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
-- Parquet mode: parquet_ad_events exposes the physical ts_day directory key.
SELECT COUNT(*) AS clicks
FROM parquet_ad_events
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-08'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';
-- Only reads the ts_day=2026-05-01 through ts_day=2026-05-07 directories.

-- Catalog mode: iceberg_ad_events prunes its hidden day(ts) transform from ts.
SELECT COUNT(*) AS clicks
FROM iceberg_ad_events
WHERE ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';
```

### Sessions Table — Parquet-mode fixture validation (live table unavailable)

The live catalog was checked read-only on 2026-09-30 before this section was
updated. `iceberg_namespaces` contained one namespace (`default/native_ads`),
but `iceberg_tables` contained zero rows and
`WHERE table_name = 'sessions'` returned no row. There was therefore no live
`trace.sessions` metadata, partition list, or warehouse data against which to
run either predicate. The figures below are retained as a reproducible local
fixture validation of the repository's intended Parquet layout; they are not
production counts.

Measured on a local as-built sessions Parquet fixture (10 day partitions
`started_at_day=2026-09-01…2026-09-10`, one ZSTD Parquet file per day,
240 rows/day; the 138 MB / 20 M-row fixture variant confirmed the same file
counts), DuckDB 1.5.2, `parquet_sessions`, the service view over
`iceberg/sessions/data/**/*.parquet` with `hive_partitioning = true`.
"Files read" is `TABLE_SCAN → Total Files Read` from `EXPLAIN ANALYZE`; with
one file per day it equals partitions scanned.

| Validation scope | Total partitions | In-range partitions | GOOD scanned | BAD scanned | Conclusion |
|---|---:|---:|---:|---:|---|
| Local Parquet fixture, `[2026-09-03, 2026-09-06)` | 10 | 3 | 3 / 10 | 10 / 10 | The physical `started_at_day` predicate prunes; the timestamp-only predicate does not. |
| Live `trace.sessions` catalog table | N/A | N/A | N/A | N/A | No table was registered when checked, so live validation is unavailable. |

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

- On the local fixture, the timestamp-only range is **correct** (720 = exactly
  the rows in `[09-03, 09-06)`) but scans every file: DuckDB opens each file's footer and
  reads it, even though each file's `started_at` min/max is confined to a
  single day (`parquet_metadata` confirms `[day 00:00:00 .. day 23:54:00]`).
  The DATE_TRUNC and CAST forms behave the same at the file level while also
  evaluating an expression per row — still avoid them.
- On the local fixture, filtering the **partition column** (`started_at_day`,
  exposed by `hive_partitioning = true` and auto-cast to DATE) prunes exactly:
  equality → 1 file, 3-day range → 3 files.
- The belt-and-braces pattern (timestamp range for row filtering **plus**
  partition-column range for file pruning) is the recommended sessions query:

```sql
-- ✅ GOOD in Parquet mode (validated on the local fixture): the partition
--    column prunes files,
--    the timestamp range keeps row filtering independent of the partitioning
SELECT COUNT(*) AS sessions
FROM parquet_sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06'
  AND started_at >= TIMESTAMP '2026-09-03 00:00:00'
  AND started_at <  TIMESTAMP '2026-09-06 00:00:00';

-- ❌ BAD in Parquet mode (validated on the local fixture): timestamp-only
--    range reads every partition on the DuckDB read path (10/10 files).
SELECT COUNT(*) AS sessions
FROM parquet_sessions
WHERE started_at >= '2026-09-03' AND started_at < '2026-09-06';

-- Catalog mode uses the registered Iceberg view and prunes its hidden
-- day(started_at) transform from the timestamp range.
SELECT COUNT(*) AS sessions
FROM iceberg_sessions
WHERE started_at >= TIMESTAMP '2026-09-03 00:00:00'
  AND started_at < TIMESTAMP '2026-09-06 00:00:00';
```

### Network-Based Queries on Assets Table

The regression test `test_asset_partition_predicates_prune_files_scanned` in
`analytics/src/duckdb.rs` reads the checked-in local fixture at
`analytics/tests/fixtures/assets/`, with one Parquet file for each of four
`network`/`type` pairs across Taboola and MGID. `EXPLAIN ANALYZE` reads 2 of 4
files for `network = 'taboola'`, compared with all 4 files for the unfiltered
baseline, and the filtered query returns the two Taboola rows. The test uses
the Parquet view directly and does not connect to the Iceberg catalog. This
validates the network-pruning guidance below on the DuckDB Parquet read path;
it is a local fixture result, not a measurement of the live Iceberg catalog.

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
FROM parquet_ad_events
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
-- Catalog mode: timestamp ranges prune the hidden day(ts) transform.
SELECT *
FROM iceberg_ad_events
WHERE ts >= '2026-05-01'
  AND ts < '2026-05-08';

-- Current Parquet layout: include ts_day to skip files.
SELECT *
FROM parquet_ad_events
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-08'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-08 00:00:00';

-- Catalog mode: use a closed-open timestamp range rather than DATE_TRUNC.
SELECT *
FROM iceberg_ad_events
WHERE ts >= DATE_TRUNC('day', CURRENT_DATE + INTERVAL '-7 days')
  AND ts < DATE_TRUNC('day', CURRENT_DATE);

-- ❌ AVOID: DATE equality on timestamp
SELECT *
FROM iceberg_ad_events
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
-- Catalog mode: the timestamp predicate prunes the hidden transform.
WITH recent_campaigns AS (
    SELECT DISTINCT campaign_id
    FROM iceberg_ad_events
    WHERE ts >= CURRENT_DATE + INTERVAL '-7 days'
      AND network = 'taboola'
)
SELECT
    c.campaign_id,
    c.campaign_name,
    COUNT(*) AS total_clicks
FROM recent_campaigns c
JOIN iceberg_ad_events e
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

Fixture validation with DuckDB 1.5.2 (live table unavailable; status checked
2026-09-30):

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM parquet_sessions
WHERE started_at_day >= DATE '2026-09-03'
  AND started_at_day <  DATE '2026-09-06';

-- Look for the TABLE_SCAN node:
-- - "Total Files Read: 3"  (on the local fixture, this matches
--   days/partitions in range;
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
-- Parquet: instead of this timestamp-only predicate (which reads all files):
SELECT COUNT(*)
FROM parquet_ad_events
WHERE DATE(ts) = DATE '2026-05-01';

-- Pair the row filter with its physical partition range:
SELECT COUNT(*)
FROM parquet_ad_events
WHERE ts_day >= DATE '2026-05-01'
  AND ts_day < DATE '2026-05-02'
  AND ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-02 00:00:00';

-- In catalog mode, the timestamp range alone prunes the hidden day(ts) transform.
SELECT COUNT(*)
FROM iceberg_ad_events
WHERE ts >= TIMESTAMP '2026-05-01 00:00:00'
  AND ts < TIMESTAMP '2026-05-02 00:00:00';
```

### Issue: OR conditions prevent pruning

**Cause**: `WHERE network = 'taboola' OR network = 'outbrain'`

**Fix**: Use UNION ALL or IN clause
```sql
-- Use IN:
WHERE network IN ('taboola', 'outbrain')

-- Or UNION ALL for better control:
SELECT * FROM iceberg_ad_events WHERE network = 'taboola'
UNION ALL
SELECT * FROM iceberg_ad_events WHERE network = 'outbrain'
```

## Additional Resources

- [Iceberg Partitioning Docs](https://iceberg.apache.org/spec/#partitioning)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)
