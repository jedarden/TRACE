# Iceberg Partition Pruning Optimization Guide

## Overview

Partition pruning is a critical optimization technique that allows query engines to skip reading irrelevant partitions based on query predicates. Iceberg's hidden partitioning makes this transparent to users while maintaining performance.

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
WHERE ts >= DATE_TRUNC('day', CURRENT_DATE - INTERVAL '7' DAY)
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
    WHERE ts >= CURRENT_DATE - INTERVAL '7' DAY
      AND network = 'taboola'
)
SELECT
    c.campaign_id,
    c.campaign_name,
    COUNT(*) AS total_clicks
FROM recent_campaigns c
JOIN trace.ad_events e
    ON c.campaign_id = e.campaign_id
WHERE e.ts >= CURRENT_DATE - INTERVAL '7' DAY
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

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM trace.ad_events
WHERE ts >= '2026-05-01' AND ts < '2026-05-08';

-- Look for:
-- - "PARQUET_SCAN: 7 files" (should match days in range)
-- - "PROJECTION" and "AGGREGATE" operators
```

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
