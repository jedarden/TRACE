-- ============================================================================
-- Daily Session Summary from the materialized trace.sessions table
-- ============================================================================
-- Reads the session rows the materializer landed under
-- iceberg/sessions/data/started_at_day=YYYY-MM-DD/ (schema in
-- analytics/schemas/sessions_iceberg.sql) instead of re-sessionizing raw
-- events.
--
-- {{sessions_partition_filter}} filters the physical started_at_day partition
-- column, which is what actually prunes files on the DuckDB Parquet read
-- path (hive_partitioning = true exposes it from the directory name). The
-- started_at range is kept alongside it: it stays the authoritative row
-- filter on any engine, and against a true Iceberg table (where the
-- partition filter renders as TRUE) its day(started_at) transform is what
-- prunes.
--
-- Usage:
--   Replace {{sessions_table}} with the sessions table or view
--   Replace {{start_date}} and {{end_date}} with your date range
-- ============================================================================

SELECT
    CAST(started_at AS DATE) AS date,
    network,
    COUNT(*) AS sessions,
    SUM(pageviews) AS total_pageviews,
    SUM(clicks) AS total_clicks,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS converted_sessions,
    ROUND(
        100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS conversion_rate_pct,
    ROUND(AVG(duration_seconds), 1) AS avg_duration_seconds,
    SUM(CASE WHEN bounce THEN 1 ELSE 0 END) AS bounces,
    ROUND(
        100.0 * SUM(CASE WHEN bounce THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS bounce_rate_pct
FROM {{sessions_table}}
WHERE started_at >= '{{start_date}}'::TIMESTAMP
    AND started_at < '{{end_date}}'::TIMESTAMP

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
    AND {{sessions_partition_filter}}
GROUP BY 1, 2
ORDER BY 1 DESC, total_clicks DESC;
