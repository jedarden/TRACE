//! Session materialization: stitched sessions → the `trace.sessions` Iceberg table
//!
//! The Phase 7 stitcher (`session_stitcher.rs`) generates SQL that reconstructs
//! sessions from raw event Parquet, but nothing persisted its output. This
//! module is the write side: for one UTC day it re-runs gap-based
//! sessionization over the raw events and lands the session rows as a single
//! Parquet file under
//!
//!   `<prefix>/iceberg/sessions/data/started_at_day=<YYYY-MM-DD>/sessions-<YYYY-MM-DD>.parquet`
//!
//! That is the layout the compactor expects
//! (`IcebergTableSpec::sessions` in compactor/src/iceberg.rs): the hive-style
//! `started_at_day=` directory is what its partition extraction scans for, and
//! the bare value after `=` is what it records as the Iceberg partition value.
//! The file name is deterministic (one per day), so re-running the step for a
//! day overwrites that day's file in place — idempotent, and stale sessions
//! from a previous run never survive alongside the new ones.
//!
//! Row schema mirrors the `trace.sessions` DDL
//! (analytics/schemas/sessions_iceberg.sql) column for column, with counts as
//! INT, `conversion_value` as DOUBLE and timestamps as TIMESTAMP (micros), so
//! the files read back as the DDL expects.
//!
//! Sessions are keyed by the UTC day of `started_at`. Events are filtered to
//! one closed-open UTC day window before sessionizing, so every emitted row's
//! `started_at` falls on the target day — which is exactly the partition it is
//! written to. A logical session crossing midnight is therefore cut at the day
//! boundary (standard daily sessionization); the two halves get distinct
//! `session_id`s because the id embeds the day.

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use duckdb::Connection;
use std::path::PathBuf;
use tracing::info;

use crate::config::Config;
use crate::s3::S3Client;
use crate::session_stitcher::SessionConfig;

/// Table-relative data prefix — must match `IcebergTableSpec::sessions`
/// (`source_prefix`) in compactor/src/iceberg.rs.
pub const SESSIONS_DATA_PREFIX: &str = "iceberg/sessions/data";

/// Hive-style partition directory prefix — must match `partition_prefix` on
/// `IcebergTableSpec::sessions` in compactor/src/iceberg.rs.
pub const PARTITION_DIR_PREFIX: &str = "started_at_day=";

/// Event types that mark a session as converted. `purchase` and `signup` are
/// conversion flavors the attribution queries treat alongside `conversion`
/// (see raw_log_parser.rs and analytics/queries/attribution_*.sql).
const CONVERSION_TYPES: &str = "('conversion', 'purchase', 'signup')";

/// The UTC day a session started on — the day partition its row belongs to.
pub fn started_at_day(started_at: DateTime<Utc>) -> NaiveDate {
    started_at.date_naive()
}

/// Hive-style partition directory name for a day
/// (`started_at_day=2026-09-14`).
pub fn partition_dir(day: NaiveDate) -> String {
    format!("{}{}", PARTITION_DIR_PREFIX, day.format("%Y-%m-%d"))
}

/// Bucket-relative S3 key for a day's materialized sessions file — the same
/// bucket-relative convention the compactor keys objects by (its
/// `key_prefix`/`TRACE_S3_PREFIX` is prepended to this).
pub fn sessions_object_key(prefix: &str, day: NaiveDate) -> String {
    format!(
        "{}/{}/{}/sessions-{}.parquet",
        prefix.trim_end_matches('/'),
        SESSIONS_DATA_PREFIX,
        partition_dir(day),
        day.format("%Y-%m-%d")
    )
}

/// Closed-open UTC window of a day: `[day 00:00:00Z, day+1 00:00:00Z)`.
/// Filtering events to this window guarantees every sessionized row's
/// `started_at` lands inside the target day partition.
pub fn day_window(day: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = day
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc();
    let end = day
        .succ_opt()
        .expect("every date has a successor")
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc();
    (start, end)
}

/// The day the step materializes when none was requested: yesterday UTC, the
/// most recent complete day of events.
pub fn default_day(now: DateTime<Utc>) -> NaiveDate {
    (now - chrono::Duration::days(1)).date_naive()
}

/// Default DuckDB relation the sessionization reads events from: the same
/// events glob every view and query doc in this crate uses.
pub fn default_events_source(config: &Config) -> String {
    format!(
        "read_parquet('s3://{}/{}/events/**/*.parquet')",
        config.s3_bucket, config.s3_prefix
    )
}

/// Sessionization query for one day, projecting the `trace.sessions` DDL
/// columns in DDL order. `events_source` is the DuckDB relation raw events are
/// read from (a `read_parquet(...)` call in production, a table name in
/// tests). Gap thresholds come from the shared stitcher `SessionConfig`.
pub fn sessionization_query(
    day: NaiveDate,
    events_source: &str,
    config: &SessionConfig,
) -> String {
    let (window_start, window_end) = day_window(day);
    let max_session_seconds = config.max_session_hours * 3600;

    format!(
        r#"WITH day_events AS (
    SELECT
        ts,
        url,
        type,
        session_id,
        user_id,
        network,
        campaign_id,
        campaign_name,
        creative_id,
        headline,
        device_type,
        device_os,
        referrer,
        params
    FROM {events_source}
    WHERE ts >= '{window_start}'::TIMESTAMP
        AND ts < '{window_end}'::TIMESTAMP
        AND session_id IS NOT NULL
),
event_gaps AS (
    SELECT
        *,
        DATE_DIFF('second', LAG(ts) OVER (PARTITION BY session_id ORDER BY ts), ts) AS gap_seconds
    FROM day_events
),
session_assignments AS (
    SELECT
        *,
        SUM(CASE
                WHEN gap_seconds IS NULL THEN 1
                WHEN gap_seconds / 60.0 > {timeout_minutes} THEN 1
                ELSE 0
            END) OVER (PARTITION BY session_id ORDER BY ts) AS session_seq
    FROM event_gaps
)
SELECT
    -- Day-qualified reconstruction id: unique per day-cut row, and stable
    -- across re-runs of the same day (the bare stitcher id format would
    -- collide across partitions for sessions that cross midnight).
    session_id || '_' || strftime(MIN(ts), '%Y%m%d') || '_' || session_seq AS session_id,
    arg_min(user_id, ts) AS user_id,
    MIN(ts) AS started_at,
    MAX(ts) AS ended_at,
    CAST(COUNT(*) FILTER (WHERE type = 'pageview') AS INT) AS pageviews,
    CAST(COUNT(*) FILTER (WHERE type = 'click') AS INT) AS clicks,
    CAST(COUNT(*) FILTER (WHERE type = 'scroll') AS INT) AS scrolls,
    CAST(COUNT(*) FILTER (WHERE type = 'dwell') AS INT) AS dwells,
    CAST(COUNT(*) AS INT) AS event_count,
    arg_min(url, ts) AS entry_url,
    arg_max(url, ts) AS exit_url,
    arg_min(network, ts) AS network,
    arg_min(campaign_id, ts) AS campaign_id,
    arg_min(campaign_name, ts) AS campaign_name,
    arg_min(creative_id, ts) AS creative_id,
    arg_min(headline, ts) AS headline,
    BOOL_OR(type IN {conversion_types}) AS converted,
    CAST(COALESCE(SUM(TRY_CAST(params['revenue'] AS DOUBLE))
        FILTER (WHERE type IN {conversion_types}), 0.0) AS DOUBLE) AS conversion_value,
    arg_min(device_type, ts) AS device_type,
    arg_min(device_os, ts) AS device_os,
    arg_min(referrer, ts) AS referrer,
    CAST(DATE_DIFF('second', MIN(ts), MAX(ts)) AS INT) AS duration_seconds,
    COUNT(*) = 1 AS bounce,
    CAST(COUNT(DISTINCT url) AS INT) AS depth
FROM session_assignments
GROUP BY session_id, session_seq
HAVING DATE_DIFF('second', MIN(ts), MAX(ts)) <= {max_session_seconds}
ORDER BY session_id"#,
        events_source = events_source,
        window_start = window_start.format("%Y-%m-%d %H:%M:%S"),
        window_end = window_end.format("%Y-%m-%d %H:%M:%S"),
        timeout_minutes = config.session_timeout_minutes,
        conversion_types = CONVERSION_TYPES,
        max_session_seconds = max_session_seconds,
    )
}

/// Wrap a query in the DuckDB `COPY ... TO` that lands it as ZSTD-compressed
/// Parquet at `path` — the compression the sessions DDL declares
/// (`write.compression-codec = 'zstd'`).
pub fn copy_to_parquet_sql(query: &str, path: &PathBuf) -> String {
    format!(
        "COPY (\n{}\n) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD);",
        query,
        path.display()
    )
}

/// Outcome of one day's materialization run
#[derive(Debug, Clone)]
pub struct MaterializedDay {
    /// The UTC day that was materialized
    pub day: NaiveDate,
    /// Bucket-relative key the Parquet file was written to
    pub object_key: String,
    /// Session rows written (0 for a day with no events)
    pub row_count: usize,
    /// Size of the uploaded Parquet file in bytes
    pub size_bytes: usize,
}

/// Materialize stitched sessions for one UTC day into `trace.sessions`.
///
/// Runs the sessionization query in DuckDB, writes the result as Parquet to a
/// local temp file, and uploads it to the day's deterministic object key —
/// overwriting whatever that day held before, which is what makes re-runs
/// idempotent. `day = None` means yesterday UTC.
pub async fn materialize_sessions(
    conn: &Connection,
    s3: &S3Client,
    config: &Config,
    day: Option<NaiveDate>,
    events_glob: Option<&str>,
) -> Result<MaterializedDay> {
    let day = day.unwrap_or_else(|| default_day(Utc::now()));
    let events_source = match events_glob {
        Some(glob) => format!("read_parquet('{}')", glob),
        None => default_events_source(config),
    };

    let local_path = std::env::temp_dir().join(format!(
        "trace-sessions-{}-{}.parquet",
        day.format("%Y%m%d"),
        std::process::id()
    ));

    write_day_parquet(conn, day, &events_source, &local_path)?;
    let row_count = count_parquet_rows(conn, &local_path)?;
    let bytes = std::fs::read(&local_path)
        .with_context(|| format!("Failed to read materialized Parquet at {}", local_path.display()))?;

    let object_key = sessions_object_key(&config.s3_prefix, day);
    s3.put_object(&object_key, &bytes).await?;

    // Best-effort cleanup: the upload already succeeded, a leftover temp file
    // must not fail the run.
    let _ = std::fs::remove_file(&local_path);

    info!(
        "Materialized {} sessions for {} into {} ({} bytes)",
        row_count,
        day.format("%Y-%m-%d"),
        object_key,
        bytes.len()
    );

    Ok(MaterializedDay {
        day,
        object_key,
        row_count,
        size_bytes: bytes.len(),
    })
}

/// Run the day's sessionization into a local Parquet file
fn write_day_parquet(
    conn: &Connection,
    day: NaiveDate,
    events_source: &str,
    local_path: &PathBuf,
) -> Result<()> {
    let session_config = SessionConfig::default();
    let query = sessionization_query(day, events_source, &session_config);
    let sql = copy_to_parquet_sql(&query, local_path);
    conn.execute_batch(&sql)
        .context("Sessionization COPY failed")
}

/// Count the rows in a Parquet file via the same engine that wrote it
fn count_parquet_rows(conn: &Connection, path: &PathBuf) -> Result<usize> {
    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}');",
        path.display()
    );
    let count: i64 = conn
        .prepare(&sql)?
        .query_map([], |row| row.get(0))?
        .next()
        .context("COUNT(*) returned no rows")??;
    Ok(count as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap()
    }

    // ------------------------------------------------------------------
    // Day-partition derivation from started_at (acceptance criterion)
    // ------------------------------------------------------------------

    #[test]
    fn test_started_at_day_keys_by_utc_day() {
        // Mid-day timestamps map to their own day
        assert_eq!(started_at_day(ts(2026, 9, 14, 10, 0, 0)), naive(2026, 9, 14));

        // The last second of a day still belongs to that day
        assert_eq!(started_at_day(ts(2026, 9, 14, 23, 59, 59)), naive(2026, 9, 14));

        // Midnight rollover: the first second of a day belongs to the next
        assert_eq!(started_at_day(ts(2026, 9, 15, 0, 0, 0)), naive(2026, 9, 15));
    }

    #[test]
    fn test_started_at_day_is_utc_not_local() {
        // 2026-09-15 01:30 at UTC+2 is 2026-09-14 23:30 UTC — the session must
        // partition to the 14th, because partitions key on the UTC day.
        let utc = ts(2026, 9, 14, 23, 30, 0);
        let offset = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
        let local = utc.with_timezone(&offset);
        assert_eq!(local.format("%Y-%m-%d %H:%M").to_string(), "2026-09-15 01:30");
        assert_eq!(started_at_day(utc), naive(2026, 9, 14));
    }

    #[test]
    fn test_day_window_is_closed_open() {
        let (start, end) = day_window(naive(2026, 9, 14));
        assert_eq!(start, ts(2026, 9, 14, 0, 0, 0));
        assert_eq!(end, ts(2026, 9, 15, 0, 0, 0));

        // The window is half-open: the day's last event is included, the next
        // day's first event is not.
        let day_len = end - start;
        assert_eq!(day_len, chrono::Duration::hours(24));
        assert_eq!(started_at_day(end), naive(2026, 9, 15));
    }

    // ------------------------------------------------------------------
    // Hive-style partition layout the compactor expects
    // ------------------------------------------------------------------

    #[test]
    fn test_partition_dir_is_hive_style() {
        assert_eq!(
            partition_dir(naive(2026, 9, 14)),
            "started_at_day=2026-09-14"
        );
    }

    #[test]
    fn test_object_key_matches_compactor_layout() {
        let key = sessions_object_key("trace-events", naive(2026, 9, 14));

        // Exact shape: <prefix>/iceberg/sessions/data/started_at_day=<day>/sessions-<day>.parquet
        assert_eq!(
            key,
            "trace-events/iceberg/sessions/data/started_at_day=2026-09-14/sessions-2026-09-14.parquet"
        );

        // The compactor's partition extraction finds the partition component
        // by the started_at_day= prefix and recovers the bare value, which
        // must parse back to the same day.
        let partition = key
            .split('/')
            .find(|part| part.starts_with(PARTITION_DIR_PREFIX))
            .expect("key must contain the partition directory");
        assert_eq!(partition, "started_at_day=2026-09-14");
        let value = partition
            .strip_prefix(PARTITION_DIR_PREFIX)
            .expect("partition must carry the prefix");
        assert_eq!(
            NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap(),
            naive(2026, 9, 14),
            "compactor must be able to parse the partition value as a date"
        );
    }

    #[test]
    fn test_object_key_handles_trailing_slash_prefix() {
        assert_eq!(
            sessions_object_key("trace-events/", naive(2026, 1, 2)),
            sessions_object_key("trace-events", naive(2026, 1, 2))
        );
    }

    #[test]
    fn test_object_key_is_deterministic_per_day() {
        // Idempotency rests on the key being a pure function of (prefix, day)
        assert_eq!(
            sessions_object_key("p", naive(2026, 9, 14)),
            sessions_object_key("p", naive(2026, 9, 14))
        );
        assert_ne!(
            sessions_object_key("p", naive(2026, 9, 14)),
            sessions_object_key("p", naive(2026, 9, 15))
        );
    }

    #[test]
    fn test_default_day_is_yesterday() {
        assert_eq!(default_day(ts(2026, 9, 15, 0, 0, 1)), naive(2026, 9, 14));
        assert_eq!(default_day(ts(2026, 9, 15, 23, 59, 59)), naive(2026, 9, 14));
        // Month and year boundaries
        assert_eq!(default_day(ts(2026, 3, 1, 12, 0, 0)), naive(2026, 2, 28));
        assert_eq!(default_day(ts(2026, 1, 1, 12, 0, 0)), naive(2025, 12, 31));
    }

    // ------------------------------------------------------------------
    // Sessionization SQL shape
    // ------------------------------------------------------------------

    #[test]
    fn test_sessionization_query_projects_ddl_columns_in_order() {
        let query = sessionization_query(naive(2026, 9, 14), "read_parquet('s3://b/p/events/**/*.parquet')", &SessionConfig::default());

        // Column source of truth: analytics/schemas/sessions_iceberg.sql
        let expected = [
            "session_id",
            "user_id",
            "started_at",
            "ended_at",
            "pageviews",
            "clicks",
            "scrolls",
            "dwells",
            "event_count",
            "entry_url",
            "exit_url",
            "network",
            "campaign_id",
            "campaign_name",
            "creative_id",
            "headline",
            "converted",
            "conversion_value",
            "device_type",
            "device_os",
            "referrer",
            "duration_seconds",
            "bounce",
            "depth",
        ];
        let select_start = query.find("SELECT").expect("query has a SELECT");
        for (i, column) in expected.iter().enumerate() {
            let after = &query[select_start..];
            let found = after.find(&format!("AS {}", column));
            assert!(found.is_some(), "missing projection AS {}", column);
            // Columns must appear in DDL order
            if i > 0 {
                let prev = after
                    .find(&format!("AS {}", expected[i - 1]))
                    .expect("previous column exists");
                assert!(
                    found.unwrap() > prev,
                    "{} must come after {}",
                    column,
                    expected[i - 1]
                );
            }
        }
    }

    #[test]
    fn test_sessionization_query_filters_to_day_window() {
        let query = sessionization_query(naive(2026, 9, 14), "events", &SessionConfig::default());
        assert!(query.contains(">= '2026-09-14 00:00:00'::TIMESTAMP"));
        assert!(query.contains("< '2026-09-15 00:00:00'::TIMESTAMP"));
        // Thresholds come from the shared stitcher config
        assert!(query.contains("> 30"));
        assert!(query.contains("<= 14400"));
    }

    fn naive(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    // ------------------------------------------------------------------
    // End-to-end: sessionize in-memory events into a Parquet file
    // ------------------------------------------------------------------

    /// Mirror of the raw event Parquet columns the materializer consumes.
    fn create_events_table(conn: &Connection) {
        conn.execute_batch(
            r#"
            CREATE TABLE events (
                ts TIMESTAMP,
                url VARCHAR,
                type VARCHAR,
                session_id VARCHAR,
                user_id VARCHAR,
                network VARCHAR,
                campaign_id VARCHAR,
                campaign_name VARCHAR,
                creative_id VARCHAR,
                headline VARCHAR,
                device_type VARCHAR,
                device_os VARCHAR,
                referrer VARCHAR,
                params MAP(VARCHAR, VARCHAR)
            );
            INSERT INTO events VALUES
                -- s1: one continuous 20-minute session with every count type
                ('2026-09-14 10:00:00', 'https://x.com/a', 'pageview', 's1', 'u1', 'taboola', 'camp1', 'Camp One', 'cr1', 'Headline One', 'mobile', 'Android', 'https://r.example', NULL),
                ('2026-09-14 10:05:00', 'https://x.com/b', 'click', 's1', 'u1', NULL, NULL, NULL, NULL, NULL, 'mobile', 'Android', NULL, NULL),
                ('2026-09-14 10:12:00', 'https://x.com/c', 'scroll', 's1', 'u1', NULL, NULL, NULL, NULL, NULL, 'mobile', 'Android', NULL, NULL),
                ('2026-09-14 10:20:00', 'https://x.com/conv', 'conversion', 's1', 'u1', NULL, NULL, NULL, NULL, NULL, 'mobile', 'Android', NULL, MAP{'revenue': '42.5'}),
                -- s1 continued after a 70-minute gap: must split into its own session
                ('2026-09-14 11:30:00', 'https://x.com/d', 'pageview', 's1', 'u1', 'outbrain', 'camp2', 'Camp Two', 'cr2', 'Headline Two', 'desktop', 'Linux', NULL, NULL),
                ('2026-09-14 11:31:00', 'https://x.com/e', 'dwell', 's1', 'u1', NULL, NULL, NULL, NULL, NULL, 'desktop', 'Linux', NULL, NULL),
                -- s2: single event -> bounce
                ('2026-09-14 12:00:00', 'https://x.com/f', 'pageview', 's2', 'u2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                -- s3 lives on the next day: excluded by the day window
                ('2026-09-15 00:30:00', 'https://x.com/g', 'pageview', 's3', 'u3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            "#,
        )
        .unwrap();
    }

    #[test]
    fn test_day_materialization_end_to_end() {
        let conn = Connection::open_in_memory().unwrap();
        create_events_table(&conn);

        let out_path = std::env::temp_dir().join(format!(
            "trace-sessions-e2e-{}.parquet",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&out_path);

        write_day_parquet(&conn, naive(2026, 9, 14), "events", &out_path).unwrap();

        // Exactly three sessions survive: s1 split in two, plus the bounce.
        // The next-day session is excluded by the window.
        let count: i64 = count_parquet_rows(&conn, &out_path).unwrap() as i64;
        assert_eq!(count, 3);

        // Every row's started_at must fall on the partition day — this is the
        // invariant that makes started_at_day=<date> a truthful directory.
        let wrong_day: i64 = conn
            .prepare(&format!(
                "SELECT COUNT(*) FROM read_parquet('{}') WHERE strftime(started_at, '%Y-%m-%d') <> '2026-09-14'",
                out_path.display()
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(wrong_day, 0);

        // Schema must read back exactly as the trace.sessions DDL declares it:
        // same columns, same order, same engine types.
        let expected_schema = [
            ("session_id", "VARCHAR"),
            ("user_id", "VARCHAR"),
            ("started_at", "TIMESTAMP"),
            ("ended_at", "TIMESTAMP"),
            ("pageviews", "INTEGER"),
            ("clicks", "INTEGER"),
            ("scrolls", "INTEGER"),
            ("dwells", "INTEGER"),
            ("event_count", "INTEGER"),
            ("entry_url", "VARCHAR"),
            ("exit_url", "VARCHAR"),
            ("network", "VARCHAR"),
            ("campaign_id", "VARCHAR"),
            ("campaign_name", "VARCHAR"),
            ("creative_id", "VARCHAR"),
            ("headline", "VARCHAR"),
            ("converted", "BOOLEAN"),
            ("conversion_value", "DOUBLE"),
            ("device_type", "VARCHAR"),
            ("device_os", "VARCHAR"),
            ("referrer", "VARCHAR"),
            ("duration_seconds", "INTEGER"),
            ("bounce", "BOOLEAN"),
            ("depth", "INTEGER"),
        ];
        let mut stmt = conn
            .prepare(&format!(
                "DESCRIBE SELECT * FROM read_parquet('{}')",
                out_path.display()
            ))
            .unwrap();
        let actual_schema: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual_schema.len(), expected_schema.len());
        for (i, (name, ty)) in expected_schema.iter().enumerate() {
            assert_eq!(
                (actual_schema[i].0.as_str(), actual_schema[i].1.as_str()),
                (*name, *ty),
                "column {} (0-indexed) mismatch",
                i
            );
        }

        // First s1 segment: first-touch attribution from the earliest event,
        // per-type counts, conversion revenue summed from the params map.
        let mut stmt = conn
            .prepare(&format!(
                "SELECT session_id, user_id, pageviews, clicks, scrolls, dwells, event_count,
                        entry_url, exit_url, network, campaign_id, converted, conversion_value,
                        device_type, duration_seconds, bounce, depth
                 FROM read_parquet('{}') WHERE session_id = 's1_20260914_1'",
                out_path.display()
            ))
            .unwrap();
        let row = stmt
            .query_row([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, i32>(4)?,
                    row.get::<_, i32>(5)?,
                    row.get::<_, i32>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, bool>(11)?,
                    row.get::<_, f64>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, i32>(14)?,
                    row.get::<_, bool>(15)?,
                    row.get::<_, i32>(16)?,
                ))
            })
            .unwrap();
        assert_eq!(row.0, "s1_20260914_1");
        assert_eq!(row.1.as_deref(), Some("u1"));
        assert_eq!((row.2, row.3, row.4, row.5, row.6), (1, 1, 1, 0, 4));
        assert_eq!(row.7, "https://x.com/a");
        assert_eq!(row.8, "https://x.com/conv");
        assert_eq!(row.9.as_deref(), Some("taboola"));
        assert_eq!(row.10.as_deref(), Some("camp1"));
        assert!(row.11, "conversion event must mark the session converted");
        assert!((row.12 - 42.5).abs() < f64::EPSILON);
        assert_eq!(row.13, Some("mobile".to_string()));
        assert_eq!(row.14, 1200, "10:00 -> 10:20");
        assert!(!row.15);
        assert_eq!(row.16, 4);

        // Second s1 segment: the 70-minute gap started a new session with its
        // own first-touch attribution.
        let mut stmt = conn
            .prepare(&format!(
                "SELECT network, campaign_id, event_count, bounce
                 FROM read_parquet('{}') WHERE session_id = 's1_20260914_2'",
                out_path.display()
            ))
            .unwrap();
        let row = stmt
            .query_row([], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
            .unwrap();
        assert_eq!(row.0.as_deref(), Some("outbrain"));
        assert_eq!(row.1.as_deref(), Some("camp2"));
        assert_eq!(row.2, 2);
        assert!(!row.3);

        // The bounce session: a single event.
        let mut stmt = conn
            .prepare(&format!(
                "SELECT bounce, event_count, depth, conversion_value
                 FROM read_parquet('{}') WHERE session_id = 's2_20260914_1'",
                out_path.display()
            ))
            .unwrap();
        let row = stmt
            .query_row([], |row| {
                Ok((
                    row.get::<_, bool>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, f64>(3)?,
                ))
            })
            .unwrap();
        assert!(row.0);
        assert_eq!(row.1, 1);
        assert_eq!(row.2, 1);
        assert_eq!(row.3, 0.0);

        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_rerunning_a_day_overwrites_the_same_output() {
        let conn = Connection::open_in_memory().unwrap();
        create_events_table(&conn);

        let out_path = std::env::temp_dir().join(format!(
            "trace-sessions-rerun-{}.parquet",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&out_path);

        // Same day, same destination: the second run must replace the file,
        // not fail or stack a second copy beside it.
        write_day_parquet(&conn, naive(2026, 9, 14), "events", &out_path).unwrap();
        let first = count_parquet_rows(&conn, &out_path).unwrap();
        write_day_parquet(&conn, naive(2026, 9, 14), "events", &out_path).unwrap();
        let second = count_parquet_rows(&conn, &out_path).unwrap();

        assert_eq!(first, second, "same input must produce the same rows");
        assert_eq!(first, 3);

        // A different day targets a different window: only that day's rows.
        write_day_parquet(&conn, naive(2026, 9, 15), "events", &out_path).unwrap();
        let next_day = count_parquet_rows(&conn, &out_path).unwrap();
        assert_eq!(next_day, 1, "only the 2026-09-15 session");

        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_empty_day_writes_a_valid_empty_file() {
        let conn = Connection::open_in_memory().unwrap();
        create_events_table(&conn);

        let out_path = std::env::temp_dir().join(format!(
            "trace-sessions-empty-{}.parquet",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&out_path);

        write_day_parquet(&conn, naive(2025, 1, 1), "events", &out_path).unwrap();
        assert_eq!(count_parquet_rows(&conn, &out_path).unwrap(), 0);

        let _ = std::fs::remove_file(&out_path);
    }
}
