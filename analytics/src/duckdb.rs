use crate::config::Config;
use anyhow::{Context, Result};
use duckdb::{params, Connection};

/// SQL for a view over a Hive-partitioned Parquet glob. The
/// `hive_partitioning = true` flag is what exposes the directory-named
/// partition columns (`dt`, `ts_day`, `started_at_day`, …) — without it those
/// columns don't exist and no partition predicate can prune files.
pub fn hive_parquet_view_sql(view_name: &str, glob: &str) -> String {
    format!(
        "CREATE OR REPLACE VIEW {} AS \
         SELECT * FROM read_parquet('{}', hive_partitioning = true);",
        view_name, glob
    )
}

fn create_optional_parquet_view(
    conn: &Connection,
    view_name: &str,
    glob: &str,
    error_context: &str,
) -> Result<()> {
    match conn.execute(&hive_parquet_view_sql(view_name, glob), params![]) {
        Ok(_) => Ok(()),
        Err(error) if format!("{error:#}").contains("No files found that match the pattern") => {
            Ok(())
        }
        Err(error) => Err(error).context(error_context.to_string()),
    }
}

pub struct DuckDBClient {
    conn: Connection,
}

impl DuckDBClient {
    pub fn new(config: &Config) -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        // Install and load required extensions
        let mut extensions = vec!["INSTALL httpfs;", "LOAD httpfs;"];

        // Load Iceberg extension if catalog is configured
        if config.iceberg_catalog_uri.is_some() {
            extensions.push("INSTALL iceberg;");
            extensions.push("LOAD iceberg;");
        }

        conn.execute_batch(&extensions.join("\n"))
            .context("Failed to load DuckDB extensions")?;

        // Configure S3 credentials if provided
        if let (Some(access_key), Some(secret_key)) =
            (&config.s3_access_key_id, &config.s3_secret_access_key)
        {
            let endpoint = config.s3_endpoint.as_deref().unwrap_or("s3.amazonaws.com");
            conn.execute("SET s3_endpoint=?;", params![endpoint])?;
            conn.execute("SET s3_access_key_id=?;", params![access_key])?;
            conn.execute("SET s3_secret_access_key=?;", params![secret_key])?;
        }

        conn.execute("SET s3_region=?;", params![&config.s3_region])?;

        conn.execute("SET s3_use_ssl=true;", params![])?;

        // Set memory limits for large queries
        conn.execute("SET memory_limit='2GB';", params![])?;

        conn.execute("SET threads=4;", params![])?;

        let client = Self { conn };

        // Setup Iceberg views if configured
        if config.is_iceberg_enabled() {
            client.setup_iceberg_views(config)?;
        } else {
            // Setup Parquet views for backward compatibility
            let s3_path = format!("s3://{}/{}", config.s3_bucket, config.s3_prefix);
            client.setup_parquet_views(&s3_path, config)?;
        }

        Ok(client)
    }

    /// Direct access to the underlying connection for modules that run their
    /// own SQL against the same configured engine (e.g. session
    /// materialization), instead of going through the report layer.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.prepare(sql)?;
        let columns: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
        let rows = stmt
            .query_map([], |row| {
                let mut values = Vec::new();
                for i in 0..row.as_ref().column_count() {
                    let value: Option<String> = row.get(i)?;
                    values.push(value.unwrap_or_else(|| "NULL".to_string()));
                }
                Ok(values)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QueryResult { columns, rows })
    }

    pub fn setup_views(&self, s3_path: &str) -> Result<()> {
        let view_sql = hive_parquet_view_sql("events", &format!("{}/events/**/*.parquet", s3_path));
        self.conn.execute(&view_sql, params![])?;

        let compacted_sql = hive_parquet_view_sql(
            "events_compacted",
            &format!("{}/events-compacted/**/*.parquet", s3_path),
        );
        self.conn.execute(&compacted_sql, params![])?;

        Ok(())
    }

    /// Setup views for Iceberg tables
    /// Requires Iceberg extension and catalog URI to be configured
    pub fn setup_iceberg_views(&self, config: &Config) -> Result<()> {
        let catalog_uri = config
            .iceberg_catalog_uri
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Iceberg catalog URI not configured"))?;
        let warehouse = config
            .iceberg_warehouse
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Iceberg warehouse not configured"))?;

        // Build the catalog connection string
        // DuckDB iceberg_scan format: iceberg_scan('table_path', catalog_uri => 'uri')
        let catalog_option = format!("catalog_uri => '{}'", catalog_uri);

        // Create view for ad_events table
        let ad_events_path = format!("{}/ad_events", warehouse);
        let ad_events_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_ad_events AS \
             SELECT * FROM iceberg_scan('{}', {});",
            ad_events_path, catalog_option
        );
        self.conn
            .execute(&ad_events_sql, params![])
            .context("Failed to create view for Iceberg ad_events table")?;

        // Create view for campaigns dimension table
        let campaigns_path = format!("{}/campaigns", warehouse);
        let campaigns_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_campaigns AS \
             SELECT * FROM iceberg_scan('{}', {});",
            campaigns_path, catalog_option
        );
        self.conn
            .execute(&campaigns_sql, params![])
            .context("Failed to create view for Iceberg campaigns table")?;

        // Create view for creatives dimension table
        let creatives_path = format!("{}/creatives", warehouse);
        let creatives_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_creatives AS \
             SELECT * FROM iceberg_scan('{}', {});",
            creatives_path, catalog_option
        );
        self.conn
            .execute(&creatives_sql, params![])
            .context("Failed to create view for Iceberg creatives table")?;

        // Create view for assets dimension table (headlines, images, and
        // landing pages exploded from synced creatives — schema in
        // analytics/schemas/assets_iceberg.sql)
        let assets_path = format!("{}/assets", warehouse);
        let assets_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_assets AS \
             SELECT * FROM iceberg_scan('{}', {});",
            assets_path, catalog_option
        );
        self.conn
            .execute(&assets_sql, params![])
            .context("Failed to create view for Iceberg assets table")?;

        // Create view for the sessions table (day(started_at) partition
        // transform — schema in analytics/schemas/sessions_iceberg.sql)
        let sessions_path = format!("{}/sessions", warehouse);
        let sessions_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_sessions AS \
             SELECT * FROM iceberg_scan('{}', {});",
            sessions_path, catalog_option
        );
        self.conn
            .execute(&sessions_sql, params![])
            .context("Failed to create view for Iceberg sessions table")?;

        Ok(())
    }

    /// Setup views for Parquet files (legacy mode)
    /// Falls back to Parquet when Iceberg is not configured
    ///
    /// Every view sets `hive_partitioning = true`: the pipeline writes
    /// Hive-style partition directories (`events/dt=YYYY-MM-DD/hour=HH/`,
    /// `events-compacted/dt=YYYY-MM-DD/`, `iceberg/ad_events/data/ts_day=…/`,
    /// `iceberg/sessions/data/started_at_day=…/` — none of which carry the
    /// partition value as an in-file column), so the partition columns only
    /// exist if hive extraction is on. Without it a `dt`/`ts_day`/
    /// `started_at_day` predicate is an unknown-column error, and with it the
    /// predicate is what lets DuckDB skip whole directories — filtering the
    /// in-file `ts` timestamp alone reads every file (verified, see
    /// docs/analytics/iceberg_partition_pruning.md and
    /// `test_partition_predicate_prunes_files_scanned` below).
    ///
    /// The two event views have one opt-in exception: with
    /// `compat_event_views` they come from `crate::events_compat` instead,
    /// trading the day-directory pruning for the ability to span flusher
    /// generations whose schemas differ.
    pub fn setup_parquet_views(&self, s3_path: &str, config: &Config) -> Result<()> {
        if config.compat_event_views {
            // Opt-in (TRACE_COMPAT_EVENT_VIEWS=1): the events prefix spans
            // flusher generations (docs/analytics/event_schema_versions.md),
            // and a single plain read_parquet glob cannot — the first file's
            // schema wins and files missing its columns error at query time.
            // The compat path classifies each file by footer and projects the
            // canonical schema per generation. It derives `dt` from `ts`, so
            // it gives up day-directory pruning on these two views — which
            // is why it is not the default. Unlike the plain views below it
            // is eager (footer reads at setup), so an unreachable/empty glob
            // fails here instead of at first query.
            crate::events_compat::setup_compat_events_views(self.connection(), s3_path)
                .context("Failed to create schema-version compatibility views for events")?;
        } else {
            let views = [
                (
                    "parquet_events",
                    format!("{}/events/**/*.parquet", s3_path),
                    "Failed to create view for Parquet events",
                ),
                (
                    "parquet_events_compacted",
                    format!("{}/events-compacted/**/*.parquet", s3_path),
                    "Failed to create view for compacted Parquet events",
                ),
            ];
            for (view_name, glob, err_msg) in views {
                create_optional_parquet_view(&self.conn, view_name, &glob, err_msg)?;
            }
        }

        let ad_events_glob = format!("{}/iceberg/ad_events/data/**/*.parquet", s3_path);
        create_optional_parquet_view(
            &self.conn,
            "parquet_ad_events",
            &ad_events_glob,
            "Failed to create view for partitioned ad_events",
        )?;

        let sessions_glob = format!("{}/iceberg/sessions/data/**/*.parquet", s3_path);
        create_optional_parquet_view(
            &self.conn,
            "parquet_sessions",
            &sessions_glob,
            "Failed to create view for partitioned sessions",
        )?;

        Ok(())
    }

    /// Get the SQL fragment for querying events (either Iceberg or Parquet)
    /// Returns the appropriate view name based on configuration
    pub fn events_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_ad_events".to_string()
        } else {
            "parquet_events".to_string()
        }
    }

    /// Get the SQL fragment for querying compacted events
    /// For Iceberg, we filter on the main table; for Parquet, use compacted view
    pub fn events_compacted_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            // For Iceberg, we can filter on the main table with time-based partition pruning
            "iceberg_ad_events".to_string()
        } else {
            "parquet_events_compacted".to_string()
        }
    }

    /// Get the SQL fragment for querying campaigns dimension table
    pub fn campaigns_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_campaigns".to_string()
        } else {
            // For Parquet mode, campaigns data is embedded in events
            // Return a subquery that extracts unique campaigns from events
            format!(
                "(SELECT DISTINCT campaign_id, campaign_name, network FROM {})",
                self.events_table_sql(config)
            )
        }
    }

    /// Get the SQL fragment for querying creatives dimension table
    pub fn creatives_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_creatives".to_string()
        } else {
            // For Parquet mode, creatives data is embedded in events
            // Return a subquery that extracts unique creatives from events
            format!(
                "(SELECT DISTINCT creative_id, headline, image_id, network FROM {})",
                self.events_table_sql(config)
            )
        }
    }

    /// Get the SQL fragment for querying the assets dimension table
    /// (headlines, images, and landing pages synced from the ad network
    /// APIs, partitioned by network and type)
    pub fn assets_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_assets".to_string()
        } else {
            // For Parquet mode, read the syncer's asset dimension files
            // directly. Each file carries network/type as columns, so no
            // hive_partitioning is needed (it would duplicate them)
            let assets_path = format!("s3://{}/{}/assets", config.s3_bucket, config.s3_prefix);
            format!(
                "(SELECT * FROM read_parquet('{}/**/*.parquet'))",
                assets_path
            )
        }
    }

    /// Get the SQL fragment for querying the sessions table
    /// (materialized stitcher output under iceberg/sessions/data/)
    pub fn sessions_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_sessions".to_string()
        } else {
            "parquet_sessions".to_string()
        }
    }

    /// The physical day-partition column of the events tables, when the
    /// backend has one.
    ///
    /// Parquet mode reads Hive-style `dt=YYYY-MM-DD/` directories
    /// (`events/**`, `events-compacted/**`), so `dt` is a real — and the only
    /// prunable — partition column; a `ts` range alone does not skip files on
    /// this read path. Iceberg mode reads a true Iceberg table where
    /// partitioning is hidden (`DAYS(ts)` transform): there is no `ts_day`
    /// column to reference and the `ts` range predicate itself prunes, so
    /// this returns None and template partition filters render as TRUE.
    pub fn events_partition_column(&self, config: &Config) -> Option<&'static str> {
        if config.is_iceberg_enabled() {
            None
        } else {
            Some("dt")
        }
    }

    /// The physical day-partition column of the sessions table, when the
    /// backend has one. Parquet mode reads the materializer's
    /// `started_at_day=YYYY-MM-DD/` directories; see
    /// [`Self::events_partition_column`] for why Iceberg mode returns None.
    pub fn sessions_partition_column(&self, config: &Config) -> Option<&'static str> {
        if config.is_iceberg_enabled() {
            None
        } else {
            Some("started_at_day")
        }
    }
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl QueryResult {
    pub fn to_json(&self) -> String {
        let mut json = String::from("[");

        for (i, row) in self.rows.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            json.push('{');
            for (j, value) in row.iter().enumerate() {
                if j > 0 {
                    json.push(',');
                }
                json.push_str(&format!(
                    "\"{}\":{}",
                    self.columns[j],
                    escape_json_value(value)
                ));
            }
            json.push('}');
        }

        json.push(']');
        json
    }

    pub fn to_csv(&self) -> String {
        let mut csv = String::new();

        // Header row
        csv.push_str(&self.columns.join(","));
        csv.push('\n');

        // Data rows
        for row in &self.rows {
            csv.push_str(
                &row.iter()
                    .map(|v| escape_csv_value(v))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            csv.push('\n');
        }

        csv
    }
}

fn escape_json_value(value: &str) -> String {
    if value == "NULL" {
        return "null".to_string();
    }

    // Try to parse as number
    if value.parse::<f64>().is_ok() {
        return value.to_string();
    }

    // Escape string
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_json_value_null() {
        assert_eq!(escape_json_value("NULL"), "null");
    }

    #[test]
    fn test_escape_json_value_number() {
        assert_eq!(escape_json_value("123"), "123");
        assert_eq!(escape_json_value("45.67"), "45.67");
    }

    #[test]
    fn test_escape_json_value_string() {
        assert_eq!(escape_json_value("hello"), "\"hello\"");
        assert_eq!(escape_json_value("hello\"world"), "\"hello\\\"world\"");
        assert_eq!(escape_json_value("hello\\world"), "\"hello\\\\world\"");
    }

    #[test]
    fn test_escape_csv_value_simple() {
        assert_eq!(escape_csv_value("hello"), "hello");
        assert_eq!(escape_csv_value("hello world"), "hello world");
    }

    #[test]
    fn test_escape_csv_value_with_comma() {
        assert_eq!(escape_csv_value("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_escape_csv_value_with_quote() {
        assert_eq!(escape_csv_value("hello\"world"), "\"hello\"\"world\"");
    }

    #[test]
    fn test_escape_csv_value_with_newline() {
        assert_eq!(escape_csv_value("hello\nworld"), "\"hello\nworld\"");
    }

    #[test]
    fn test_query_result_to_json_empty() {
        let result = QueryResult {
            columns: vec!["col1".to_string(), "col2".to_string()],
            rows: vec![],
        };
        assert_eq!(result.to_json(), "[]");
    }

    #[test]
    fn test_query_result_to_json_single_row() {
        let result = QueryResult {
            columns: vec!["col1".to_string(), "col2".to_string()],
            rows: vec![vec!["value1".to_string(), "value2".to_string()]],
        };
        let json = result.to_json();
        assert!(json.contains("\"col1\":\"value1\""));
        assert!(json.contains("\"col2\":\"value2\""));
    }

    #[test]
    fn test_query_result_to_csv_empty() {
        let result = QueryResult {
            columns: vec!["col1".to_string(), "col2".to_string()],
            rows: vec![],
        };
        let csv = result.to_csv();
        assert_eq!(csv, "col1,col2\n");
    }

    #[test]
    fn test_query_result_to_csv_with_data() {
        let result = QueryResult {
            columns: vec!["col1".to_string(), "col2".to_string()],
            rows: vec![
                vec!["value1".to_string(), "value2".to_string()],
                vec!["value3".to_string(), "value4".to_string()],
            ],
        };
        let csv = result.to_csv();
        assert_eq!(csv, "col1,col2\nvalue1,value2\nvalue3,value4\n");
    }

    #[test]
    fn test_query_result_to_csv_with_special_chars() {
        let result = QueryResult {
            columns: vec!["col1".to_string()],
            rows: vec![vec!["value,with,commas".to_string()]],
        };
        let csv = result.to_csv();
        assert_eq!(csv, "col1\n\"value,with,commas\"\n");
    }
}

/// Regression guard for partition pruning on the DuckDB read path.
///
/// The pipeline writes Hive-style day directories of Parquet
/// (`events-compacted/dt=YYYY-MM-DD/`, `iceberg/sessions/data/
/// started_at_day=YYYY-MM-DD/`, …). On this read path a filter on the
/// in-file timestamp does NOT skip files — only a filter on the partition
/// column does (measured: see docs/analytics/iceberg_partition_pruning.md).
/// These tests run real queries over a real partitioned tree and assert the
/// `TABLE_SCAN -> Total Files Read` counter from EXPLAIN ANALYZE, so a change
/// that drops `hive_partitioning = true`, breaks the partition predicate
/// rendering, or points a view at an unpartitioned layout fails here instead
/// of silently scanning the whole bucket.
#[cfg(test)]
mod partition_pruning_tests {
    use super::*;
    use crate::config::Config;
    use crate::queries::{partition_predicate, render_template_with_client, ReportParams};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    static DIR_SEQ: AtomicU32 = AtomicU32::new(0);

    /// Unique temp dir per call (no tempfile dependency; the tests remove
    /// what they create).
    fn scratch_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "trace-prune-{}-{}-{}",
            label,
            std::process::id(),
            DIR_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// Write one Parquet file per day under `<root>/events-compacted/dt=<day>/`
    /// — the layout the compactor produces (no partition column in-file; the
    /// day lives only in the directory name).
    fn write_daily_partitions(root: &Path, days: &[&str]) {
        for day in days {
            let dir = root.join(format!("events-compacted/dt={}", day));
            fs::create_dir_all(&dir).expect("create partition dir");
            let conn = Connection::open_in_memory().expect("open scratch connection");
            conn.execute_batch(&format!(
                "COPY (SELECT TIMESTAMP '{day} 10:00:00' AS ts,
                              'sess-{day}' AS session_id,
                              'pageview' AS type,
                              MAP{{'utm_source': 'taboola'}} AS params)
                 TO '{}'
                 (FORMAT parquet);",
                dir.join("part-00000.parquet").display()
            ))
            .expect("write partition file");
        }
    }

    /// `TABLE_SCAN -> Total Files Read` from EXPLAIN ANALYZE of `sql`.
    fn files_scanned(conn: &Connection, sql: &str) -> usize {
        let mut stmt = conn
            .prepare(&format!("EXPLAIN ANALYZE {}", sql))
            .expect("prepare EXPLAIN ANALYZE");
        let plan: String = stmt
            .query_map([], |row| row.get::<usize, String>(1))
            .expect("query plan")
            .map(|r| r.expect("plan row"))
            .collect();
        plan.lines()
            .find_map(|line| {
                let idx = line.find("Total Files Read:")?;
                line[idx + "Total Files Read:".len()..]
                    .split_whitespace()
                    .next()?
                    .parse::<usize>()
                    .ok()
            })
            .unwrap_or_else(|| panic!("no 'Total Files Read' in plan:\n{}", plan))
    }

    /// Parquet-mode config pointing at nothing: the tests build their own
    /// views over local files and never touch S3.
    fn parquet_config() -> Config {
        Config {
            s3_bucket: "unused".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_prefix: "unused".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_endpoint: None,
            data_path: "/tmp".to_string(),
            reports_output_path: "/tmp".to_string(),
            iceberg_catalog_uri: None,
            iceberg_warehouse: None,
            compat_event_views: false,
        }
    }

    fn day_window(day: &str, next_day: &str) -> String {
        format!(
            "ts >= '{}'::TIMESTAMP AND ts < '{}'::TIMESTAMP",
            day, next_day
        )
    }

    #[test]
    fn test_partition_predicate_prunes_files_scanned() {
        let root = scratch_dir("predicate");
        let days = ["2026-09-01", "2026-09-02", "2026-09-03"];
        write_daily_partitions(&root, &days);

        let conn = Connection::open_in_memory().expect("open connection");
        let view_sql = hive_parquet_view_sql(
            "parquet_events_compacted",
            &format!("{}/**/*.parquet", root.join("events-compacted").display()),
        );
        conn.execute(&view_sql, params![]).expect("create view");

        // The predicate the renderer emits for the events table in Parquet
        // mode, for the middle day only.
        let predicate = partition_predicate(Some("dt"), "2026-09-02", "2026-09-03");

        let pruned = files_scanned(
            &conn,
            &format!(
                "SELECT COUNT(*) FROM parquet_events_compacted WHERE {} AND {}",
                day_window("2026-09-02", "2026-09-03"),
                predicate
            ),
        );
        assert_eq!(
            pruned, 1,
            "partition predicate must read exactly 1 of 3 files"
        );

        let baseline = files_scanned(
            &conn,
            &format!(
                "SELECT COUNT(*) FROM parquet_events_compacted WHERE {}",
                day_window("2026-09-02", "2026-09-03")
            ),
        );
        assert_eq!(baseline, 3, "timestamp-only filter must read all 3 files");

        // Row counts must agree: pruning changes I/O, not results
        let pruned_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM parquet_events_compacted WHERE {} AND {}",
                    day_window("2026-09-02", "2026-09-03"),
                    predicate
                ),
                [],
                |row| row.get(0),
            )
            .expect("count pruned");
        let baseline_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM parquet_events_compacted WHERE {}",
                    day_window("2026-09-02", "2026-09-03")
                ),
                [],
                |row| row.get(0),
            )
            .expect("count baseline");
        assert_eq!(
            pruned_rows, baseline_rows,
            "pruning must not change results"
        );

        fs::remove_dir_all(&root).ok();
    }

    /// End-to-end through the report runner's rendering path: a shipped
    /// template (daily_summary) rendered for Parquet mode must carry the
    /// `dt` partition predicate and actually prune; the same template with
    /// the placeholder blanked must not.
    #[test]
    fn test_rendered_daily_summary_report_prunes_files_scanned() {
        let root = scratch_dir("report");
        let days = ["2026-09-01", "2026-09-02", "2026-09-03"];
        write_daily_partitions(&root, &days);

        let conn = Connection::open_in_memory().expect("open connection");
        let view_sql = hive_parquet_view_sql(
            "parquet_events",
            &format!("{}/**/*.parquet", root.join("events-compacted").display()),
        );
        conn.execute(&view_sql, params![]).expect("create view");

        let config = parquet_config();
        let db = DuckDBClient { conn };
        let params = ReportParams {
            s3_path: None,
            start_date: Some("2026-09-02".to_string()),
            end_date: Some("2026-09-03".to_string()),
        };

        let template = include_str!("../queries/daily_summary.sql");
        let sql = render_template_with_client(template, &params, &db, &config);
        assert!(
            sql.contains("dt >= '2026-09-02'::DATE"),
            "rendered report must filter the partition column:\n{}",
            sql
        );

        let pruned = files_scanned(db.connection(), &sql);
        assert_eq!(pruned, 1, "rendered report must read exactly 1 of 3 files");

        // Control: strip the partition conjunct — same rows, every file read.
        // If this ever starts reading 1 file too, the assertion above is
        // passing for the wrong reason.
        let sql_no_partition = sql
            .lines()
            .filter(|line| !line.contains("dt >= '2026-09-02'::DATE"))
            .collect::<Vec<_>>()
            .join("\n");
        let unpruned = files_scanned(db.connection(), &sql_no_partition);
        assert_eq!(unpruned, 3, "timestamp-only variant must read all 3 files");

        // Results agree with and without the partition filter
        let with_rows: i64 = db
            .connection()
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({}) q",
                    sql.trim().trim_end_matches(';')
                ),
                [],
                |row| row.get(0),
            )
            .expect("run rendered report");
        let without_rows: i64 = db
            .connection()
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM ({}) q",
                    sql_no_partition.trim().trim_end_matches(';')
                ),
                [],
                |row| row.get(0),
            )
            .expect("run unpruned report");
        assert_eq!(with_rows, without_rows);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn test_iceberg_mode_partition_filter_renders_true() {
        let config = Config {
            iceberg_catalog_uri: Some("http://catalog:8181".to_string()),
            iceberg_warehouse: Some("s3://bucket/iceberg".to_string()),
            ..parquet_config()
        };
        let db = DuckDBClient {
            conn: Connection::open_in_memory().expect("open connection"),
        };

        // Hidden partitioning: no dt/started_at_day column to reference, so
        // the placeholder must render as TRUE (the ts range prunes instead)
        assert_eq!(db.events_partition_column(&config), None);
        assert_eq!(db.sessions_partition_column(&config), None);
        assert_eq!(
            crate::queries::partition_predicate(None, "2026-09-02", "2026-09-03"),
            "TRUE"
        );

        let params = ReportParams {
            s3_path: None,
            start_date: Some("2026-09-02".to_string()),
            end_date: Some("2026-09-03".to_string()),
        };
        let sql = crate::queries::render_template_with_client(
            include_str!("../queries/daily_sessions.sql"),
            &params,
            &db,
            &config,
        );
        assert!(sql.contains("AND TRUE"), "iceberg rendering:\n{}", sql);
        assert!(sql.contains("iceberg_sessions"));
        assert!(!sql.contains("{{"), "unrendered placeholder:\n{}", sql);
    }

    #[test]
    fn test_parquet_mode_rendering_resolves_all_placeholders() {
        let config = parquet_config();
        let db = DuckDBClient {
            conn: Connection::open_in_memory().expect("open connection"),
        };
        let params = ReportParams {
            s3_path: None,
            start_date: Some("2026-09-02".to_string()),
            end_date: Some("2026-09-03".to_string()),
        };

        for report in crate::queries::list_reports() {
            // Each template must carry the partition predicate of the table
            // it reads: `dt` for the event reports, `started_at_day` for the
            // sessions report. Both the date-spliced form and the
            // expression-bound rolling-window form render as `<col> >= …`.
            let expected_column = if report.sql_template.contains("{{sessions_table}}") {
                "started_at_day"
            } else {
                "dt"
            };
            let sql = crate::queries::render_template_with_client(
                &report.sql_template,
                &params,
                &db,
                &config,
            );
            assert!(
                !sql.contains("{{"),
                "report '{}' left unrendered placeholders:\n{}",
                report.name,
                sql
            );
            assert!(
                sql.contains(&format!("{} >=", expected_column)),
                "report '{}' lost its {} partition predicate:\n{}",
                report.name,
                expected_column,
                sql
            );
        }
    }

    /// The sessions side of the same guarantee: the shipped daily_sessions
    /// report must prune via `started_at_day` the way the event reports
    /// prune via `dt`, and must actually execute against the materialized
    /// session schema.
    #[test]
    fn test_rendered_daily_sessions_report_prunes_files_scanned() {
        let root = scratch_dir("sessions");
        let days = ["2026-09-01", "2026-09-02", "2026-09-03"];
        for day in &days {
            let dir = root.join(format!("started_at_day={}", day));
            fs::create_dir_all(&dir).expect("create session partition dir");
            let conn = Connection::open_in_memory().expect("open scratch connection");
            conn.execute_batch(&format!(
                "COPY (SELECT TIMESTAMP '{day} 09:00:00' AS started_at,
                              'taboola' AS network,
                              3 AS pageviews, 1 AS clicks,
                              FALSE AS converted, 45.0 AS duration_seconds,
                              FALSE AS bounce)
                 TO '{}' (FORMAT parquet);",
                dir.join("part-00000.parquet").display()
            ))
            .expect("write session partition file");
        }

        let conn = Connection::open_in_memory().expect("open connection");
        let view_sql = hive_parquet_view_sql(
            "parquet_sessions",
            &format!("{}/**/*.parquet", root.display()),
        );
        conn.execute(&view_sql, params![]).expect("create view");

        let config = parquet_config();
        let db = DuckDBClient { conn };
        let params = ReportParams {
            s3_path: None,
            start_date: Some("2026-09-02".to_string()),
            end_date: Some("2026-09-03".to_string()),
        };

        let sql = render_template_with_client(
            include_str!("../queries/daily_sessions.sql"),
            &params,
            &db,
            &config,
        );
        assert!(
            sql.contains("started_at_day >= '2026-09-02'::DATE"),
            "sessions report must filter the started_at_day partition column:\n{}",
            sql
        );

        let pruned = files_scanned(db.connection(), &sql);
        assert_eq!(
            pruned, 1,
            "rendered sessions report must read exactly 1 of 3 files"
        );

        // The report must also execute end to end over the materialized
        // session schema (this is what fails if the view and the DDL drift).
        db.connection()
            .execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE daily_sessions_out AS {}",
                sql
            ))
            .expect("run rendered sessions report");
        let rows: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM daily_sessions_out", [], |row| {
                row.get(0)
            })
            .expect("count output rows");
        assert_eq!(rows, 1, "one day x one network in the pruned window");

        fs::remove_dir_all(&root).ok();
    }

    /// The `compat_event_views` opt-in: a bucket holding files from more
    /// than one flusher generation cannot be read by the plain views (the
    /// first file's schema wins; the other files error), while the
    /// compat views project every generation onto the canonical schema.
    #[test]
    fn test_compat_event_views_opt_in_spans_generations() {
        let root = scratch_dir("compat");
        // Flushers write dt= directories, so the files sit one level down.
        let ev1_dir = root.join("events/dt=2026-09-01");
        let ev3_dir = root.join("events/dt=2026-09-02");
        fs::create_dir_all(&ev1_dir).expect("create EV1 partition dir");
        fs::create_dir_all(&ev3_dir).expect("create EV3 partition dir");

        let scratch = Connection::open_in_memory().expect("open scratch connection");
        // EV1: no identity columns, params as a JSON string
        scratch
            .execute_batch(&format!(
                "COPY (SELECT TIMESTAMP '2026-09-01 10:00:00' AS ts, '1.2.3.4' AS ip,
                              'ua' AS ua, 'http://a/1' AS url, 'pageview' AS type,
                              '{{\"utm_source\":\"taboola\"}}' AS params)
                 TO '{}' (FORMAT parquet);",
                ev1_dir.join("part-00000.parquet").display()
            ))
            .expect("write EV1 file");
        // EV3: identity columns present, params as a MAP
        scratch
            .execute_batch(&format!(
                "COPY (SELECT TIMESTAMP '2026-09-02 10:00:00' AS ts, '1.2.3.4' AS ip,
                              'ua' AS ua, 'http://a/2' AS url, 'pageview' AS type,
                              'sess-2' AS session_id, 'user-2' AS user_id,
                              MAP{{'utm_source': 'mgid'}} AS params)
                 TO '{}' (FORMAT parquet);",
                ev3_dir.join("part-00000.parquet").display()
            ))
            .expect("write EV3 file");

        let s3_path = root.display().to_string();

        // Default: plain hive views. Creation is lazy, but the first query
        // fails — VARCHAR and MAP `params` cannot unify in one scan.
        let plain_config = parquet_config();
        let plain_db = DuckDBClient {
            conn: Connection::open_in_memory().expect("open connection"),
        };
        plain_db
            .setup_parquet_views(&s3_path, &plain_config)
            .expect("plain setup succeeds (views are lazy)");
        let plain_result: Result<i64, _> = plain_db.connection().query_row(
            "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' IS NOT NULL",
            [],
            |row| row.get(0),
        );
        assert!(
            plain_result.is_err(),
            "mixed-generation bucket must fail the plain event view"
        );

        // Opt-in: the compat views span both generations.
        let mut compat_config = parquet_config();
        compat_config.compat_event_views = true;
        let compat_db = DuckDBClient {
            conn: Connection::open_in_memory().expect("open connection"),
        };
        compat_db
            .setup_parquet_views(&s3_path, &compat_config)
            .expect("compat setup");
        let count: i64 = compat_db
            .connection()
            .query_row("SELECT COUNT(*) FROM parquet_events", [], |row| row.get(0))
            .expect("compat count");
        assert_eq!(count, 2, "both generations are visible");
        let legacy_rows: i64 = compat_db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM parquet_events WHERE session_id IS NULL",
                [],
                |row| row.get(0),
            )
            .expect("legacy identity check");
        assert_eq!(legacy_rows, 1, "EV1 rows read with NULL identity");
        let with_source: i64 = compat_db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .expect("params check");
        assert_eq!(
            with_source, 2,
            "legacy JSON-string params must convert to the canonical MAP"
        );

        fs::remove_dir_all(&root).ok();
    }
}
