//! Event Parquet schema-version compatibility.
//!
//! The events glob (`<prefix>/events/**/*.parquet`) holds files written by
//! three flusher generations, documented in
//! `docs/analytics/event_schema_versions.md`:
//!
//! - **EV1** — `ts, ip, ua, url, params (JSON VARCHAR), type`. No identity
//!   columns at all: the session/user analytics queries see NULL.
//! - **EV2** — EV1 + `session_id`, `user_id`. `params` is still a JSON
//!   VARCHAR string.
//! - **EV3** — the current flusher schema: the full `trace.ad_events`
//!   column set with `params` as a real `MAP(VARCHAR, VARCHAR)`.
//!
//! A single `read_parquet` glob cannot span those generations: without
//! `union_by_name` the schema is taken from the first file (files missing
//! later columns error), and with it the `params` column still has to unify
//! VARCHAR (EV1/EV2) with MAP (EV3), which is not castable in either
//! direction. The failure surfaces at query time, not at view creation, so
//! a plain view over the glob looks healthy until the first report runs.
//!
//! This module classifies each file under the glob by the *physical* type of
//! its `params` column (a footer read via `parquet_schema` — no data scan)
//! and builds a view that projects the canonical column set from each
//! generation separately, converting legacy JSON-string `params` to the
//! canonical MAP. The view also exposes a `dt` day column derived from `ts`
//! so partition-style predicates (`dt BETWEEN ...`) keep working across
//! layouts that name their directories differently (`dt=`, `date=`, or flat).
//! Deriving costs file-skipping on this view; homogeneous buckets keep the
//! pruning views in [`crate::duckdb::DuckDBClient::setup_parquet_views`].

use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashSet;

/// The canonical event column set: the flusher's current Parquet schema
/// (flusher/src/main.rs), which is also the `trace.ad_events` DDL
/// (`analytics/schemas/ad_events_iceberg.sql`) as a set — the DDL declares
/// `params` mid-table, the flusher writes it last; only membership is
/// contractual.
///
/// Each entry is (column name, DuckDB type). The type is used to synthesize
/// typed NULLs for columns a file generation never wrote — and only for
/// that; the physical types come from the files themselves.
///
/// A regression test pins this list to the DDL, so a column added to one
/// without the other fails `cargo test`.
pub const CANONICAL_COLUMNS: &[(&str, &str)] = &[
    ("ts", "TIMESTAMP"),
    ("ip", "VARCHAR"),
    ("ua", "VARCHAR"),
    ("url", "VARCHAR"),
    ("type", "VARCHAR"),
    ("session_id", "VARCHAR"),
    ("user_id", "VARCHAR"),
    ("cookie_id", "VARCHAR"),
    ("network", "VARCHAR"),
    ("campaign_id", "VARCHAR"),
    ("campaign_name", "VARCHAR"),
    ("creative_id", "VARCHAR"),
    ("headline", "VARCHAR"),
    ("image_id", "VARCHAR"),
    ("item_id", "VARCHAR"),
    // V002 (migrations/V002__add_referrer_attribution.sql)
    ("referrer", "VARCHAR"),
    ("referrer_network", "VARCHAR"),
    ("attribution_campaign_id", "VARCHAR"),
    ("attribution_creative_id", "VARCHAR"),
    ("attribution_touches", "BIGINT"),
    ("attribution_days_to_convert", "BIGINT"),
    ("device_type", "VARCHAR"),
    ("device_os", "VARCHAR"),
    ("device_browser", "VARCHAR"),
    // V003 (migrations/V003__add_engagement_metrics.sql)
    ("scroll_depth_pct", "BIGINT"),
    ("scroll_time_ms", "BIGINT"),
    ("dwell_time_ms", "BIGINT"),
    ("dwell_visible_pct", "BIGINT"),
    ("viewport_width", "BIGINT"),
    ("viewport_height", "BIGINT"),
    // V004 (migrations/V004__add_quality_scores.sql)
    ("quality_score", "DOUBLE"),
    ("bot_probability", "DOUBLE"),
    ("fraud_score", "DOUBLE"),
    ("is_valid", "BOOLEAN"),
    ("is_verified", "BOOLEAN"),
    ("validation_reason", "VARCHAR"),
    ("enriched_at", "TIMESTAMP"),
    ("enrichment_version", "VARCHAR"),
    // Raw parameters
    ("params", "MAP(VARCHAR, VARCHAR)"),
];

/// Convert a legacy JSON-string `params` column to the canonical MAP form.
///
/// `CAST(varchar AS MAP)` does not exist in DuckDB, so the map is rebuilt
/// from its keys. `json_valid` gates the conversion: NULL and malformed
/// strings (the old flusher wrote `unwrap_or_default()`, i.e. `""`, when
/// serialization failed) become NULL rather than erroring the query.
const JSON_PARAMS_TO_MAP: &str = "CASE WHEN json_valid(params) \
     THEN map(json_keys(params), \
              list_transform(json_keys(params), \
                             k -> json_extract_string(params, '$.\"' || k || '\"'))) \
     ELSE NULL END";

/// Files under a glob, split by the physical type of their `params` column,
/// with each side's available column set.
///
/// `legacy_params`/`map_params` hold the per-file names; `legacy_columns`/
/// `map_columns` are the union of top-level column names seen across each
/// side's files (a column is addressable on a side if *any* of its files
/// has it — `union_by_name` fills NULL for the rest).
#[derive(Debug, Default)]
pub struct EventFileClasses {
    pub legacy_params: Vec<String>,
    pub map_params: Vec<String>,
    pub legacy_columns: HashSet<String>,
    pub map_columns: HashSet<String>,
}

impl EventFileClasses {
    pub fn is_mixed(&self) -> bool {
        !self.legacy_params.is_empty() && !self.map_params.is_empty()
    }
}

/// Classify every Parquet file under `glob` by its `params` physical type.
///
/// In a Parquet footer a MAP column is a group node — `type` is NULL and a
/// repeated `key_value` child follows — while a VARCHAR column is a plain
/// BYTE_ARRAY leaf. `parquet_schema` reads only footers, so this costs one
/// metadata request per file regardless of file size.
pub fn classify_event_files(conn: &Connection, glob: &str) -> Result<EventFileClasses> {
    let presence: String = CANONICAL_COLUMNS
        .iter()
        .map(|(name, _)| format!("MAX(CASE WHEN name = '{}' THEN 1 ELSE 0 END)", name))
        .collect::<Vec<_>>()
        .join(",\n           ");

    let sql = format!(
        "SELECT file_name,
           MAX(CASE WHEN name = 'params' AND type IS NULL THEN 1 ELSE 0 END) AS map_params,
           {}
        FROM parquet_schema('{}')
        GROUP BY file_name",
        presence, glob
    );

    let mut stmt = conn
        .prepare(&sql)
        .context("Classifying event Parquet footers failed")?;
    let width = CANONICAL_COLUMNS.len();

    let mut classes = EventFileClasses::default();
    let rows = stmt
        .query_map([], |row| {
            let mut flags = Vec::with_capacity(width);
            for i in 0..width {
                flags.push(row.get::<_, i64>(2 + i)? != 0);
            }
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0, flags))
        })
        .context("Reading event Parquet footer classification failed")?;

    for entry in rows {
        let (file_name, is_map, flags) = entry?;
        let (names, columns) = if is_map {
            (&mut classes.map_params, &mut classes.map_columns)
        } else {
            (&mut classes.legacy_params, &mut classes.legacy_columns)
        };
        names.push(file_name);
        for ((col, _), present) in CANONICAL_COLUMNS.iter().zip(flags) {
            if present {
                columns.insert(col.to_string());
            }
        }
    }

    Ok(classes)
}

/// SQL literal for an explicit Parquet file list: `['a', 'b']`.
fn file_list_literal(files: &[String]) -> String {
    let quoted: Vec<String> = files
        .iter()
        .map(|f| format!("'{}'", f.replace('\'', "''")))
        .collect();
    format!("[{}]", quoted.join(", "))
}

/// One side of the compatibility view: the canonical projection over a set
/// of same-`params`-type files.
///
/// Columns that generation never wrote become typed NULLs; legacy JSON
/// `params` becomes the canonical MAP. `dt` is derived from `ts` so day
/// predicates work no matter how (or whether) the directories are named.
fn side_select(columns: &HashSet<String>, legacy_params: bool) -> String {
    let mut projections: Vec<String> = CANONICAL_COLUMNS
        .iter()
        .map(|(name, ty)| match *name {
            "params" => {
                if columns.contains("params") {
                    if legacy_params {
                        format!("{} AS params", JSON_PARAMS_TO_MAP)
                    } else {
                        "params".to_string()
                    }
                } else {
                    format!("NULL::{} AS params", ty)
                }
            }
            name if columns.contains(name) => name.to_string(),
            name => format!("NULL::{} AS {}", ty, name),
        })
        .collect();
    projections.push("CAST(ts AS DATE) AS dt".to_string());
    projections.join(",\n    ")
}

/// Build the `CREATE OR REPLACE VIEW` statement exposing the canonical
/// event schema over exactly the files `classes` was built from (explicit
/// file lists — the view is pinned to them and will not see files that land
/// later).
///
/// All-current globs get a single scan (with `union_by_name` so files from
/// different EV3 point-releases that lack the newest columns still read as
/// NULL instead of erroring). Any legacy-`params` file adds a second scan
/// and a `UNION ALL`, because VARCHAR and MAP `params` cannot share one.
pub fn compat_events_view_sql(view_name: &str, classes: &EventFileClasses) -> String {
    let mut sides: Vec<String> = Vec::new();

    if !classes.legacy_params.is_empty() {
        sides.push(format!(
            "SELECT\n    {}\nFROM read_parquet({}, union_by_name = true)",
            side_select(&classes.legacy_columns, true),
            file_list_literal(&classes.legacy_params)
        ));
    }
    if !classes.map_params.is_empty() {
        sides.push(format!(
            "SELECT\n    {}\nFROM read_parquet({}, union_by_name = true)",
            side_select(&classes.map_columns, false),
            file_list_literal(&classes.map_params)
        ));
    }

    assert!(
        !sides.is_empty(),
        "compat views are only built over globs with at least one classified file"
    );

    format!(
        "CREATE OR REPLACE VIEW {} AS\n{};",
        view_name,
        sides.join("\nUNION ALL\n")
    )
}

/// Like [`compat_events_view_sql`], but when no legacy files were
/// classified the view re-scans `glob` instead of pinning the classified
/// file list, so newly written files are picked up without rebuilding the
/// view — matching how the plain views in
/// [`crate::duckdb::DuckDBClient::setup_parquet_views`] behave on a
/// homogeneous bucket.
pub fn compat_events_view_sql_for_glob(
    view_name: &str,
    glob: &str,
    classes: &EventFileClasses,
) -> String {
    if classes.legacy_params.is_empty() && !classes.map_params.is_empty() {
        format!(
            "CREATE OR REPLACE VIEW {} AS\nSELECT\n    {}\nFROM read_parquet('{}', union_by_name = true);",
            view_name,
            side_select(&classes.map_columns, false),
            glob.replace('\'', "''")
        )
    } else {
        compat_events_view_sql(view_name, classes)
    }
}

/// Create the compatibility views for the two event globs under `s3_path`
/// (`events/` and `events-compacted/`).
///
/// Drop-in replacement for the two event views in
/// [`crate::duckdb::DuckDBClient::setup_parquet_views`]: same view names,
/// canonical schema plus derived `dt`. Classification errors (an unreadable
/// footer, or a glob with no files at all) propagate — the same failures a
/// plain `read_parquet` view over the glob would hit.
pub fn setup_compat_events_views(conn: &Connection, s3_path: &str) -> Result<()> {
    let globs = [
        (format!("{}/events/**/*.parquet", s3_path), "parquet_events"),
        (
            format!("{}/events-compacted/**/*.parquet", s3_path),
            "parquet_events_compacted",
        ),
    ];

    for (glob, view_name) in globs {
        let classes = match classify_event_files(conn, &glob) {
            Ok(classes) => classes,
            Err(error)
                if format!("{error:#}").contains("No files found that match the pattern") =>
            {
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("Classifying {}", glob));
            }
        };
        tracing::info!(
            legacy_files = classes.legacy_params.len(),
            map_files = classes.map_params.len(),
            mixed = classes.is_mixed(),
            glob = %glob,
            "created {} over classified event files; day-directory pruning is not available on this view",
            view_name
        );
        let sql = compat_events_view_sql_for_glob(view_name, &glob, &classes);
        conn.execute_batch(&sql)
            .with_context(|| format!("Creating {} view failed", view_name))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    static DIR_SEQ: AtomicU32 = AtomicU32::new(0);

    /// Unique temp dir per call (no tempfile dependency; tests remove what
    /// they create).
    fn scratch_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "trace-events-compat-{}-{}-{}",
            label,
            std::process::id(),
            DIR_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// Single-value query result as a string, regardless of the value's
    /// engine type ("NULL" for SQL NULL).
    fn scalar(conn: &Connection, sql: &str) -> String {
        let wrapped = format!(
            "SELECT CAST(v.c AS VARCHAR) FROM ({}) v(c)",
            sql.trim().trim_end_matches(';').trim()
        );
        let value: Option<String> = conn.query_row(&wrapped, [], |row| row.get(0)).unwrap();
        value.unwrap_or_else(|| "NULL".to_string())
    }

    /// Write an EV1 file: params as a JSON string, no identity columns.
    fn write_ev1(root: &Path, name: &str, ts: &str, url: &str, params: &str) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES
                (TIMESTAMP '{ts}', '10.0.0.1', 'ua/1.0', '{url}', 'pageview', '{params}'))
                AS t(ts, ip, ua, url, type, params))
             TO '{}' (FORMAT PARQUET);",
            root.join(name).display()
        ))
        .unwrap();
    }

    /// Write an EV2 file: EV1 + session_id/user_id, params still a JSON string.
    #[allow(clippy::too_many_arguments)]
    fn write_ev2(
        root: &Path,
        name: &str,
        ts: &str,
        url: &str,
        params: &str,
        session_id: &str,
        user_id: &str,
        event_type: &str,
    ) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES
                (TIMESTAMP '{ts}', '10.0.0.1', 'ua/1.0', '{url}', '{event_type}', '{params}', '{session_id}', '{user_id}'))
                AS t(ts, ip, ua, url, type, params, session_id, user_id))
             TO '{}' (FORMAT PARQUET);",
            root.join(name).display()
        ))
        .unwrap();
    }

    /// Write an EV3 file: full canonical schema with params as a MAP. Only
    /// the columns with values worth asserting on get non-NULL data; the
    /// rest exist so the file's schema is complete.
    #[allow(clippy::too_many_arguments)]
    fn write_ev3(
        root: &Path,
        name: &str,
        ts: &str,
        url: &str,
        params_map: &str,
        session_id: &str,
        user_id: &str,
        event_type: &str,
    ) {
        let conn = Connection::open_in_memory().unwrap();
        let names: Vec<&str> = CANONICAL_COLUMNS.iter().map(|(n, _)| *n).collect();
        let values: Vec<String> = names
            .iter()
            .map(|n| match *n {
                "ts" => format!("TIMESTAMP '{}'", ts),
                "ip" => "'10.0.0.2'".to_string(),
                "ua" => "'ua/2.0'".to_string(),
                "url" => format!("'{}'", url),
                "type" => format!("'{}'", event_type),
                "session_id" => format!("'{}'", session_id),
                "user_id" => format!("'{}'", user_id),
                "cookie_id" => "'cookie-1'".to_string(),
                "network" => "'taboola'".to_string(),
                "campaign_id" => "'camp-1'".to_string(),
                "referrer" => "'https://google.com/'".to_string(),
                "quality_score" => "0.9".to_string(),
                "params" => params_map.to_string(),
                _ => "NULL".to_string(),
            })
            .collect();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ({})) AS t({}))
             TO '{}' (FORMAT PARQUET);",
            values.join(", "),
            names.join(", "),
            root.join(name).display()
        ))
        .unwrap();
    }

    fn glob_of(root: &Path) -> String {
        format!("{}/**/*.parquet", root.display())
    }

    // ------------------------------------------------------------------
    // Classification
    // ------------------------------------------------------------------

    #[test]
    fn classify_splits_files_by_params_physical_type() {
        let dir = scratch_dir("classify");
        write_ev1(
            &dir,
            "ev1.parquet",
            "2026-05-01 10:00:00",
            "http://a/",
            "{\"utm_source\":\"taboola\"}",
        );
        write_ev2(
            &dir,
            "ev2.parquet",
            "2026-05-02 10:00:00",
            "http://a/",
            "{\"utm_source\":\"taboola\"}",
            "s-2",
            "u-2",
            "pageview",
        );
        write_ev3(
            &dir,
            "ev3.parquet",
            "2026-05-03 10:00:00",
            "http://a/",
            "MAP{'utm_source': 'taboola'}",
            "s-3",
            "u-3",
            "pageview",
        );

        let conn = Connection::open_in_memory().unwrap();
        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();

        assert_eq!(
            classes.legacy_params.len(),
            2,
            "EV1 + EV2 are legacy-params"
        );
        assert_eq!(classes.map_params.len(), 1, "EV3 is map-params");
        assert!(classes.is_mixed());
        // Column availability is per side, from the files themselves
        assert!(!classes.legacy_columns.contains("cookie_id"));
        assert!(
            classes.legacy_columns.contains("session_id"),
            "EV2 contributes it"
        );
        assert!(classes.map_columns.contains("cookie_id"));
        assert!(classes.map_columns.contains("quality_score"));

        fs::remove_dir_all(&dir).unwrap();
    }

    // ------------------------------------------------------------------
    // View SQL shape
    // ------------------------------------------------------------------

    #[test]
    fn fast_path_is_a_single_scan_of_the_glob() {
        let classes = EventFileClasses {
            map_params: vec!["/tmp/new.parquet".to_string()],
            map_columns: CANONICAL_COLUMNS
                .iter()
                .map(|(n, _)| n.to_string())
                .collect(),
            ..Default::default()
        };
        let sql = compat_events_view_sql_for_glob(
            "parquet_events",
            "s3://b/p/events/**/*.parquet",
            &classes,
        );
        assert!(
            sql.contains("FROM read_parquet('s3://b/p/events/**/*.parquet', union_by_name = true)")
        );
        assert!(!sql.contains("UNION ALL"), "no legacy side to union");
        assert!(!sql.contains("json_valid"), "no conversion needed");
    }

    #[test]
    fn mixed_path_unions_legacy_and_current_scans() {
        let classes = EventFileClasses {
            legacy_params: vec!["/tmp/old.parquet".to_string()],
            map_params: vec!["/tmp/new.parquet".to_string()],
            legacy_columns: [
                "ts",
                "ip",
                "ua",
                "url",
                "type",
                "params",
                "session_id",
                "user_id",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            map_columns: CANONICAL_COLUMNS
                .iter()
                .map(|(n, _)| n.to_string())
                .collect(),
        };
        let sql = compat_events_view_sql("parquet_events", &classes);
        assert!(sql.contains("read_parquet(['/tmp/old.parquet'], union_by_name = true)"));
        assert!(sql.contains("read_parquet(['/tmp/new.parquet'], union_by_name = true)"));
        assert_eq!(sql.matches("UNION ALL").count(), 1);
        // Legacy params go through the JSON->MAP conversion, current do not
        assert_eq!(sql.matches("json_valid(params)").count(), 1);
        // Columns the legacy side never wrote are typed NULLs
        assert!(sql.contains("NULL::VARCHAR AS cookie_id"));
        assert!(sql.contains("NULL::DOUBLE AS quality_score"));
    }

    #[test]
    fn legacy_side_without_params_column_synthesizes_a_typed_null() {
        // An ancient file with no params column at all must not break view
        // creation with an unbound `params` reference.
        let classes = EventFileClasses {
            legacy_params: vec!["/tmp/ancient.parquet".to_string()],
            legacy_columns: ["ts", "ip", "ua", "url", "type"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            ..Default::default()
        };
        let sql = compat_events_view_sql("parquet_events", &classes);
        assert!(sql.contains("NULL::MAP(VARCHAR, VARCHAR) AS params"));
        assert!(!sql.contains("json_valid"));
    }

    // ------------------------------------------------------------------
    // End to end against old and new Parquet
    // ------------------------------------------------------------------

    /// Mixed EV1/EV2/EV3 directory: every generation readable through one
    /// view, canonical columns everywhere, params usable via both the
    /// `->>` operator the report templates use and the `['key']` subscript
    /// the session materializer uses.
    #[test]
    fn compat_view_unifies_mixed_generations() {
        let dir = scratch_dir("mixed");
        write_ev1(
            &dir,
            "ev1.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\",\"utm_campaign\":\"c-old\"}",
        );
        write_ev2(
            &dir,
            "ev2.parquet",
            "2026-05-01 10:01:00",
            "http://a/2",
            "{\"utm_source\":\"taboola\",\"utm_campaign\":\"c-old\"}",
            "s-old",
            "u-old",
            "pageview",
        );
        write_ev3(
            &dir,
            "ev3.parquet",
            "2026-05-02 11:00:00",
            "http://a/3",
            "MAP{'utm_source': 'taboola', 'utm_campaign': 'c-new'}",
            "s-new",
            "u-new",
            "click",
        );

        let conn = Connection::open_in_memory().unwrap();
        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        conn.execute_batch(&compat_events_view_sql("parquet_events", &classes))
            .unwrap();

        // All three generations are visible
        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM parquet_events"), "3");

        // session_id: NULL for EV1, real for EV2/EV3
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE session_id IS NOT NULL"
            ),
            "2"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(DISTINCT session_id) FROM parquet_events"
            ),
            "2"
        );

        // params read identically from JSON-string and MAP files
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' = 'taboola'"
            ),
            "3"
        );
        assert_eq!(
            scalar(&conn, "SELECT COUNT(*) FROM parquet_events WHERE params['utm_campaign'] IN ('c-old', 'c-new')"),
            "3"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(DISTINCT params->>'utm_campaign') FROM parquet_events"
            ),
            "2"
        );

        // Later-generation columns are NULL, not missing, on old rows
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE cookie_id IS NULL"
            ),
            "2"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE quality_score IS NOT NULL"
            ),
            "1"
        );
        // Canonical NOT NULL columns survive from every generation
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE url IS NOT NULL AND type IS NOT NULL"
            ),
            "3"
        );

        // Derived day column drives partition-style predicates on any layout
        assert_eq!(
            scalar(&conn, "SELECT COUNT(*) FROM parquet_events WHERE dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-02'"),
            "3"
        );

        // The view's schema is exactly the canonical set plus dt
        let mut stmt = conn.prepare("DESCRIBE parquet_events").unwrap();
        let view_columns: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let mut expected: Vec<&str> = CANONICAL_COLUMNS.iter().map(|(n, _)| *n).collect();
        expected.push("dt");
        assert_eq!(view_columns, expected, "view exposes the canonical schema");

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Legacy-only directory (no EV3 files at all): the view is just the
    /// legacy scan, and EV1's missing identity columns read as NULL.
    #[test]
    fn compat_view_legacy_only_directory() {
        let dir = scratch_dir("legacy-only");
        write_ev1(
            &dir,
            "ev1a.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\"}",
        );
        write_ev1(
            &dir,
            "ev1b.parquet",
            "2026-05-01 10:05:00",
            "http://a/2",
            "{\"utm_source\":\"mgid\"}",
        );
        write_ev2(
            &dir,
            "ev2.parquet",
            "2026-05-01 10:10:00",
            "http://a/3",
            "{\"utm_source\":\"outbrain\"}",
            "s-2",
            "u-2",
            "click",
        );

        let conn = Connection::open_in_memory().unwrap();
        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        assert!(classes.map_params.is_empty());
        conn.execute_batch(&compat_events_view_sql("parquet_events", &classes))
            .unwrap();

        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM parquet_events"), "3");
        // EV1 rows count as events but not as sessions
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE session_id IS NOT NULL"
            ),
            "1"
        );
        // params from every legacy row
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' IS NOT NULL"
            ),
            "3"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// All-current directory with heterogeneous EV3 files (an early EV3
    /// point-release without `enriched_at`): single scan of the glob,
    /// missing columns read as NULL instead of erroring.
    #[test]
    fn compat_view_all_current_with_column_drift() {
        let dir = scratch_dir("all-current");
        // Early EV3: full schema minus enriched_at
        let conn = Connection::open_in_memory().unwrap();
        let names: Vec<&str> = CANONICAL_COLUMNS
            .iter()
            .map(|(n, _)| *n)
            .filter(|n| *n != "enriched_at")
            .collect();
        let values: Vec<String> = names
            .iter()
            .map(|n| match *n {
                "ts" => "TIMESTAMP '2026-05-01 10:00:00'".to_string(),
                "url" => "'http://a/1'".to_string(),
                "type" => "'pageview'".to_string(),
                "session_id" => "'s-early'".to_string(),
                "params" => "MAP{'utm_source': 'taboola'}".to_string(),
                _ => "NULL".to_string(),
            })
            .collect();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES ({})) AS t({})) TO '{}/early.parquet' (FORMAT PARQUET);",
            values.join(", "),
            names.join(", "),
            dir.display()
        ))
        .unwrap();
        write_ev3(
            &dir,
            "late.parquet",
            "2026-05-02 10:00:00",
            "http://a/2",
            "MAP{'utm_source': 'mgid'}",
            "s-late",
            "u-late",
            "pageview",
        );

        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        assert!(classes.legacy_params.is_empty());
        assert!(classes.map_columns.contains("enriched_at"));
        conn.execute_batch(&compat_events_view_sql_for_glob(
            "parquet_events",
            &glob_of(&dir),
            &classes,
        ))
        .unwrap();

        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM parquet_events"), "2");
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE enriched_at IS NULL"
            ),
            "2"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// Malformed legacy params (the old flusher's `unwrap_or_default()` empty
    /// string) degrade to NULL instead of failing every query on the view.
    #[test]
    fn malformed_legacy_params_become_null() {
        let dir = scratch_dir("bad-params");
        write_ev2(
            &dir,
            "ev2.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\"}",
            "s-2",
            "u-2",
            "pageview",
        );
        // Write a file whose params is a non-JSON string
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES (TIMESTAMP '2026-05-01 11:00:00', 'http://a/2', 'pageview', '', 's-bad', 'u-bad'))
                AS t(ts, url, type, params, session_id, user_id))
             TO '{}/bad.parquet' (FORMAT PARQUET);",
            dir.display()
        ))
        .unwrap();

        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        conn.execute_batch(&compat_events_view_sql("parquet_events", &classes))
            .unwrap();

        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM parquet_events"), "2");
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE params IS NULL"
            ),
            "1"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' = 'taboola'"
            ),
            "1"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// The drop-in entry point works over a mixed directory laid out like
    /// the bucket it targets (events/ and events-compacted/ prefixes).
    #[test]
    fn setup_creates_both_views_over_mixed_buckets() {
        let root = scratch_dir("setup");
        let events = root.join("events");
        let compacted = root.join("events-compacted");
        fs::create_dir_all(events.join("dt=2026-05-01")).unwrap();
        fs::create_dir_all(compacted.join("dt=2026-04-01")).unwrap();
        write_ev2(
            &events.join("dt=2026-05-01"),
            "old.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\"}",
            "s-old",
            "u-old",
            "pageview",
        );
        write_ev3(
            &events,
            "new.parquet",
            "2026-05-02 10:00:00",
            "http://a/2",
            "MAP{'utm_source': 'mgid'}",
            "s-new",
            "u-new",
            "click",
        );
        write_ev1(
            &compacted.join("dt=2026-04-01"),
            "ancient.parquet",
            "2026-04-01 10:00:00",
            "http://a/0",
            "{\"utm_source\":\"outbrain\"}",
        );

        let conn = Connection::open_in_memory().unwrap();
        setup_compat_events_views(&conn, &root.display().to_string()).unwrap();

        assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM parquet_events"), "2");
        assert_eq!(
            scalar(&conn, "SELECT COUNT(*) FROM parquet_events_compacted"),
            "1"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(*) FROM parquet_events WHERE params->>'utm_source' IS NOT NULL"
            ),
            "2"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    // ------------------------------------------------------------------
    // Affected report templates over the compatibility view
    // ------------------------------------------------------------------

    /// Render a shipped report template the way the service does, but
    /// against an explicit events source (the test's view) — the templates
    /// are parameterized by `{{events_table}}`, so pointing that at the
    /// compatibility view is the whole integration.
    fn render_template(template: &str, events_table: &str, start: &str, end: &str) -> String {
        template
            .replace("{{events_table}}", events_table)
            .replace(
                "{{ts_partition_filter}}",
                "dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-02'",
            )
            .replace("{{start_date}}", start)
            .replace("{{end_date}}", end)
    }

    /// The session-dependent report templates — the ones that made the
    /// generic schema's missing `session_id` a real gap — run unchanged
    /// against a mixed-generation bucket through the compatibility view.
    #[test]
    fn session_reports_run_against_mixed_generations() {
        let dir = scratch_dir("reports");
        // One old-style session (EV2) with a pageview and a click
        write_ev2(
            &dir,
            "ev2a.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\",\"utm_campaign\":\"c-old\",\"revenue\":\"0\"}",
            "s-old",
            "u-old",
            "pageview",
        );
        write_ev2(
            &dir,
            "ev2b.parquet",
            "2026-05-01 10:01:00",
            "http://a/2",
            "{\"utm_source\":\"taboola\",\"utm_campaign\":\"c-old\"}",
            "s-old",
            "u-old",
            "click",
        );
        // One new-style session (EV3) with a conversion carrying revenue
        write_ev3(
            &dir,
            "ev3a.parquet",
            "2026-05-02 11:00:00",
            "http://a/3",
            "MAP{'utm_source': 'taboola', 'utm_campaign': 'c-new', 'revenue': '12.5'}",
            "s-new",
            "u-new",
            "conversion",
        );
        // EV1 noise: counted as events, invisible to session analytics
        write_ev1(
            &dir,
            "ev1.parquet",
            "2026-05-01 12:00:00",
            "http://a/4",
            "{\"utm_source\":\"mgid\"}",
        );

        let conn = Connection::open_in_memory().unwrap();
        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        conn.execute_batch(&compat_events_view_sql("parquet_events", &classes))
            .unwrap();

        // daily_summary: events and sessions per day/source/type. The EV1
        // row counts as an event with no session; both real sessions count.
        let daily = render_template(
            include_str!("../queries/daily_summary.sql"),
            "parquet_events",
            "2026-05-01",
            "2026-05-03",
        );
        assert_eq!(
            scalar(
                &conn,
                &format!(
                    "SELECT SUM(events) FROM ({}) q WHERE CAST(date AS VARCHAR) = '2026-05-01'",
                    daily.trim().trim_end_matches(';').trim()
                )
            ),
            "3",
            "three events on day 1 across EV1+EV2"
        );
        assert_eq!(
            scalar(
                &conn,
                "SELECT COUNT(DISTINCT session_id) FROM parquet_events"
            ),
            "2",
            "one old session, one new session"
        );

        // session_reconstruction: stitches across the mixed files; needs
        // device_type (a V002 column) which the view NULL-fills for EV2
        let reconstruction = render_template(
            include_str!("../queries/session_reconstruction.sql"),
            "parquet_events",
            "2026-05-01",
            "2026-05-03",
        );
        let mut stmt = conn.prepare(&reconstruction).unwrap();
        let sessions: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(sessions.len(), 2, "both generations sessionize");
        assert!(sessions.iter().any(|s| s.starts_with("s-old")));
        assert!(sessions.iter().any(|s| s.starts_with("s-new")));

        // attribution_first_touch: params from both eras feed one funnel;
        // revenue is parsed out of the EV3 MAP params
        let attribution = render_template(
            include_str!("../queries/attribution_first_touch.sql"),
            "parquet_events",
            "2026-05-01",
            "2026-05-03",
        );
        assert_eq!(
            scalar(
                &conn,
                &format!(
                    "SELECT SUM(attributed_revenue) FROM ({}) q",
                    attribution.trim().trim_end_matches(';').trim()
                )
            ),
            "12.50"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    /// The session materializer's sessionization query — a third consumer
    /// with its own column needs (`params['revenue']`, `device_type`) —
    /// runs over the compatibility view too.
    #[test]
    fn sessionization_runs_over_compat_view() {
        use crate::session_materializer::sessionization_query;
        use crate::session_stitcher::SessionConfig;

        let dir = scratch_dir("materializer");
        write_ev2(
            &dir,
            "ev2a.parquet",
            "2026-05-01 10:00:00",
            "http://a/1",
            "{\"utm_source\":\"taboola\"}",
            "s-old",
            "u-old",
            "pageview",
        );
        write_ev2(
            &dir,
            "ev2b.parquet",
            "2026-05-01 10:05:00",
            "http://a/2",
            "{\"utm_source\":\"taboola\"}",
            "s-old",
            "u-old",
            "click",
        );
        write_ev3(
            &dir,
            "ev3.parquet",
            "2026-05-01 10:10:00",
            "http://a/3",
            "MAP{'revenue': '7.5'}",
            "s-new",
            "u-new",
            "conversion",
        );

        let conn = Connection::open_in_memory().unwrap();
        let classes = classify_event_files(&conn, &glob_of(&dir)).unwrap();
        conn.execute_batch(&compat_events_view_sql("parquet_events", &classes))
            .unwrap();

        let day = chrono::NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
        let sql = sessionization_query(day, "parquet_events", &SessionConfig::default());
        let mut stmt = conn.prepare(&sql).unwrap();
        // Column order is pinned by the materializer's DDL projection:
        // 0 session_id, 4 pageviews, 5 clicks, 8 event_count,
        // 17 conversion_value
        let rows: Vec<(String, i32, i32, i32, f64)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(8)?,
                    row.get(17)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(rows.len(), 2, "one session per generation");
        let old = rows.iter().find(|(s, ..)| s.contains("s-old")).unwrap();
        assert_eq!(
            (old.3, old.1, old.2),
            (2, 1, 1),
            "old session: 2 events, 1 pageview, 1 click"
        );
        let new = rows.iter().find(|(s, ..)| s.contains("s-new")).unwrap();
        assert!(
            (new.4 - 7.5).abs() < 1e-9,
            "revenue from EV3 MAP params, got {}",
            new.4
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    // ------------------------------------------------------------------
    // Drift guard: canonical list vs the DDL
    // ------------------------------------------------------------------

    /// Column declarations inside the `trace.ad_events` CREATE TABLE block
    /// of `analytics/schemas/ad_events_iceberg.sql`.
    fn ad_events_ddl_columns() -> Vec<&'static str> {
        let ddl = include_str!("../schemas/ad_events_iceberg.sql");
        let start = ddl
            .lines()
            .position(|l| l.contains("CREATE TABLE IF NOT EXISTS trace.ad_events ("))
            .expect("ad_events CREATE TABLE exists");
        let end = ddl
            .lines()
            .skip(start)
            .position(|line| line.trim() == ")")
            .expect("ad_events table terminator");

        let mut columns = Vec::new();
        for line in ddl.lines().skip(start + 1).take(end) {
            let trimmed = line.trim();
            // Column lines: a bare lowercase identifier followed by a type
            // token. Everything else in the block (comments, blank lines)
            // is skipped.
            let Some((first, rest)) = trimmed.split_once(' ') else {
                continue;
            };
            if first.starts_with("--") || first.contains('(') {
                continue;
            }
            let identifier_ok = first
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            let is_type = [
                "STRING",
                "INT",
                "BIGINT",
                "DOUBLE",
                "BOOLEAN",
                "TIMESTAMP",
                "MAP<",
            ]
            .iter()
            .any(|t| rest.starts_with(t));
            if identifier_ok && is_type {
                columns.push(first);
            }
        }
        columns
    }

    /// Pin `CANONICAL_COLUMNS` to the `trace.ad_events` DDL — the same
    /// discipline the syncer applies to `trace.assets`
    /// (`test_assets_parquet_schema_matches_iceberg_ddl`): the compatibility
    /// view's column set must be the DDL's column set, or a schema change
    /// on one side silently stops matching the other.
    #[test]
    fn canonical_columns_match_ad_events_ddl() {
        let ddl_columns = ad_events_ddl_columns();
        assert!(
            ddl_columns.len() > 10,
            "column parser found suspiciously few ad_events columns: {:?}",
            ddl_columns
        );

        let canonical: HashSet<&str> = CANONICAL_COLUMNS.iter().map(|(n, _)| *n).collect();
        let ddl_set: HashSet<&str> = ddl_columns.iter().copied().collect();

        for (name, _) in CANONICAL_COLUMNS {
            assert!(
                ddl_set.contains(name),
                "canonical column {} missing from the trace.ad_events DDL",
                name
            );
        }
        for col in &ddl_columns {
            assert!(
                canonical.contains(col),
                "DDL column {} missing from CANONICAL_COLUMNS",
                col
            );
        }
        assert_eq!(
            canonical.len(),
            ddl_set.len(),
            "canonical list and DDL must agree on the full column set"
        );
    }
}
