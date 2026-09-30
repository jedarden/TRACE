//! One-time bootstrap from TRACE's existing Hive-partitioned Parquet tree.
//!
//! Run with writers paused and an empty `trace` Iceberg namespace. CTAS copies
//! the existing files into catalog-managed tables; it never deletes or moves
//! the source Parquet objects.

use crate::config::Config;
use anyhow::{bail, Context, Result};
use duckdb::{params, Connection};

#[derive(Debug)]
pub struct MigratedTable {
    pub name: &'static str,
    pub row_count: u64,
    pub snapshot_count: u64,
}

/// Copy event, session, and asset Parquet views to an attached REST catalog.
/// Existing catalog tables are accepted only when their row count matches the
/// current Parquet source, making a completed CTAS safe to verify after a
/// retry without appending duplicate history.
pub fn migrate_existing_files(conn: &Connection, config: &Config) -> Result<Vec<MigratedTable>> {
    let catalog_uri = config
        .iceberg_catalog_uri
        .as_deref()
        .context("ICEBERG_CATALOG_URI is required for migration")?;
    let warehouse = config
        .iceberg_warehouse
        .as_deref()
        .context("ICEBERG_WAREHOUSE is required for migration")?;
    let attach_sql = format!(
        "ATTACH '{}' AS trace_iceberg (TYPE ICEBERG, ENDPOINT '{}', ACCESS_DELEGATION_MODE 'none');\
         CREATE SCHEMA IF NOT EXISTS trace_iceberg.trace;",
        sql_literal(warehouse),
        sql_literal(catalog_uri)
    );
    conn.execute_batch(&attach_sql)
        .context("attach the Iceberg REST catalog")?;

    let plans = [
        (
            "ad_events",
            vec![
                "parquet_events",
                "parquet_events_compacted",
                "parquet_ad_events",
            ],
        ),
        ("sessions", vec!["parquet_sessions"]),
        ("assets", vec!["parquet_assets"]),
    ];
    ensure_event_views_disjoint(conn)?;

    let mut sources = Vec::new();
    for (table, views) in plans {
        let mut present = Vec::new();
        for view in views {
            if view_exists(conn, view)? {
                present.push(view);
            }
        }
        if !present.is_empty() {
            let source = present
                .into_iter()
                .map(|view| format!("SELECT * FROM {view}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL BY NAME ");
            sources.push((table, source, false));
        }
    }
    if sources.is_empty() {
        bail!("no existing Parquet views were found to migrate");
    }

    for (table, source, already_migrated) in &mut sources {
        let exists: i64 = conn.query_row(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_catalog = 'trace_iceberg' AND table_schema = 'trace' AND table_name = ?",
            params![*table],
            |row| row.get(0),
        )?;
        if exists > 0 {
            let source_count = count_query(
                conn,
                &format!("SELECT count(*) FROM ({source}) AS migration_source"),
            )?;
            let table_count = count_query(
                conn,
                &format!("SELECT count(*) FROM trace_iceberg.trace.{table}"),
            )?;
            anyhow::ensure!(
                source_count == table_count,
                "trace_iceberg.trace.{table} already exists with {table_count} rows, but source Parquet has {source_count}; refusing to append or replace"
            );
            *already_migrated = true;
        }
    }

    let mut migrated = Vec::new();
    for (table, source, already_migrated) in sources {
        if !already_migrated {
            conn.execute_batch(&format!(
                "CREATE TABLE trace_iceberg.trace.{table} AS {source};"
            ))
            .with_context(|| format!("migrate existing Parquet into trace.{table}"))?;
        }

        // Verify the new catalog table directly, through iceberg_scan, and by
        // reading one historical snapshot (the initial CTAS commit).
        let row_count = count_query(
            conn,
            &format!("SELECT count(*) FROM trace_iceberg.trace.{table}"),
        )?;
        let scan_count = count_query(
            conn,
            &format!(
                "SELECT count(*) FROM iceberg_scan('{}', catalog_uri => '{}')",
                sql_literal(&format!("{warehouse}/trace/{table}")),
                sql_literal(catalog_uri)
            ),
        )?;
        anyhow::ensure!(
            row_count == scan_count,
            "iceberg_scan row count mismatch for {table}"
        );

        let snapshots = format!("iceberg_snapshots(trace_iceberg.trace.{table})");
        let snapshot_count = count_query(conn, &format!("SELECT count(*) FROM {snapshots}"))?;
        anyhow::ensure!(snapshot_count > 0, "catalog table {table} has no snapshots");
        let first_snapshot: u64 = conn.query_row(
            &format!("SELECT snapshot_id FROM {snapshots} ORDER BY sequence_number LIMIT 1"),
            [],
            |row| row.get(0),
        )?;
        let historical_count = count_query(
            conn,
            &format!(
                "SELECT count(*) FROM iceberg_scan('{}', catalog_uri => '{}', snapshot_from_id => {first_snapshot})",
                sql_literal(&format!("{warehouse}/trace/{table}")),
                sql_literal(catalog_uri)
            ),
        )?;
        anyhow::ensure!(
            historical_count == row_count,
            "initial snapshot row count mismatch for {table}"
        );
        migrated.push(MigratedTable {
            name: table,
            row_count,
            snapshot_count,
        });
    }
    Ok(migrated)
}

fn view_exists(conn: &Connection, view: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM information_schema.views WHERE table_name = ?",
        params![view],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn ensure_event_views_disjoint(conn: &Connection) -> Result<()> {
    let views = [
        "parquet_events",
        "parquet_events_compacted",
        "parquet_ad_events",
    ];
    let present: Vec<&str> = views
        .into_iter()
        .filter_map(|view| match view_exists(conn, view) {
            Ok(true) => Some(Ok(view)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<_>>()?;
    for (index, left) in present.iter().enumerate() {
        for right in present.iter().skip(index + 1) {
            let overlapping_days: i64 = conn.query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT DISTINCT CAST(ts AS DATE) FROM {left} \
                     INTERSECT SELECT DISTINCT CAST(ts AS DATE) FROM {right})"
                ),
                [],
                |row| row.get(0),
            )?;
            anyhow::ensure!(
                overlapping_days == 0,
                "event views {left} and {right} overlap on {overlapping_days} day(s); reconcile them before migration"
            );
        }
    }
    Ok(())
}

fn count_query(conn: &Connection, sql: &str) -> Result<u64> {
    let count: i64 = conn
        .query_row(sql, [], |row| row.get(0))
        .with_context(|| format!("run verification query: {sql}"))?;
    u64::try_from(count).context("query returned a negative row count")
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_sql_string_literals_for_catalog_options() {
        assert_eq!(
            sql_literal("http://catalog/o'brien"),
            "http://catalog/o''brien"
        );
    }

    #[test]
    fn allows_migration_when_raw_and_compacted_days_do_not_overlap() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE VIEW parquet_events AS SELECT DATE '2026-09-29' AS dt, TIMESTAMP '2026-09-29 10:00:00' AS ts;\
             CREATE VIEW parquet_events_compacted AS SELECT DATE '2026-09-28' AS dt, TIMESTAMP '2026-09-28 10:00:00' AS ts;",
        )
        .unwrap();
        ensure_event_views_disjoint(&conn).unwrap();
    }

    #[test]
    fn rejects_migration_when_raw_and_compacted_days_overlap() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE VIEW parquet_events AS SELECT DATE '2026-09-29' AS dt, TIMESTAMP '2026-09-29 10:00:00' AS ts;\
             CREATE VIEW parquet_events_compacted AS SELECT DATE '2026-09-29' AS dt, TIMESTAMP '2026-09-29 12:00:00' AS ts;",
        )
        .unwrap();
        assert!(ensure_event_views_disjoint(&conn).is_err());
    }
}
