use anyhow::{Context, Result};
use duckdb::{Connection, params};
use crate::config::Config;

pub struct DuckDBClient {
    conn: Connection,
}

impl DuckDBClient {
    pub fn new(config: &Config) -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        // Install and load required extensions
        let mut extensions = vec![
            "INSTALL httpfs;",
            "LOAD httpfs;"
        ];

        // Load Iceberg extension if catalog is configured
        if config.iceberg_catalog_uri.is_some() {
            extensions.push("INSTALL iceberg;");
            extensions.push("LOAD iceberg;");
        }

        conn.execute_batch(&extensions.join("\n"))
            .context("Failed to load DuckDB extensions")?;

        // Configure S3 credentials if provided
        if let (Some(access_key), Some(secret_key)) = (&config.s3_access_key_id, &config.s3_secret_access_key) {
            let endpoint = config.s3_endpoint.as_deref().unwrap_or("s3.amazonaws.com");
            conn.execute(
                "SET s3_endpoint=?;",
                params![endpoint]
            )?;
            conn.execute(
                "SET s3_access_key_id=?;",
                params![access_key]
            )?;
            conn.execute(
                "SET s3_secret_access_key=?;",
                params![secret_key]
            )?;
        }

        conn.execute(
            "SET s3_region=?;",
            params![&config.s3_region]
        )?;

        conn.execute(
            "SET s3_use_ssl=true;",
            params![]
        )?;

        // Set memory limits for large queries
        conn.execute(
            "SET memory_limit='2GB';",
            params![]
        )?;

        conn.execute(
            "SET threads=4;",
            params![]
        )?;

        Ok(Self { conn })
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.prepare(sql)?;
        let columns: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
        let rows = stmt.query_map([], |row| {
            let mut values = Vec::new();
            for i in 0..row.as_ref().column_count() {
                let value: Option<String> = row.get(i)?;
                values.push(value.unwrap_or_else(|| "NULL".to_string()));
            }
            Ok(values)
        })?.collect::<Result<Vec<_>, _>>()?;

        Ok(QueryResult { columns, rows })
    }

    pub fn setup_views(&self, s3_path: &str) -> Result<()> {
        let view_sql = format!(
            "CREATE OR REPLACE VIEW events AS
             SELECT * FROM read_parquet('{}/events/**/*.parquet');",
            s3_path
        );
        self.conn.execute(&view_sql, params![])?;

        let compacted_sql = format!(
            "CREATE OR REPLACE VIEW events_compacted AS
             SELECT * FROM read_parquet('{}/events-compacted/**/*.parquet');",
            s3_path
        );
        self.conn.execute(&compacted_sql, params![])?;

        Ok(())
    }

    /// Setup views for Iceberg tables
    /// Requires Iceberg extension and catalog URI to be configured
    pub fn setup_iceberg_views(&self, config: &Config) -> Result<()> {
        let catalog_uri = config.iceberg_catalog_uri.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Iceberg catalog URI not configured"))?;
        let warehouse = config.iceberg_warehouse.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Iceberg warehouse not configured"))?;

        // Create view for ad_events table
        let ad_events_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_ad_events AS
             SELECT * FROM iceberg_scan('{}',
                 catalog_uri => '{}'
             );",
            format!("{}/ad_events", warehouse),
            catalog_uri
        );
        self.conn.execute(&ad_events_sql, params![])?;

        // Create view for campaigns dimension table
        let campaigns_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_campaigns AS
             SELECT * FROM iceberg_scan('{}',
                 catalog_uri => '{}'
             );",
            format!("{}/campaigns", warehouse),
            catalog_uri
        );
        self.conn.execute(&campaigns_sql, params![])?;

        // Create view for creatives dimension table
        let creatives_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_creatives AS
             SELECT * FROM iceberg_scan('{}',
                 catalog_uri => '{}'
             );",
            format!("{}/creatives", warehouse),
            catalog_uri
        );
        self.conn.execute(&creatives_sql, params![])?;

        Ok(())
    }

    /// Get the SQL fragment for querying events (either Iceberg or Parquet)
    /// Returns Iceberg table SQL if configured, otherwise falls back to Parquet
    pub fn events_table_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            "iceberg_ad_events".to_string()
        } else {
            format!("read_parquet('{}/events/**/*.parquet')", config.s3_events_path())
        }
    }

    /// Get the SQL fragment for querying compacted events
    pub fn events_compacted_sql(&self, config: &Config) -> String {
        if config.is_iceberg_enabled() {
            // For Iceberg, we can filter on the main table
            "iceberg_ad_events".to_string()
        } else {
            format!("read_parquet('{}/**/*.parquet')", config.s3_compacted_path())
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
                json.push_str(&format!("\"{}\":{}", self.columns[j], escape_json_value(value)));
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
            csv.push_str(&row.iter().map(|v| escape_csv_value(v)).collect::<Vec<_>>().join(","));
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
