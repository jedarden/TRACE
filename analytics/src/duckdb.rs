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
        conn.execute_batch(
            "INSTALL httpfs;
             LOAD httpfs;"
        ).context("Failed to load DuckDB extensions")?;

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
