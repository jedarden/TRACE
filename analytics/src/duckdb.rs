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

        let mut client = Self { conn };

        // Setup Iceberg views if configured
        if config.is_iceberg_enabled() {
            client.setup_iceberg_views(config)?;
        } else {
            // Setup Parquet views for backward compatibility
            let s3_path = format!("s3://{}/{}", config.s3_bucket, config.s3_prefix);
            client.setup_parquet_views(&s3_path)?;
        }

        Ok(client)
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
        self.conn.execute(&ad_events_sql, params![])
            .context("Failed to create view for Iceberg ad_events table")?;

        // Create view for campaigns dimension table
        let campaigns_path = format!("{}/campaigns", warehouse);
        let campaigns_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_campaigns AS \
             SELECT * FROM iceberg_scan('{}', {});",
            campaigns_path, catalog_option
        );
        self.conn.execute(&campaigns_sql, params![])
            .context("Failed to create view for Iceberg campaigns table")?;

        // Create view for creatives dimension table
        let creatives_path = format!("{}/creatives", warehouse);
        let creatives_sql = format!(
            "CREATE OR REPLACE VIEW iceberg_creatives AS \
             SELECT * FROM iceberg_scan('{}', {});",
            creatives_path, catalog_option
        );
        self.conn.execute(&creatives_sql, params![])
            .context("Failed to create view for Iceberg creatives table")?;

        Ok(())
    }

    /// Setup views for Parquet files (legacy mode)
    /// Falls back to Parquet when Iceberg is not configured
    pub fn setup_parquet_views(&self, s3_path: &str) -> Result<()> {
        let view_sql = format!(
            "CREATE OR REPLACE VIEW parquet_events AS \
             SELECT * FROM read_parquet('{}/events/**/*.parquet');",
            s3_path
        );
        self.conn.execute(&view_sql, params![])
            .context("Failed to create view for Parquet events")?;

        let compacted_sql = format!(
            "CREATE OR REPLACE VIEW parquet_events_compacted AS \
             SELECT * FROM read_parquet('{}/events-compacted/**/*.parquet');",
            s3_path
        );
        self.conn.execute(&compacted_sql, params![])
            .context("Failed to create view for compacted Parquet events")?;

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
        assert_eq!(result.to_json(), "[{}]");
    }

    #[test]
    fn test_query_result_to_json_single_row() {
        let result = QueryResult {
            columns: vec!["col1".to_string(), "col2".to_string()],
            rows: vec![
                vec!["value1".to_string(), "value2".to_string()],
            ],
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
            rows: vec![
                vec!["value,with,commas".to_string()],
            ],
        };
        let csv = result.to_csv();
        assert_eq!(csv, "col1\n\"value,with,commas\"\n");
    }
}
