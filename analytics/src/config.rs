use anyhow::{Context, Result};
use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_prefix: String,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_endpoint: Option<String>,
    pub data_path: String,
    pub reports_output_path: String,
    /// Iceberg REST catalog URI (e.g., http://iceberg-catalog:8181)
    pub iceberg_catalog_uri: Option<String>,
    /// Iceberg warehouse path (e.g., s3://my-trace-bucket/iceberg)
    pub iceberg_warehouse: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            s3_bucket: env::var("TRACE_S3_BUCKET")
                .or_else(|_| env::var("S3_BUCKET"))
                .unwrap_or_else(|_| "my-trace-bucket".to_string()),
            s3_region: env::var("TRACE_S3_REGION")
                .or_else(|_| env::var("S3_REGION"))
                .unwrap_or_else(|_| "us-east-1".to_string()),
            s3_prefix: env::var("TRACE_S3_PREFIX")
                .or_else(|_| env::var("S3_PREFIX"))
                .unwrap_or_else(|_| "trace-events".to_string()),
            s3_access_key_id: env::var("AWS_ACCESS_KEY_ID").ok(),
            s3_secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
            s3_endpoint: env::var("S3_ENDPOINT").ok(),
            data_path: env::var("TRACE_DATA_PATH")
                .unwrap_or_else(|_| "/data/analytics".to_string()),
            reports_output_path: env::var("TRACE_REPORTS_OUTPUT_PATH")
                .unwrap_or_else(|_| "/data/reports".to_string()),
            iceberg_catalog_uri: env::var("ICEBERG_CATALOG_URI").ok(),
            iceberg_warehouse: env::var("ICEBERG_WAREHOUSE").ok(),
        })
    }

    pub fn s3_events_path(&self) -> String {
        format!("s3://{}/{}/events", self.s3_bucket, self.s3_prefix)
    }

    pub fn s3_compacted_path(&self) -> String {
        format!("s3://{}/{}/events-compacted", self.s3_bucket, self.s3_prefix)
    }

    /// Check if Iceberg catalog is configured
    pub fn is_iceberg_enabled(&self) -> bool {
        self.iceberg_catalog_uri.is_some() && self.iceberg_warehouse.is_some()
    }

    /// Get the Iceberg table path for ad_events
    pub fn iceberg_ad_events_path(&self) -> Option<String> {
        self.iceberg_warehouse.as_ref()
            .map(|w| format!("{}/ad_events", w))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_values() {
        // Create a config with minimal values
        let config = Config {
            s3_bucket: "test-bucket".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_prefix: "test-events".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_endpoint: None,
            data_path: "/data/analytics".to_string(),
            reports_output_path: "/data/reports".to_string(),
            iceberg_catalog_uri: None,
            iceberg_warehouse: None,
        };

        assert_eq!(config.s3_bucket, "test-bucket");
        assert_eq!(config.s3_region, "us-east-1");
        assert_eq!(config.s3_prefix, "test-events");
        assert!(!config.is_iceberg_enabled());
    }

    #[test]
    fn test_config_is_iceberg_enabled() {
        let mut config = Config {
            s3_bucket: "test-bucket".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_prefix: "test-events".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_endpoint: None,
            data_path: "/data/analytics".to_string(),
            reports_output_path: "/data/reports".to_string(),
            iceberg_catalog_uri: None,
            iceberg_warehouse: None,
        };

        // Not enabled when only catalog URI is set
        config.iceberg_catalog_uri = Some("http://catalog:8181".to_string());
        assert!(!config.is_iceberg_enabled());

        // Not enabled when only warehouse is set
        config.iceberg_catalog_uri = None;
        config.iceberg_warehouse = Some("s3://bucket/iceberg".to_string());
        assert!(!config.is_iceberg_enabled());

        // Enabled when both are set
        config.iceberg_catalog_uri = Some("http://catalog:8181".to_string());
        assert!(config.is_iceberg_enabled());
    }

    #[test]
    fn test_config_s3_paths() {
        let config = Config {
            s3_bucket: "my-bucket".to_string(),
            s3_region: "us-west-2".to_string(),
            s3_prefix: "trace-events".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_endpoint: None,
            data_path: "/data/analytics".to_string(),
            reports_output_path: "/data/reports".to_string(),
            iceberg_catalog_uri: None,
            iceberg_warehouse: None,
        };

        assert_eq!(config.s3_events_path(), "s3://my-bucket/trace-events/events");
        assert_eq!(config.s3_compacted_path(), "s3://my-bucket/trace-events/events-compacted");
    }

    #[test]
    fn test_config_iceberg_ad_events_path() {
        let mut config = Config {
            s3_bucket: "my-bucket".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_prefix: "trace-events".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_endpoint: None,
            data_path: "/data/analytics".to_string(),
            reports_output_path: "/data/reports".to_string(),
            iceberg_catalog_uri: None,
            iceberg_warehouse: None,
        };

        // None when warehouse is not set
        assert!(config.iceberg_ad_events_path().is_none());

        // Some when warehouse is set
        config.iceberg_warehouse = Some("s3://my-bucket/iceberg".to_string());
        assert_eq!(config.iceberg_ad_events_path(), Some("s3://my-bucket/iceberg/ad_events".to_string()));
    }
}
