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
        })
    }

    pub fn s3_events_path(&self) -> String {
        format!("s3://{}/{}/events", self.s3_bucket, self.s3_prefix)
    }

    pub fn s3_compacted_path(&self) -> String {
        format!("s3://{}/{}/events-compacted", self.s3_bucket, self.s3_prefix)
    }
}
