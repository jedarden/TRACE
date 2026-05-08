use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_s3::bucket::Bucket;
use rust_s3::credentials::Credentials;
use rust_s3::s3::S3Action;
use tracing::info;

use crate::config::Config;

pub struct S3Client {
    bucket: Bucket,
}

impl S3Client {
    pub fn new(config: &Config) -> Result<Self> {
        let credentials = match (&config.s3_access_key_id, &config.s3_secret_access_key) {
            (Some(key_id), Some(secret)) => Credentials::new(
                Some(key_id),
                Some(secret),
                None,
                None,
                None,
            ),
            _ => Credentials::new(None, None, None, None, None),
        };

        let region = rust_s3::Region::Custom {
            region: config.s3_region.clone(),
            endpoint: config.s3_endpoint.clone().unwrap_or_else(|| format!(
                "s3.{}.amazonaws.com",
                config.s3_region
            )),
        };

        let bucket = Bucket::new(
            &config.s3_bucket,
            region,
            credentials,
        )?.with_path_style();

        Ok(Self { bucket })
    }

    pub async fn upload_report(
        &self,
        report_name: &str,
        data: &[u8],
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let path = format!(
            "{}/reports/{}/{}/{}.json",
            self.bucket.name(),
            report_name,
            timestamp.format("%Y-%m-%d"),
            timestamp.format("%Y%m%d-%H%M%S")
        );

        info!("Uploading report to: {}", path);

        self.bucket
            .put_object(&path, data)
            .await
            .context("Failed to upload report to S3")?;

        info!("Report uploaded successfully");
        Ok(())
    }

    pub async fn list_reports(&self, report_name: &str) -> Result<Vec<String>> {
        let prefix = format!("{}/reports/{}/", self.bucket.name(), report_name);

        let response = self.bucket
            .list_prefix::<&str>(Some(&prefix), None)
            .await
            .context("Failed to list reports from S3")?;

        let files: Vec<String> = response
            .contents
            .into_iter()
            .map(|obj => obj.key)
            .collect();

        Ok(files)
    }
}
