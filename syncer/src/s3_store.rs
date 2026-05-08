//! S3 storage for creative metadata

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client};
use aws_smithy_types::byte_stream::ByteStream;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_writer::ArrowWriter;

use crate::creative::{CreativeMetadata, PerformanceMetrics};
use crate::hierarchy::AccountHierarchy;

/// Trait for creative metadata storage
#[async_trait]
pub trait CreativeStore: Send + Sync {
    /// Store creative metadata
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()>;

    /// Load creative metadata
    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>>;
}

/// Trait for performance metrics storage
#[async_trait]
pub trait MetricsStore: Send + Sync {
    /// Store performance metrics
    async fn store_metrics(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<()>;

    /// Load performance metrics for a date range
    async fn load_metrics(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<PerformanceMetrics>>;
}

/// Trait for hierarchy storage
#[async_trait]
pub trait HierarchyStore: Send + Sync {
    /// Store account hierarchy
    async fn store_hierarchy(&self, hierarchy: &AccountHierarchy) -> anyhow::Result<()>;

    /// Load account hierarchy for a specific network and account
    async fn load_hierarchy(&self, network: &str, account_id: &str) -> anyhow::Result<Option<AccountHierarchy>>;

    /// List all available hierarchies
    async fn list_hierarchies(&self) -> anyhow::Result<Vec<(String, String)>>;
}

/// S3-backed creative store
pub struct S3CreativeStore {
    client: Client,
    bucket: String,
    key_prefix: String,
}

impl S3CreativeStore {
    /// Create a new S3 creative store
    pub async fn new(bucket: String, region: String, key_prefix: String) -> anyhow::Result<Self> {
        let region = Region::new(region);
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let client = Client::new(&aws_config);

        Ok(Self {
            client,
            bucket,
            key_prefix,
        })
    }

    /// Get the S3 key for the creative registry
    fn registry_key(&self) -> String {
        format!("{}/creative-registry.parquet", self.key_prefix)
    }

    /// Get the S3 key for performance metrics for a specific date
    fn metrics_key(&self, date: chrono::NaiveDate) -> String {
        format!("{}/metrics/metrics-{}.parquet", self.key_prefix, date)
    }

    /// Get the S3 key for hierarchy data
    fn hierarchy_key(&self, network: &str, account_id: &str) -> String {
        format!("{}/hierarchy/{}-{}.json", self.key_prefix, network, account_id)
    }

    /// Convert performance metrics to Parquet format
    fn metrics_to_parquet(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<Vec<u8>> {
        use arrow::array::{
            Int64Array, LongArray, NullArray, StringArray, TimestampMillisecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let networks: Vec<String> = metrics.iter().map(|m| m.network.clone()).collect();
        let campaign_ids: Vec<String> = metrics.iter().map(|m| m.campaign_id.clone()).collect();
        let campaign_names: Vec<Option<String>> =
            metrics.iter().map(|m| m.campaign_name.clone()).collect();
        let creative_ids: Vec<Option<String>> =
            metrics.iter().map(|m| m.creative_id.clone()).collect();
        let dates: Vec<i32> = metrics
            .iter()
            .map(|m| m.date.format("%Y%m%d").to_string().parse::<i32>().unwrap())
            .collect();
        let impressions: Vec<i64> = metrics.iter().map(|m| m.impressions).collect();
        let clicks: Vec<i64> = metrics.iter().map(|m| m.clicks).collect();
        let spend_micros: Vec<i64> = metrics.iter().map(|m| m.spend_micros).collect();
        let conversions: Vec<Option<i64>> = metrics.iter().map(|m| m.conversions).collect();
        let ctr_bps: Vec<Option<i32>> = metrics.iter().map(|m| m.ctr_bps).collect();
        let cpc_micros: Vec<Option<i64>> = metrics.iter().map(|m| m.cpc_micros).collect();
        let cpm_micros: Vec<Option<i64>> = metrics.iter().map(|m| m.cpm_micros).collect();
        let synced_at: Vec<i64> = metrics
            .iter()
            .map(|m| m.synced_at.timestamp_millis())
            .collect();

        let schema = Schema::new(vec![
            Field::new("network", DataType::Utf8, false),
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("campaign_name", DataType::Utf8, true),
            Field::new("creative_id", DataType::Utf8, true),
            Field::new("date", DataType::Int32, false),
            Field::new("impressions", DataType::Int64, false),
            Field::new("clicks", DataType::Int64, false),
            Field::new("spend_micros", DataType::Int64, false),
            Field::new("conversions", DataType::Int64, true),
            Field::new("ctr_bps", DataType::Int32, true),
            Field::new("cpc_micros", DataType::Int64, true),
            Field::new("cpm_micros", DataType::Int64, true),
            Field::new(
                "synced_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(networks)),
                Arc::new(StringArray::from(campaign_ids)),
                Arc::new(StringArray::from(campaign_names)),
                Arc::new(StringArray::from(creative_ids)),
                Arc::new(LongArray::from(dates)),
                Arc::new(Int64Array::from(impressions)),
                Arc::new(Int64Array::from(clicks)),
                Arc::new(Int64Array::from(spend_micros)),
                Arc::new(Int64Array::from(conversions)),
                Arc::new(Int64Array::from(ctr_bps.map(|v| v as i64))),
                Arc::new(Int64Array::from(cpc_micros)),
                Arc::new(Int64Array::from(cpm_micros)),
                Arc::new(TimestampMillisecondArray::from(synced_at)),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(buffer)
    }

    /// Convert Parquet data to performance metrics
    fn parquet_to_metrics(&self, data: &[u8]) -> anyhow::Result<Vec<PerformanceMetrics>> {
        use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::from(data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let mut metrics = Vec::new();

        for batch in reader {
            let batch = batch?;

            let networks = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast network column"))?;

            let campaign_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_id column"))?;

            let campaign_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_name column"))?;

            let creative_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast creative_id column"))?;

            let dates = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast date column"))?;

            let impressions = batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast impressions column"))?;

            let clicks = batch
                .column(6)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast clicks column"))?;

            let spend_micros = batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast spend_micros column"))?;

            let conversions = batch
                .column(8)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast conversions column"))?;

            let ctr_bps = batch
                .column(9)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast ctr_bps column"))?;

            let cpc_micros = batch
                .column(10)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast cpc_micros column"))?;

            let cpm_micros = batch
                .column(11)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast cpm_micros column"))?;

            let synced_at = batch
                .column(12)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast synced_at column"))?;

            for i in 0..batch.num_rows() {
                let date_str = format!("{:08}", dates.value(i));
                let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")?;

                metrics.push(PerformanceMetrics {
                    network: networks.value(i).to_string(),
                    campaign_id: campaign_ids.value(i).to_string(),
                    campaign_name: campaign_names
                        .is_valid(i)
                        .then(|| campaign_names.value(i).to_string()),
                    creative_id: creative_ids
                        .is_valid(i)
                        .then(|| creative_ids.value(i).to_string()),
                    date,
                    impressions: impressions.value(i),
                    clicks: clicks.value(i),
                    spend_micros: spend_micros.value(i),
                    conversions: conversions.is_valid(i).then(|| conversions.value(i)),
                    ctr_bps: ctr_bps.is_valid(i).then(|| ctr_bps.value(i) as i32),
                    cpc_micros: cpc_micros.is_valid(i).then(|| cpc_micros.value(i)),
                    cpm_micros: cpm_micros.is_valid(i).then(|| cpm_micros.value(i)),
                    synced_at: DateTime::from_timestamp_millis(synced_at.value(i)).unwrap(),
                });
            }
        }

        Ok(metrics)
    }

    /// Convert creatives to Parquet format
    fn creatives_to_parquet(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<Vec<u8>> {
        use arrow::array::{StringArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let networks: Vec<String> = creatives.iter().map(|c| c.network.clone()).collect();
        let campaign_ids: Vec<Option<String>> =
            creatives.iter().map(|c| c.campaign_id.clone()).collect();
        let campaign_names: Vec<Option<String>> =
            creatives.iter().map(|c| c.campaign_name.clone()).collect();
        let creative_ids: Vec<Option<String>> =
            creatives.iter().map(|c| c.creative_id.clone()).collect();
        let headlines: Vec<Option<String>> = creatives.iter().map(|c| c.headline.clone()).collect();
        let image_urls: Vec<Option<String>> =
            creatives.iter().map(|c| c.image_url.clone()).collect();
        let landing_page_urls: Vec<Option<String>> = creatives
            .iter()
            .map(|c| c.landing_page_url.clone())
            .collect();
        let item_ids: Vec<Option<String>> = creatives.iter().map(|c| c.item_id.clone()).collect();
        let synced_at: Vec<i64> = creatives
            .iter()
            .map(|c| c.synced_at.timestamp_millis())
            .collect();

        let schema = Schema::new(vec![
            Field::new("network", DataType::Utf8, false),
            Field::new("campaign_id", DataType::Utf8, true),
            Field::new("campaign_name", DataType::Utf8, true),
            Field::new("creative_id", DataType::Utf8, true),
            Field::new("headline", DataType::Utf8, true),
            Field::new("image_url", DataType::Utf8, true),
            Field::new("landing_page_url", DataType::Utf8, true),
            Field::new("item_id", DataType::Utf8, true),
            Field::new(
                "synced_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(networks)),
                Arc::new(StringArray::from(campaign_ids)),
                Arc::new(StringArray::from(campaign_names)),
                Arc::new(StringArray::from(creative_ids)),
                Arc::new(StringArray::from(headlines)),
                Arc::new(StringArray::from(image_urls)),
                Arc::new(StringArray::from(landing_page_urls)),
                Arc::new(StringArray::from(item_ids)),
                Arc::new(TimestampMillisecondArray::from(synced_at)),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(buffer)
    }

    /// Convert Parquet data to creatives
    fn parquet_to_creatives(&self, data: &[u8]) -> anyhow::Result<Vec<CreativeMetadata>> {
        use arrow::array::{Array, StringArray, TimestampMillisecondArray};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::from(data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let mut creatives = Vec::new();

        for batch in reader {
            let batch = batch?;

            let networks = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast network column"))?;

            let campaign_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_id column"))?;

            let campaign_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_name column"))?;

            let creative_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast creative_id column"))?;

            let headlines = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast headline column"))?;

            let image_urls = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast image_url column"))?;

            let landing_page_urls = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast landing_page_url column"))?;

            let item_ids = batch
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast item_id column"))?;

            let synced_at = batch
                .column(8)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast synced_at column"))?;

            for i in 0..batch.num_rows() {
                creatives.push(CreativeMetadata {
                    network: networks.value(i).to_string(),
                    campaign_id: campaign_ids
                        .is_valid(i)
                        .then(|| campaign_ids.value(i).to_string()),
                    campaign_name: campaign_names
                        .is_valid(i)
                        .then(|| campaign_names.value(i).to_string()),
                    creative_id: creative_ids
                        .is_valid(i)
                        .then(|| creative_ids.value(i).to_string()),
                    headline: headlines
                        .is_valid(i)
                        .then(|| headlines.value(i).to_string()),
                    image_url: image_urls
                        .is_valid(i)
                        .then(|| image_urls.value(i).to_string()),
                    landing_page_url: landing_page_urls
                        .is_valid(i)
                        .then(|| landing_page_urls.value(i).to_string()),
                    item_id: item_ids.is_valid(i).then(|| item_ids.value(i).to_string()),
                    synced_at: DateTime::from_timestamp_millis(synced_at.value(i)).unwrap(),
                });
            }
        }

        Ok(creatives)
    }
}

#[async_trait]
impl CreativeStore for S3CreativeStore {
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()> {
        let parquet_data = self.creatives_to_parquet(creatives)?;

        let key = self.registry_key();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(parquet_data))
            .send()
            .await?;

        tracing::info!("Stored creative registry to s3://{}/{}", self.bucket, key);
        Ok(())
    }

    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>> {
        let key = self.registry_key();

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;

        let data = response.body.collect().await?.into_bytes().to_vec();
        let creatives = self.parquet_to_creatives(&data)?;

        tracing::info!("Loaded {} creatives from s3://{}", creatives.len(), key);
        Ok(creatives)
    }
}

#[async_trait]
impl MetricsStore for S3CreativeStore {
    async fn store_metrics(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<()> {
        // Group metrics by date
        let mut metrics_by_date: std::collections::HashMap<
            chrono::NaiveDate,
            Vec<PerformanceMetrics>,
        > = std::collections::HashMap::new();

        for metric in metrics {
            metrics_by_date.entry(metric.date).or_default().push(metric);
        }

        // Store each date's metrics separately
        for (date, date_metrics) in metrics_by_date {
            let parquet_data = self.metrics_to_parquet(date_metrics)?;
            let key = self.metrics_key(date);

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(parquet_data))
                .send()
                .await?;

            tracing::info!("Stored metrics for {} to s3://{}", date, key);
        }

        Ok(())
    }

    async fn load_metrics(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<PerformanceMetrics>> {
        let mut all_metrics = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let key = self.metrics_key(current_date);

            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(response) => {
                    let data = response.body.collect().await?.into_bytes().to_vec();
                    let metrics = self.parquet_to_metrics(&data)?;
                    all_metrics.extend(metrics);
                }
                Err(e) => {
                    // Log warning but continue - metrics might not exist for all dates
                    tracing::warn!("Failed to load metrics for {}: {}", key, e);
                }
            }

            current_date = current_date.succ_opt().unwrap_or(current_date);
        }

        tracing::info!(
            "Loaded {} metrics from {} to {}",
            all_metrics.len(),
            start_date,
            end_date
        );
        Ok(all_metrics)
    }
}

#[async_trait]
impl HierarchyStore for S3CreativeStore {
    async fn store_hierarchy(&self, hierarchy: &AccountHierarchy) -> anyhow::Result<()> {
        let json_data = serde_json::to_vec_pretty(hierarchy)?;
        let key = self.hierarchy_key(&hierarchy.network, &hierarchy.account_id);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(json_data))
            .send()
            .await?;

        tracing::info!("Stored hierarchy for {}:{} to s3://{}", hierarchy.network, hierarchy.account_id, key);
        Ok(())
    }

    async fn load_hierarchy(&self, network: &str, account_id: &str) -> anyhow::Result<Option<AccountHierarchy>> {
        let key = self.hierarchy_key(network, account_id);

        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(response) => {
                let data = response.body.collect().await?.into_bytes().to_vec();
                let hierarchy = serde_json::from_slice(&data)?;
                tracing::info!("Loaded hierarchy for {}:{} from s3://{}", network, account_id, key);
                Ok(Some(hierarchy))
            }
            Err(e) => {
                tracing::debug!("No hierarchy found for {}:{}: {}", network, account_id, e);
                Ok(None)
            }
        }
    }

    async fn list_hierarchies(&self) -> anyhow::Result<Vec<(String, String)>> {
        let prefix = format!("{}/hierarchy/", self.key_prefix);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await?;

        let mut hierarchies = Vec::new();

        if let Some(objects) = response.contents {
            for obj in objects {
                if let Some(key) = obj.key() {
                    // Extract network and account_id from key pattern: "prefix/hierarchy/network-account_id.json"
                    if let Some(filename) = key.strip_prefix(&prefix) {
                        if let Some(stem) = filename.strip_suffix(".json") {
                            if let Some((network, account_id)) = stem.split_once('-') {
                                hierarchies.push((network.to_string(), account_id.to_string()));
                            }
                        }
                    }
                }
            }
        }

        tracing::info!("Found {} hierarchies in s3://{}", hierarchies.len(), self.bucket);
        Ok(hierarchies)
    }
}

/// Mock store for testing
pub struct MockCreativeStore {
    data: std::sync::Arc<tokio::sync::RwLock<Vec<CreativeMetadata>>>,
}

impl MockCreativeStore {
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl CreativeStore for MockCreativeStore {
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()> {
        let mut data = self.data.write().await;
        *data = creatives;
        Ok(())
    }

    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>> {
        let data = self.data.read().await;
        Ok(data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to create a test store without requiring AWS credentials
    /// The S3 client is created with a dummy config for testing parquet conversion only
    fn create_test_store() -> S3CreativeStore {
        // Create a minimal config that won't actually connect to S3
        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        S3CreativeStore {
            client: Client::from_conf(config),
            bucket: "test-bucket".to_string(),
            key_prefix: "test-prefix".to_string(),
        }
    }

    #[test]
    fn test_creatives_to_parquet() {
        let store = create_test_store();

        let creatives = vec![CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: Some("Test Campaign".to_string()),
            creative_id: Some("cr456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: Some("https://example.com/page".to_string()),
            item_id: Some("item789".to_string()),
            synced_at: Utc::now(),
        }];

        let result = store.creatives_to_parquet(creatives);
        assert!(result.is_ok());
        let parquet_data = result.unwrap();
        assert!(!parquet_data.is_empty());
    }

    #[test]
    fn test_parquet_roundtrip() {
        let store = create_test_store();

        let original = vec![CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: Some("Test Campaign".to_string()),
            creative_id: Some("cr456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: Some("https://example.com/page".to_string()),
            item_id: Some("item789".to_string()),
            synced_at: Utc::now(),
        }];

        let parquet_data = store.creatives_to_parquet(original.clone()).unwrap();
        let restored = store.parquet_to_creatives(&parquet_data).unwrap();

        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].network, original[0].network);
        assert_eq!(restored[0].campaign_id, original[0].campaign_id);
        assert_eq!(restored[0].headline, original[0].headline);
    }
}
