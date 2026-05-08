pub mod iceberg;

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client};
use aws_smithy_types::byte_stream::ByteStream;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use tracing_subscriber::prelude::*;
use futures::StreamExt;

use arrow::array::Array;
use arrow::datatypes::DataType;

/// Parse date from partition path (dt=YYYY-MM-DD)
#[allow(dead_code)]
fn parse_date_from_partition(path: &str) -> Option<NaiveDate> {
    let date_part = path.strip_prefix("dt=")?;
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

/// S3 configuration
#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
}

/// S3 operations trait for testability
#[async_trait]
pub trait S3Ops: Send + Sync {
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
    async fn get_object(&self, key: &str) -> Result<Vec<u8>>;
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()>;
    async fn delete_objects(&self, keys: Vec<String>) -> Result<()>;

    /// Allow downcasting for accessing S3Client-specific methods
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Real S3 implementation
pub struct S3Client {
    pub client: Client,
    pub config: S3Config,
}

impl S3Client {
    pub async fn new(config: S3Config) -> Result<Self> {
        let region = Region::new(config.region.clone());
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let client = Client::new(&aws_config);

        Ok(Self { client, config })
    }

    pub fn full_key(&self, key: &str) -> String {
        format!("{}/{}", self.config.key_prefix, key)
    }
}

#[async_trait]
impl S3Ops for S3Client {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);

        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.config.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.context("S3 list failed")?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        // Strip prefix to return relative keys
                        let relative = key
                            .strip_prefix(&self.config.key_prefix)
                            .unwrap_or(&key)
                            .trim_start_matches('/');
                        keys.push(relative.to_string());
                    }
                }
            }

            continuation_token = response.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(keys)
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .context("S3 get failed")?;

        let data = response
            .body
            .collect()
            .await
            .context("Failed to read body")?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let full_key = self.full_key(key);

        debug!(
            "Uploading {} bytes to s3://{}/{}",
            data.len(),
            self.config.bucket,
            full_key
        );

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(ByteStream::from(data))
            .send()
            .await
            .context("S3 put failed")?;

        info!("Uploaded to s3://{}/{}", self.config.bucket, full_key);
        Ok(())
    }

    async fn delete_objects(&self, keys: Vec<String>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // Delete in batches of 1000 (S3 limit)
        for chunk in keys.chunks(1000) {
            let mut delete_ids = Vec::new();
            for key in chunk {
                let id = aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(self.full_key(key))
                    .build()
                    .context("Invalid key")?;
                delete_ids.push(id);
            }

            self.client
                .delete_objects()
                .bucket(&self.config.bucket)
                .delete(aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(delete_ids))
                    .build()
                    .context("Invalid delete request")?)
                .send()
                .await
                .context("S3 delete failed")?;

            info!("Deleted {} objects", chunk.len());
        }

        Ok(())
    }
}

/// Compaction configuration
#[derive(Clone)]
struct CompactorConfig {
    lookback_days: i64,
    min_files_to_compact: usize,
    target_row_group_size: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            lookback_days: 7, // Compact last 7 days
            min_files_to_compact: 2,
            target_row_group_size: 1_000_000,
        }
    }
}

/// Merge multiple Parquet files into one
pub async fn merge_parquet_files(
    s3: Arc<dyn S3Ops>,
    keys: Vec<String>,
    target_row_group_size: usize,
) -> Result<Vec<u8>> {
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<Schema> = None;

    // Read all files
    for key in &keys {
        let data = s3.get_object(key).await?;
        let cursor = std::io::Cursor::new(data);

        let reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await?
            .build()?;

        // Use schema from first file
        if schema.is_none() {
            schema = Some(reader.schema().as_ref().clone());
        }

        // Collect all batches
        let mut batches = Vec::new();
        let mut stream = reader;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            batches.push(batch);
        }

        let batch_count = batches.len();
        all_batches.extend(batches);
        debug!("Read {} batches from {}", batch_count, key);
    }

    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Combine all batches
    let combined = combine_batches(all_batches)?;

    // Write merged Parquet
    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_size(target_row_group_size)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, combined.schema(), Some(props))?;
    writer.write(&combined)?;
    writer.close()?;

    Ok(buffer)
}

/// Combine multiple record batches into one
fn combine_batches(batches: Vec<arrow::record_batch::RecordBatch>) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{StringArray, TimestampMillisecondArray};

    if batches.is_empty() {
        anyhow::bail!("No batches to combine");
    }

    let schema = batches[0].schema();
    let num_columns = schema.fields().len();

    let mut columns: Vec<Box<dyn arrow::array::ArrayBuilder>> = Vec::new();

    for col_idx in 0..num_columns {
        let field = schema.field(col_idx);
        let data_type = field.data_type();

        match data_type {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
                let mut builder = arrow::array::TimestampMillisecondBuilder::new();
                for batch in &batches {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Column {} is not Timestamp", col_idx))?;
                    for val in col.iter() {
                        if let Some(v) = val {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                columns.push(Box::new(builder));
            }
            DataType::Utf8 => {
                let builder = arrow::array::StringBuilder::new();
                columns.push(Box::new(builder));
            }
            DataType::Null => {
                // For nullable columns, use String builder
                let builder = arrow::array::StringBuilder::new();
                columns.push(Box::new(builder));
            }
            _ => {
                anyhow::bail!("Unsupported data type: {:?}", data_type);
            }
        }
    }

    // Actually combine the data
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut ts_data = Vec::with_capacity(total_rows);
    let mut ip_data = Vec::with_capacity(total_rows);
    let mut ua_data = Vec::with_capacity(total_rows);
    let mut url_data = Vec::with_capacity(total_rows);
    let mut params_data = Vec::with_capacity(total_rows);
    let mut type_data = Vec::with_capacity(total_rows);

    for batch in &batches {
        let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>();
        let ip_col = batch.column(1).as_any().downcast_ref::<StringArray>();
        let ua_col = batch.column(2).as_any().downcast_ref::<StringArray>();
        let url_col = batch.column(3).as_any().downcast_ref::<StringArray>();
        let params_col = batch.column(4).as_any().downcast_ref::<StringArray>();
        let type_col = batch.column(5).as_any().downcast_ref::<StringArray>();

        if let (Some(ts), Some(ip), Some(ua), Some(url), Some(params), Some(ty)) =
            (ts_col, ip_col, ua_col, url_col, params_col, type_col)
        {
            for i in 0..batch.num_rows() {
                ts_data.push(ts.value(i));
                ip_data.push(ip.is_valid(i).then(|| ip.value(i).to_string()));
                ua_data.push(ua.is_valid(i).then(|| ua.value(i).to_string()));
                url_data.push(url.value(i).to_string());
                params_data.push(params.value(i).to_string());
                type_data.push(ty.value(i).to_string());
            }
        }
    }

    let combined_schema = schema.as_ref().clone();
    let combined_batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(combined_schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts_data)),
            Arc::new(StringArray::from(ip_data)),
            Arc::new(StringArray::from(ua_data)),
            Arc::new(StringArray::from(url_data)),
            Arc::new(StringArray::from(params_data)),
            Arc::new(StringArray::from(type_data)),
        ],
    )?;

    Ok(combined_batch)
}

/// Compact a single day's hourly files into daily partitions
async fn compact_day(
    s3: Arc<dyn S3Ops>,
    date: NaiveDate,
    config: &CompactorConfig,
) -> Result<()> {
    let date_str = date.format("%Y-%m-%d").to_string();
    info!("Compacting data for {}", date_str);

    // List all hourly files for this day
    let prefix = format!("events/dt={}/", date_str);
    let keys = s3.list_objects(&prefix).await?;

    // Filter for Parquet files in hour partitions
    let hourly_files: Vec<String> = keys
        .into_iter()
        .filter(|k| k.contains("/hour=") && k.ends_with(".parquet"))
        .collect();

    if hourly_files.len() < config.min_files_to_compact {
        info!(
            "Skipping {}: only {} hourly files (minimum: {})",
            date_str,
            hourly_files.len(),
            config.min_files_to_compact
        );
        return Ok(());
    }

    info!("Merging {} hourly files for {}", hourly_files.len(), date_str);

    // Merge all files
    let merged_data = merge_parquet_files(
        s3.clone(),
        hourly_files.clone(),
        config.target_row_group_size,
    )
    .await?;

    if merged_data.is_empty() {
        warn!("No data to write for {}", date_str);
        return Ok(());
    }

    // Write compacted file to daily partition (no hour subdirectory)
    let output_key = format!("events-compacted/dt={}/part-00000.parquet", date_str);
    s3.put_object(&output_key, merged_data).await?;

    info!("Compacted {} written to {}", date_str, output_key);

    // Delete original hourly files
    s3.delete_objects(hourly_files).await?;

    info!("Cleaned up hourly files for {}", date_str);
    Ok(())
}

/// Run compaction for configured lookback period
async fn run_compaction(s3: Arc<dyn S3Ops>, config: CompactorConfig) -> Result<()> {
    let today = Utc::now().date_naive();

    for day_offset in 0..config.lookback_days {
        let target_date = today - Duration::days(day_offset);

        if let Err(e) = compact_day(s3.clone(), target_date, &config).await {
            error!("Failed to compact {}: {}", target_date, e);
            // Continue with other days
        }
    }

    Ok(())
}

/// Compactor state
struct CompactorState {
    s3: Arc<dyn S3Ops>,
    config: CompactorConfig,
    last_run: Arc<Mutex<Option<DateTime<Utc>>>>,
}

/// Run scheduled compaction (called by cron/scheduler)
async fn scheduled_compaction(state: Arc<CompactorState>) -> Result<()> {
    // Check if we need to run (at most once per day)
    {
        let mut last_run = state.last_run.lock().await;
        let now = Utc::now();

        if let Some(last) = *last_run {
            let hours_since = (now - last).num_hours();
            if hours_since < 12 {
                // Don't run more than once every 12 hours
                info!("Skipping compaction: ran {} hours ago", hours_since);
                return Ok(());
            }
        }

        *last_run = Some(now);
    }

    info!("Starting scheduled compaction");
    run_compaction(state.s3.clone(), state.config.clone()).await?;
    info!("Compaction completed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_compactor=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let s3_bucket =
        std::env::var("TRACE_S3_BUCKET").expect("TRACE_S3_BUCKET must be set");
    let s3_region =
        std::env::var("TRACE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let s3_prefix =
        std::env::var("TRACE_S3_PREFIX").unwrap_or_else(|_| "trace-events".to_string());

    // Check if we should run Iceberg compaction
    // Set ICEBERG_COMPACTION=true to enable Iceberg table compaction
    // instead of regular event compaction
    let iceberg_mode = std::env::var("ICEBERG_COMPACTION")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(false);

    let lookback_days = std::env::var("COMPACTOR_LOOKBACK_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);

    let s3_config = S3Config {
        bucket: s3_bucket,
        region: s3_region,
        key_prefix: s3_prefix,
    };

    let s3_client = S3Client::new(s3_config.clone()).await?;
    let s3: Arc<dyn S3Ops> = Arc::new(s3_client);

    if iceberg_mode {
        // Run Iceberg compaction
        let iceberg_config = iceberg::IcebergCompactorConfig {
            lookback_days,
            ..Default::default()
        };

        info!("Starting Iceberg compaction mode");
        if let Err(e) = iceberg::run_iceberg_compaction(s3, iceberg_config).await {
            error!("Iceberg compaction failed: {}", e);
            return Err(e);
        }
        info!("Iceberg compaction completed successfully");
    } else {
        // Run regular event compaction
        let config = CompactorConfig {
            lookback_days,
            ..Default::default()
        };

        let state = Arc::new(CompactorState {
            s3,
            config,
            last_run: Arc::new(Mutex::new(None)),
        });

        // Run once on startup
        info!("Running initial compaction");
        if let Err(e) = scheduled_compaction(state.clone()).await {
            error!("Initial compaction failed: {}", e);
        }

        // Enter scheduling mode - wait for SIGTERM
        info!("TRACE compactor running (waiting for scheduled runs or shutdown)");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
            }
            _ = shutdown_signal() => {
                info!("Shutting down...");
            }
        }
    }

    Ok(())
}

/// Listen for shutdown signals
async fn shutdown_signal() {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap()
        .recv()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_date_from_partition() {
        assert_eq!(
            parse_date_from_partition("dt=2026-05-08"),
            Some(NaiveDate::from_ymd_opt(2026, 5, 8).unwrap())
        );
        assert_eq!(
            parse_date_from_partition("dt=2026-01-01"),
            Some(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
        );
        assert_eq!(parse_date_from_partition("dt=invalid"), None);
        assert_eq!(parse_date_from_partition("events/"), None);
    }

    #[test]
    fn test_compactor_config_default() {
        let config = CompactorConfig::default();
        assert_eq!(config.lookback_days, 7);
        assert_eq!(config.min_files_to_compact, 2);
        assert_eq!(config.target_row_group_size, 1_000_000);
    }

    struct MockS3 {
        data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl MockS3 {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        #[allow(dead_code)]
        async fn put_test_data(&self, key: &str, data: Vec<u8>) {
            let mut store = self.data.lock().await;
            store.insert(key.to_string(), data);
        }
    }

    #[async_trait]
    impl S3Ops for MockS3 {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
            let store = self.data.lock().await;
            let keys: Vec<String> = store
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            Ok(keys)
        }

        async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
            let store = self.data.lock().await;
            store
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Key not found: {}", key))
        }

        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
            let mut store = self.data.lock().await;
            store.insert(key.to_string(), data);
            Ok(())
        }

        async fn delete_objects(&self, keys: Vec<String>) -> Result<()> {
            let mut store = self.data.lock().await;
            for key in keys {
                store.remove(&key);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_s3_mock_operations() {
        let mock = Arc::new(MockS3::new());
        let s3: Arc<dyn S3Ops> = mock.clone();

        // Test put and get
        let test_data = b"hello world".to_vec();
        s3.put_object("test/key", test_data.clone()).await.unwrap();

        let retrieved = s3.get_object("test/key").await.unwrap();
        assert_eq!(retrieved, test_data);

        // Test list
        s3.put_object("test/another", vec![1, 2, 3]).await.unwrap();
        let keys = s3.list_objects("test/").await.unwrap();
        assert_eq!(keys.len(), 2);

        // Test delete
        s3.delete_objects(vec!["test/key".to_string()]).await.unwrap();
        let keys_after = s3.list_objects("test/").await.unwrap();
        assert_eq!(keys_after.len(), 1);
    }

    #[tokio::test]
    async fn test_compactor_config_lookback() {
        let config = CompactorConfig {
            lookback_days: 3,
            ..Default::default()
        };
        assert_eq!(config.lookback_days, 3);
    }
}
