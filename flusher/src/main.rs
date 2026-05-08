use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client};
use aws_smithy_types::byte_stream::ByteStream;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use tracing_subscriber::prelude::*;

/// Event from the collector (matches collector schema)
#[derive(Debug, Deserialize, Serialize)]
struct CollectorEvent {
    ts: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ua: Option<String>,
    url: String,
    params: HashMap<String, String>,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
}

/// S3 configuration
#[derive(Clone)]
struct S3Config {
    bucket: String,
    region: String,
    key_prefix: String,
    endpoint_url: Option<String>,
}

/// S3 upload trait for testability
#[async_trait]
trait S3Upload: Send + Sync {
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()>;
}

/// Real S3 implementation
struct S3Client {
    client: Client,
    config: S3Config,
}

impl S3Client {
    async fn new(config: S3Config) -> Result<Self> {
        let region = Region::new(config.region.clone());

        let client = if let Some(endpoint) = &config.endpoint_url {
            // MinIO or S3-compatible service with custom endpoint
            info!("Using custom S3 endpoint: {}", endpoint);
            let s3_config = aws_sdk_s3::Config::builder()
                .region(region)
                .endpoint_url(endpoint)
                .behavior_version_latest()
                .build();

            Client::from_conf(s3_config)
        } else {
            // Standard AWS S3
            info!("Using AWS S3 in region: {}", config.region);
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .load()
                .await;

            Client::new(&aws_config)
        };

        Ok(Self { client, config })
    }
}

#[async_trait]
impl S3Upload for S3Client {
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let full_key = format!("{}/{}", self.config.key_prefix, key);

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
            .context("S3 upload failed")?;

        info!("Uploaded to s3://{}/{}", self.config.bucket, full_key);
        Ok(())
    }
}

/// Flusher state
struct FlusherState {
    log_dir: PathBuf,
    s3: Arc<dyn S3Upload>,
    dlq_dir: PathBuf,
    processed: Arc<Mutex<HashMap<String, bool>>>,
    batch: Arc<Mutex<BatchAccumulator>>,
}

/// Batch accumulator configuration
#[derive(Clone)]
struct BatchConfig {
    max_batch_size_bytes: usize,
    max_batch_age_secs: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size_bytes: 10 * 1024 * 1024,
            max_batch_age_secs: 300,
        }
    }
}

#[derive(Clone)]
struct BatchEntry {
    data: Vec<u8>,
    key: String,
    added_at: Instant,
    source_file: PathBuf,
}

struct BatchAccumulator {
    entries: HashMap<String, Vec<BatchEntry>>,
    total_size_bytes: usize,
    oldest_entry_at: Option<Instant>,
    config: BatchConfig,
}

impl BatchAccumulator {
    fn new(config: BatchConfig) -> Self {
        Self {
            entries: HashMap::new(),
            total_size_bytes: 0,
            oldest_entry_at: None,
            config,
        }
    }

    fn add(&mut self, partition_key: String, data: Vec<u8>, source_file: PathBuf) -> AddedToBatch {
        let entry_size = data.len();
        let now = Instant::now();

        if self.oldest_entry_at.is_none() {
            self.oldest_entry_at = Some(now);
        }

        // Get the current count for this partition to generate a unique part number
        let part_number = self
            .entries
            .get(&partition_key)
            .map(|v| v.len())
            .unwrap_or(0);
        let key = format!("{}/part-{:05}.parquet", partition_key, part_number);
        let entry = BatchEntry {
            data,
            key,
            added_at: now,
            source_file,
        };

        self.entries
            .entry(partition_key)
            .or_insert_with(Vec::new)
            .push(entry);
        self.total_size_bytes += entry_size;

        self.should_flush()
    }

    fn should_flush(&self) -> AddedToBatch {
        if self.total_size_bytes >= self.config.max_batch_size_bytes {
            return AddedToBatch::ShouldFlushSize(self.total_size_bytes);
        }

        if let Some(oldest) = self.oldest_entry_at {
            let elapsed = oldest.elapsed().as_secs();
            if elapsed >= self.config.max_batch_age_secs {
                return AddedToBatch::ShouldFlushTime(elapsed);
            }
        }

        AddedToBatch::Continue
    }

    fn drain(&mut self) -> HashMap<String, Vec<BatchEntry>> {
        self.total_size_bytes = 0;
        self.oldest_entry_at = None;
        std::mem::take(&mut self.entries)
    }

    fn size_bytes(&self) -> usize {
        self.total_size_bytes
    }

    fn entry_count(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }
}

enum AddedToBatch {
    Continue,
    ShouldFlushSize(usize),
    ShouldFlushTime(u64),
}

/// Parse hour key from filename (events-YYYYMMDD-HH.jsonl.gz)
fn parse_hour_key(filename: &str) -> Option<(String, String)> {
    let base = filename.strip_suffix(".jsonl.gz")?;
    let rest = base.strip_prefix("events-")?;

    if let Some(idx) = rest.rfind('-') {
        let date_part = &rest[..idx];
        let hour_part = &rest[idx + 1..];

        if date_part.len() == 8 && hour_part.len() == 2 {
            // Format: YYYYMMDD-HH -> YYYY-MM-DD and HH
            let dt = format!(
                "{}-{}-{}",
                &date_part[0..4],
                &date_part[4..6],
                &date_part[6..8]
            );
            return Some((dt, hour_part.to_string()));
        }
    }

    None
}

/// Convert JSONL to Parquet in memory
fn jsonl_to_parquet(events: Vec<CollectorEvent>) -> Result<Vec<u8>> {
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let timestamps: Vec<i64> = events.iter().map(|e| e.ts.timestamp_millis()).collect();
    let ips: Vec<Option<String>> = events.iter().map(|e| e.ip.clone()).collect();
    let uas: Vec<Option<String>> = events.iter().map(|e| e.ua.clone()).collect();
    let urls: Vec<String> = events.iter().map(|e| e.url.clone()).collect();
    let params_json: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(&e.params).unwrap_or_default())
        .collect();
    let types: Vec<String> = events.iter().map(|e| e.event_type.clone()).collect();
    let session_ids: Vec<Option<String>> = events.iter().map(|e| e.session_id.clone()).collect();
    let user_ids: Vec<Option<String>> = events.iter().map(|e| e.user_id.clone()).collect();

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("ip", DataType::Utf8, true),
        Field::new("ua", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, false),
        Field::new("params", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(ips)),
            Arc::new(StringArray::from(uas)),
            Arc::new(StringArray::from(urls)),
            Arc::new(StringArray::from(params_json)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(session_ids)),
            Arc::new(StringArray::from(user_ids)),
        ],
    )?;

    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}

/// Process a single JSONL.gz file and add to batch
async fn process_file(state: &FlusherState, path: &PathBuf) -> Result<AddedToBatch> {
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

    info!("Processing file: {}", filename);

    let (dt, hour) = parse_hour_key(filename)
        .ok_or_else(|| anyhow::anyhow!("Cannot parse hour key from filename"))?;

    // Parse JSONL and collect events
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if let Ok(event) = serde_json::from_str::<CollectorEvent>(&line) {
            events.push(event);
        } else {
            warn!("Skipping invalid JSON line in {}", filename);
        }
    }

    if events.is_empty() {
        info!("No valid events in {}, skipping", filename);
        // Mark as processed and delete the empty file
        {
            let mut processed = state.processed.lock().await;
            processed.insert(filename.to_string(), true);
        }
        tokio::fs::remove_file(path).await?;
        info!("Removed empty file: {}", filename);
        return Ok(AddedToBatch::Continue);
    }

    // Convert to Parquet
    let parquet_data = jsonl_to_parquet(events)?;

    // Add to batch accumulator
    let partition_key = format!("events/dt={}/hour={}", dt, hour);
    let result = {
        let mut batch = state.batch.lock().await;
        batch.add(partition_key, parquet_data, path.clone())
    };

    // Mark as processed
    {
        let mut processed = state.processed.lock().await;
        processed.insert(filename.to_string(), true);
    }

    info!(
        "Added {} to batch (size: {}, entries: {})",
        filename,
        state.batch.lock().await.size_bytes(),
        state.batch.lock().await.entry_count()
    );

    Ok(result)
}

/// Flush the batch accumulator to S3
async fn flush_batch(state: &FlusherState, reason: &str) -> Result<()> {
    let entries = {
        let mut batch = state.batch.lock().await;
        if batch.entry_count() == 0 {
            debug!("Batch is empty, nothing to flush");
            return Ok(());
        }
        info!(
            "Flushing batch: {} entries, {} bytes (reason: {})",
            batch.entry_count(),
            batch.size_bytes(),
            reason
        );
        batch.drain()
    };

    let mut upload_errors = Vec::new();

    // Upload all entries to S3
    for (partition_key, entries) in &entries {
        for entry in entries {
            match state.s3.upload(&entry.key, entry.data.clone()).await {
                Ok(()) => {
                    info!("Uploaded: {}", entry.key);
                }
                Err(e) => {
                    error!("Failed to upload {}: {}", entry.key, e);
                    upload_errors.push((entry.clone(), e));
                }
            }
        }
    }

    // Delete source files for successful uploads
    for (partition_key, entries) in &entries {
        for entry in entries {
            // Only delete if upload succeeded (not in errors list)
            let had_error = upload_errors.iter().any(|(e, _)| e.key == entry.key);
            if !had_error {
                if let Err(e) = tokio::fs::remove_file(&entry.source_file).await {
                    warn!("Failed to remove source file {:?}: {}", entry.source_file, e);
                } else {
                    debug!("Removed source file: {:?}", entry.source_file);
                }
            }
        }
    }

    // Move failed uploads to DLQ
    for (entry, error) in upload_errors {
        move_to_dlq(state, &entry.source_file, &error.to_string()).await;
    }

    info!(
        "Batch flush complete: {} entries uploaded",
        entries.values().map(|v| v.len()).sum::<usize>() - upload_errors.len()
    );

    Ok(())
}

/// Move failed file to DLQ
async fn move_to_dlq(state: &FlusherState, path: &PathBuf, error: &str) {
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    let dlq_path = state
        .dlq_dir
        .join(filename)
        .with_extension("jsonl.gz.failed");

    if let Err(e) = tokio::fs::rename(path, &dlq_path).await {
        error!("Failed to move to DLQ: {}", e);
        return;
    }

    // Write error info
    let error_path = dlq_path.with_extension("error");
    if let Err(e) = tokio::fs::write(&error_path, error).await {
        error!("Failed to write error info: {}", e);
    }

    warn!("Moved {} to DLQ: {}", filename, error);
}

/// Handle new file event
async fn handle_file(state: Arc<FlusherState>, path: PathBuf) {
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    // Check if already processed
    {
        let processed = state.processed.lock().await;
        if processed.get(filename).is_some() {
            debug!("Already processed: {}", filename);
            return;
        }
    }

    // Only process .jsonl.gz files
    if !filename.ends_with(".jsonl.gz") {
        return;
    }

    info!("Found new file to process: {}", filename);

    // Process with retries
    let mut retries = 3;
    let state_clone = state.clone();

    while retries > 0 {
        match process_file(&state_clone, &path).await {
            Ok(AddedToBatch::Continue) => {
                info!("Successfully processed {} (added to batch)", filename);
                return;
            }
            Ok(AddedToBatch::ShouldFlushSize(size)) => {
                info!(
                    "Successfully processed {} (batch size limit reached: {} bytes)",
                    filename, size
                );
                // Trigger flush
                if let Err(e) = flush_batch(&state_clone, "size limit").await {
                    error!("Failed to flush batch: {}", e);
                }
                return;
            }
            Ok(AddedToBatch::ShouldFlushTime(age)) => {
                info!(
                    "Successfully processed {} (batch age limit reached: {} secs)",
                    filename, age
                );
                // Trigger flush
                if let Err(e) = flush_batch(&state_clone, "age limit").await {
                    error!("Failed to flush batch: {}", e);
                }
                return;
            }
            Err(e) => {
                retries -= 1;
                if retries > 0 {
                    warn!("Failed to process {}, retrying... ({})", filename, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                } else {
                    error!("Failed to process {} after retries: {}", filename, e);
                    move_to_dlq(&state_clone, &path, &e.to_string()).await;
                }
            }
        }
    }
}

/// Scan existing files in log directory
async fn scan_existing_files(state: Arc<FlusherState>) -> Result<()> {
    let mut entries = tokio::fs::read_dir(&state.log_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            handle_file(state.clone(), path).await;
        }
    }

    Ok(())
}

/// Setup file watcher
fn setup_watcher(state: Arc<FlusherState>) -> Result<RecommendedWatcher> {
    let log_dir = state.log_dir.clone();

    let mut watcher = RecommendedWatcher::new(
        move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                if event.kind.is_create() || event.kind.is_modify() {
                    for path in event.paths {
                        if path.is_file() {
                            let state = state.clone();
                            tokio::spawn(async move {
                                // Small delay to ensure file write is complete
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                handle_file(state, path).await;
                            });
                        }
                    }
                }
            }
        },
        Config::default(),
    )?;

    watcher.watch(&log_dir, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_flusher=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let log_dir =
        PathBuf::from(std::env::var("TRACE_LOG_DIR").unwrap_or_else(|_| "/data/logs".to_string()));
    let dlq_dir =
        PathBuf::from(std::env::var("TRACE_DLQ_DIR").unwrap_or_else(|_| "/data/dlq".to_string()));
    let s3_bucket = std::env::var("TRACE_S3_BUCKET").expect("TRACE_S3_BUCKET must be set");
    let s3_region = std::env::var("TRACE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let s3_prefix = std::env::var("TRACE_S3_PREFIX").unwrap_or_else(|_| "trace-events".to_string());
    let s3_endpoint = std::env::var("TRACE_S3_ENDPOINT").ok();

    // Batch configuration from environment
    let max_batch_size_bytes = std::env::var("TRACE_BATCH_MAX_SIZE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10 * 1024 * 1024);
    let max_batch_age_secs = std::env::var("TRACE_BATCH_MAX_AGE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    let batch_config = BatchConfig {
        max_batch_size_bytes,
        max_batch_age_secs,
    };
    info!(
        "Batch config: max_size_bytes={}, max_age_secs={}",
        batch_config.max_batch_size_bytes, batch_config.max_batch_age_secs
    );

    // Create directories
    tokio::fs::create_dir_all(&log_dir).await?;
    tokio::fs::create_dir_all(&dlq_dir).await?;

    let s3_config = S3Config {
        bucket: s3_bucket,
        region: s3_region,
        key_prefix: s3_prefix,
        endpoint_url: s3_endpoint,
    };

    info!(
        "S3 config: bucket={}, region={}, prefix={}, endpoint={:?}",
        s3_config.bucket, s3_config.region, s3_config.key_prefix, s3_config.endpoint_url
    );

    let s3_client = S3Client::new(s3_config.clone()).await?;
    let s3: Arc<dyn S3Upload> = Arc::new(s3_client);

    let state = Arc::new(FlusherState {
        log_dir,
        s3,
        dlq_dir,
        processed: Arc::new(Mutex::new(HashMap::new())),
        batch: Arc::new(Mutex::new(BatchAccumulator::new(batch_config))),
    });

    // Scan existing files on startup
    info!("Scanning existing files in log directory");
    if let Err(e) = scan_existing_files(state.clone()).await {
        warn!("Error scanning existing files: {}", e);
    }

    // Start file watcher
    info!("Starting file watcher for: {:?}", state.log_dir);
    let _watcher = setup_watcher(state.clone())?;

    // Start periodic flush check task
    let state_for_flush = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let should_flush = {
                let batch = state_for_flush.batch.lock().await;
                matches!(
                    batch.should_flush(),
                    AddedToBatch::ShouldFlushSize(_) | AddedToBatch::ShouldFlushTime(_)
                )
            };
            if should_flush {
                info!("Periodic check: batch needs flush");
                if let Err(e) = flush_batch(&state_for_flush, "periodic check").await {
                    error!("Failed to flush batch during periodic check: {}", e);
                }
            }
        }
    });

    // Keep alive
    info!("TRACE flusher running");

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
        _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())? => {
            info!("Received TERM signal");
        }
    }

    // Flush any remaining entries before shutdown
    info!("Flushing remaining entries before shutdown...");
    let entry_count = state.batch.lock().await.entry_count();
    if entry_count > 0 {
        if let Err(e) = flush_batch(&state, "shutdown").await {
            error!("Failed to flush batch during shutdown: {}", e);
        }
    } else {
        info!("No remaining entries to flush");
    }

    info!("Shutting down...");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hour_key_valid() {
        let cases = vec![
            (
                "events-20260508-14.jsonl.gz",
                Some(("2026-05-08".to_string(), "14".to_string())),
            ),
            (
                "events-20260101-00.jsonl.gz",
                Some(("2026-01-01".to_string(), "00".to_string())),
            ),
            (
                "events-20261231-23.jsonl.gz",
                Some(("2026-12-31".to_string(), "23".to_string())),
            ),
        ];

        for (filename, expected) in cases {
            let result = parse_hour_key(filename);
            assert_eq!(result, expected, "Failed for filename: {}", filename);
        }
    }

    #[test]
    fn test_parse_hour_key_invalid() {
        let cases = vec![
            "events-20260508-14.jsonl",
            "events-20260508.jsonl.gz",
            "20260508-14.jsonl.gz",
            "events-16-14.jsonl.gz",
        ];

        for filename in cases {
            let result = parse_hour_key(filename);
            assert!(result.is_none(), "Should return None for: {}", filename);
        }
    }

    #[test]
    fn test_jsonl_to_parquet_conversion() {
        let events = vec![
            CollectorEvent {
                ts: DateTime::parse_from_rfc3339("2026-05-08T14:30:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                ip: Some("1.2.3.4".to_string()),
                ua: Some("Mozilla/5.0".to_string()),
                url: "https://example.com?utm_source=test".to_string(),
                params: vec![("utm_source".to_string(), "test".to_string())]
                    .into_iter()
                    .collect(),
                event_type: "pageview".to_string(),
                session_id: Some("sess-abc-123".to_string()),
                user_id: Some("user-xyz-789".to_string()),
            },
            CollectorEvent {
                ts: DateTime::parse_from_rfc3339("2026-05-08T14:31:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                ip: None,
                ua: None,
                url: "https://example.com/page2".to_string(),
                params: HashMap::new(),
                event_type: "click".to_string(),
                session_id: None,
                user_id: None,
            },
        ];

        let result = jsonl_to_parquet(events);
        assert!(
            result.is_ok(),
            "Parquet conversion failed: {:?}",
            result.err()
        );

        let parquet_data = result.unwrap();
        assert!(!parquet_data.is_empty(), "Parquet data should not be empty");
        assert!(
            parquet_data.len() > 100,
            "Parquet data should have meaningful content"
        );
    }

    #[test]
    fn test_jsonl_to_parquet_empty() {
        let events: Vec<CollectorEvent> = vec![];
        let result = jsonl_to_parquet(events);
        assert!(result.is_ok(), "Should handle empty events");

        let parquet_data = result.unwrap();
        assert!(
            !parquet_data.is_empty(),
            "Empty events should still produce Parquet schema"
        );
    }

    struct MockS3Upload {
        uploads: std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>,
    }

    impl MockS3Upload {
        fn new() -> Self {
            Self {
                uploads: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn get_uploads(&self) -> Vec<(String, Vec<u8>)> {
            self.uploads.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl S3Upload for MockS3Upload {
        async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
            self.uploads.lock().unwrap().push((key.to_string(), data));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_s3_upload_trait() {
        let mock = Arc::new(MockS3Upload::new());
        let s3: Arc<dyn S3Upload> = mock.clone();

        let test_data = b"test data".to_vec();
        s3.upload("test-key", test_data).await.unwrap();

        let uploads = mock.get_uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, "test-key");
        assert_eq!(uploads[0].1, b"test data");
    }

    #[test]
    fn test_batch_accumulator_size_trigger() {
        let config = BatchConfig {
            max_batch_size_bytes: 100,
            max_batch_age_secs: 300,
        };
        let mut accumulator = BatchAccumulator::new(config);

        // Add first entry (50 bytes)
        let result = accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 50],
            PathBuf::from("/tmp/test1.jsonl.gz"),
        );
        assert!(matches!(result, AddedToBatch::Continue));

        // Add second entry (50 bytes, total 100 bytes)
        let result = accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 50],
            PathBuf::from("/tmp/test2.jsonl.gz"),
        );
        assert!(matches!(result, AddedToBatch::ShouldFlushSize(100)));
    }

    #[test]
    fn test_batch_accumulator_time_trigger() {
        let config = BatchConfig {
            max_batch_size_bytes: 10 * 1024 * 1024,
            max_batch_age_secs: 1,
        };
        let mut accumulator = BatchAccumulator::new(config);

        // Add entry
        let result = accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test1.jsonl.gz"),
        );
        assert!(matches!(result, AddedToBatch::Continue));

        // Wait for age trigger
        std::thread::sleep(Duration::from_secs(2));

        // Check should_flush
        let result = accumulator.should_flush();
        assert!(matches!(result, AddedToBatch::ShouldFlushTime(_)));
    }

    #[test]
    fn test_batch_accumulator_drain() {
        let config = BatchConfig::default();
        let mut accumulator = BatchAccumulator::new(config);

        // Add entries
        accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test1.jsonl.gz"),
        );
        accumulator.add(
            "events/dt=2026-05-08/hour=15".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test2.jsonl.gz"),
        );

        assert_eq!(accumulator.entry_count(), 2);
        assert_eq!(accumulator.size_bytes(), 200);

        // Drain
        let entries = accumulator.drain();

        assert_eq!(entries.len(), 2); // 2 partition keys
        assert_eq!(accumulator.entry_count(), 0);
        assert_eq!(accumulator.size_bytes(), 0);
        assert!(accumulator.oldest_entry_at.is_none());
    }

    #[test]
    fn test_batch_accumulator_multiple_partitions() {
        let config = BatchConfig::default();
        let mut accumulator = BatchAccumulator::new(config);

        // Add entries to different partitions
        accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test1.jsonl.gz"),
        );
        accumulator.add(
            "events/dt=2026-05-08/hour=14".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test2.jsonl.gz"),
        );
        accumulator.add(
            "events/dt=2026-05-08/hour=15".to_string(),
            vec![0u8; 100],
            PathBuf::from("/tmp/test3.jsonl.gz"),
        );

        assert_eq!(accumulator.entry_count(), 3);
        assert_eq!(accumulator.size_bytes(), 300);

        let entries = accumulator.drain();
        assert_eq!(entries.len(), 2); // 2 partition keys
        assert_eq!(entries.get("events/dt=2026-05-08/hour=14").unwrap().len(), 2);
        assert_eq!(entries.get("events/dt=2026-05-08/hour=15").unwrap().len(), 1);
    }
}
