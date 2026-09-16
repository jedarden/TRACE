//! Iceberg-specific compaction logic for small Parquet files
//!
//! This module handles compaction of small Parquet files in Iceberg table format.
//! It scans for files below a size threshold and merges them into larger, more
//! efficient files suitable for Iceberg tables.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// Import from crate root (declared in main.rs)
use crate::{merge_parquet_files, metadata, S3Client, S3Ops};

/// One Iceberg table covered by the compaction job
#[derive(Clone, Debug)]
pub struct IcebergTableSpec {
    /// Fully-qualified table name (e.g. "trace.sessions")
    pub table_name: String,
    /// Source prefix for Parquet files, relative to the S3 key prefix
    /// (e.g. "iceberg/sessions/data")
    pub source_prefix: String,
    /// Partition directory prefix inside source_prefix
    /// (e.g. "started_at_day=") — used to group files and to recover the
    /// bare partition value for Iceberg metadata
    pub partition_prefix: String,
    /// Target file size after compaction (bytes) — from the table's DDL
    pub target_file_size_bytes: usize,
}

impl IcebergTableSpec {
    /// trace.ad_events — daily partitions on ts
    pub fn ad_events() -> Self {
        Self {
            table_name: "trace.ad_events".to_string(),
            source_prefix: "iceberg/ad_events/data".to_string(),
            partition_prefix: "ts_day=".to_string(),
            target_file_size_bytes: 512 * 1024 * 1024, // 512MB from ad_events_iceberg.sql
        }
    }

    /// trace.sessions — daily partitions on started_at
    /// (PARTITIONED BY DAYS(started_at), see analytics/schemas/sessions_iceberg.sql)
    pub fn sessions() -> Self {
        Self {
            table_name: "trace.sessions".to_string(),
            source_prefix: "iceberg/sessions/data".to_string(),
            partition_prefix: "started_at_day=".to_string(),
            target_file_size_bytes: 256 * 1024 * 1024, // 256MB from sessions_iceberg.sql
        }
    }

    /// Table path relative to the S3 key prefix (e.g. "iceberg/sessions") —
    /// the anchor for the table's metadata/ directory
    pub fn data_prefix(&self) -> String {
        self.source_prefix
            .trim_end_matches('/')
            .strip_suffix("/data")
            .unwrap_or(self.source_prefix.trim_end_matches('/'))
            .to_string()
    }

    /// Absolute s3:// URI for the table, given the warehouse root
    /// (e.g. warehouse "s3://bucket/trace-events/iceberg" →
    /// "s3://bucket/trace-events/iceberg/sessions")
    pub fn table_location(&self, warehouse: &str) -> String {
        let short_name = self
            .table_name
            .rsplit('.')
            .next()
            .unwrap_or(&self.table_name);
        format!("{}/{}", warehouse.trim_end_matches('/'), short_name)
    }

    /// Bare partition value from a partition directory name
    /// ("started_at_day=2026-09-14" → "2026-09-14")
    pub fn partition_value<'a>(&self, partition: &'a str) -> &'a str {
        partition
            .strip_prefix(&self.partition_prefix)
            .unwrap_or(partition)
    }
}

/// All tables the compaction job covers by default
pub fn default_tables() -> Vec<IcebergTableSpec> {
    vec![IcebergTableSpec::ad_events(), IcebergTableSpec::sessions()]
}

/// Iceberg-specific compaction configuration
#[derive(Clone, Debug)]
pub struct IcebergCompactorConfig {
    /// Minimum file size to consider for compaction (bytes)
    pub min_file_size_bytes: usize,
    /// Minimum number of files to compact together
    pub min_input_files: usize,
    /// Maximum number of files to compact in one job
    pub max_input_files: usize,
    /// Lookback days for compaction
    pub lookback_days: i64,
    /// Tables to compact (ad_events and sessions by default)
    pub tables: Vec<IcebergTableSpec>,
    /// Warehouse root as an s3:// URI (e.g. "s3://bucket/trace-events/iceberg")
    pub warehouse: String,
}

impl Default for IcebergCompactorConfig {
    fn default() -> Self {
        Self {
            min_file_size_bytes: 64 * 1024 * 1024, // 64MB - compact files smaller than this
            min_input_files: 10,                   // Need at least 10 small files
            max_input_files: 1000,                 // Safety limit
            lookback_days: 7,
            tables: default_tables(),
            warehouse: "s3://my-trace-bucket/iceberg".to_string(),
        }
    }
}

/// Metadata about a Parquet file for compaction decisions
#[derive(Clone, Debug)]
pub struct ParquetFileMeta {
    pub key: String,
    pub size_bytes: usize,
    pub partition: String,
    pub row_count: Option<usize>,
}

/// Extract the partition directory component from an object key
/// (e.g. "started_at_day=2026-09-14" from
/// "iceberg/sessions/data/started_at_day=2026-09-14/part-00000.parquet")
fn extract_partition(key: &str, partition_prefix: &str) -> String {
    key.split('/')
        .find(|part| part.starts_with(partition_prefix))
        .unwrap_or("unknown")
        .to_string()
}

/// Get metadata about a Parquet file from S3 (HEAD request)
pub async fn get_file_metadata(
    s3: &Arc<dyn S3Ops>,
    key: &str,
    partition_prefix: &str,
) -> Result<ParquetFileMeta> {
    let s3_client = match s3.as_ref().as_any().downcast_ref::<S3Client>() {
        Some(client) => client,
        None => anyhow::bail!("Cannot get metadata from mock S3"),
    };

    let full_key = s3_client.full_key(key);
    let response = s3_client
        .client
        .head_object()
        .bucket(&s3_client.config.bucket)
        .key(&full_key)
        .send()
        .await
        .context("S3 HEAD request failed")?;

    let size_bytes = response.content_length().unwrap_or(0) as usize;

    let partition = extract_partition(key, partition_prefix);

    Ok(ParquetFileMeta {
        key: key.to_string(),
        size_bytes,
        partition,
        row_count: None, // Would need to read file to get this
    })
}

/// Identify small files that should be compacted for one table
pub async fn find_small_files(
    s3: Arc<dyn S3Ops>,
    table: &IcebergTableSpec,
    config: &IcebergCompactorConfig,
) -> Result<HashMap<String, Vec<ParquetFileMeta>>> {
    info!(
        "Scanning for small files in {} (min size: {} bytes)",
        table.source_prefix, config.min_file_size_bytes
    );

    let mut all_files: Vec<ParquetFileMeta> = Vec::new();

    // List all Parquet files in the source prefix
    let keys = s3.list_objects(&table.source_prefix).await?;

    for key in keys {
        if !key.ends_with(".parquet") {
            continue;
        }

        match get_file_metadata(&s3, &key, &table.partition_prefix).await {
            Ok(meta) => {
                if meta.size_bytes < config.min_file_size_bytes {
                    all_files.push(meta);
                }
            }
            Err(e) => {
                warn!("Failed to get metadata for {}: {}", key, e);
            }
        }
    }

    info!(
        "Found {} small files to consider for compaction",
        all_files.len()
    );

    // Group by partition
    let mut by_partition: HashMap<String, Vec<ParquetFileMeta>> = HashMap::new();
    for file in all_files {
        by_partition
            .entry(file.partition.clone())
            .or_default()
            .push(file);
    }

    // Filter partitions that meet the minimum file count
    by_partition.retain(|_partition, files| {
        let should_compact = files.len() >= config.min_input_files;
        if !should_compact {
            debug!(
                "Skipping partition with {} files (minimum: {})",
                files.len(),
                config.min_input_files
            );
        }
        should_compact
    });

    let total_partitions = by_partition.len();
    let total_files: usize = by_partition.values().map(|v| v.len()).sum();

    info!(
        "Found {} partitions with {} small files to compact",
        total_partitions, total_files
    );

    Ok(by_partition)
}

/// Compact a single partition's small files
pub async fn compact_iceberg_partition(
    s3: Arc<dyn S3Ops>,
    table: &IcebergTableSpec,
    partition: &str,
    files: Vec<ParquetFileMeta>,
    config: &IcebergCompactorConfig,
) -> Result<()> {
    let total_input_size: usize = files.iter().map(|f| f.size_bytes).sum();
    let avg_file_size = total_input_size / files.len().max(1);

    info!(
        "Compacting partition '{}' of {}: {} files, {} MB total input (avg {} MB per file)",
        partition,
        table.table_name,
        files.len(),
        total_input_size / 1_048_576,
        avg_file_size / 1_048_576
    );

    // Calculate how many output files we need
    let target_output_size = table.target_file_size_bytes;
    let num_output_files = total_input_size.div_ceil(target_output_size);
    let files_per_output = files.len().div_ceil(num_output_files.max(1));

    debug!(
        "Target: {} output files of ~{} MB each",
        num_output_files,
        target_output_size / 1_048_576
    );

    // Split files into groups for output
    let mut output_files = Vec::new();
    for chunk in files.chunks(files_per_output) {
        let keys: Vec<String> = chunk.iter().map(|f| f.key.clone()).collect();
        output_files.push(keys);
    }

    // Merge and upload each group
    let mut uploaded: Vec<metadata::CompactedDataFile> = Vec::new();
    for (idx, keys) in output_files.iter().enumerate() {
        info!(
            "Merging group {}/{} ({} files)",
            idx + 1,
            output_files.len(),
            keys.len()
        );

        let (merged_data, row_count) = merge_parquet_files(
            s3.clone(),
            keys.clone(),
            table.target_file_size_bytes / 1_048_576, // Target row group size
        )
        .await?;

        if merged_data.is_empty() {
            warn!("Group {} produced empty output, skipping", idx + 1);
            continue;
        }

        // Generate output key with Iceberg partition structure
        // Format: iceberg/<table>/data/<partition>=YYYY-MM-DD/compacted-XXXXX.parquet
        let output_key = format!(
            "{}/{}/compacted-{:05}.parquet",
            table.source_prefix, partition, idx
        );

        let output_size = merged_data.len();
        s3.put_object(&output_key, merged_data).await?;
        uploaded.push(metadata::CompactedDataFile {
            path: output_key
                .strip_prefix(&table.data_prefix())
                .unwrap_or(&output_key)
                .trim_start_matches('/')
                .to_string(),
            size_bytes: output_size,
            record_count: row_count,
        });

        info!(
            "Uploaded compacted file: {} ({} MB, {} rows)",
            output_key,
            output_size / 1_048_576,
            row_count
        );
    }

    // Register the compacted files in the table's Iceberg metadata before
    // dropping the originals, so a metadata failure never loses data.
    // Paths are recorded relative to the table prefix ("data/<partition>/...").
    let output_count = uploaded.len();
    metadata::generate_iceberg_metadata(
        s3.clone(),
        &table.table_name,
        &table.table_location(&config.warehouse),
        &table.data_prefix(),
        table.partition_value(partition),
        uploaded,
    )
    .await?;

    // Delete original small files
    let original_keys: Vec<String> = files.iter().map(|f| f.key.clone()).collect();
    s3.delete_objects(original_keys).await?;

    info!(
        "Compaction complete for '{}' partition '{}': {} input files -> {} output files",
        table.table_name,
        partition,
        files.len(),
        output_count
    );

    Ok(())
}

/// Run Iceberg compaction over every configured table
pub async fn run_iceberg_compaction(
    s3: Arc<dyn S3Ops>,
    config: IcebergCompactorConfig,
) -> Result<()> {
    let table_names: Vec<&str> = config
        .tables
        .iter()
        .map(|t| t.table_name.as_str())
        .collect();
    info!(
        "Starting Iceberg compaction for tables: {}",
        table_names.join(", ")
    );

    for table in &config.tables {
        if let Err(e) = compact_table(s3.clone(), table, &config).await {
            error!("Iceberg compaction failed for {}: {}", table.table_name, e);
        }
    }

    Ok(())
}

/// Compact one table: find small files grouped by partition, compact each
async fn compact_table(
    s3: Arc<dyn S3Ops>,
    table: &IcebergTableSpec,
    config: &IcebergCompactorConfig,
) -> Result<()> {
    // Find all small files grouped by partition
    let partitions = find_small_files(s3.clone(), table, config).await?;

    if partitions.is_empty() {
        info!("{}: no partitions need compaction", table.table_name);
        return Ok(());
    }

    // Compact each partition
    let mut successful_partitions = 0;
    let mut failed_partitions = Vec::new();

    for (partition, files) in partitions {
        // Limit files per job to avoid OOM
        let files: Vec<ParquetFileMeta> = files.into_iter().take(config.max_input_files).collect();

        match compact_iceberg_partition(s3.clone(), table, &partition, files, config).await {
            Ok(()) => {
                successful_partitions += 1;
            }
            Err(e) => {
                error!(
                    "Failed to compact partition '{}' of {}: {}",
                    partition, table.table_name, e
                );
                failed_partitions.push(partition);
            }
        }
    }

    info!(
        "{} compaction complete: {} successful, {} failed",
        table.table_name,
        successful_partitions,
        failed_partitions.len()
    );

    if !failed_partitions.is_empty() {
        warn!("Failed partitions: {:?}", failed_partitions);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_compactor_config_default() {
        let config = IcebergCompactorConfig::default();
        assert_eq!(config.min_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.min_input_files, 10);
        assert_eq!(config.max_input_files, 1000);
        assert_eq!(config.lookback_days, 7);

        // The compaction job must cover both event and session tables
        let names: Vec<&str> = config
            .tables
            .iter()
            .map(|t| t.table_name.as_str())
            .collect();
        assert!(names.contains(&"trace.ad_events"), "tables: {:?}", names);
        assert!(names.contains(&"trace.sessions"), "tables: {:?}", names);
    }

    #[test]
    fn test_sessions_table_spec() {
        let spec = IcebergTableSpec::sessions();
        assert_eq!(spec.source_prefix, "iceberg/sessions/data");
        assert_eq!(spec.partition_prefix, "started_at_day=");
        assert_eq!(spec.target_file_size_bytes, 256 * 1024 * 1024);
        assert_eq!(spec.data_prefix(), "iceberg/sessions");
        assert_eq!(
            spec.table_location("s3://bucket/trace-events/iceberg"),
            "s3://bucket/trace-events/iceberg/sessions"
        );
    }

    #[test]
    fn test_sessions_partition_value_extraction() {
        let spec = IcebergTableSpec::sessions();
        assert_eq!(
            spec.partition_value("started_at_day=2026-09-14"),
            "2026-09-14"
        );
        // The sessions partition must NOT be extracted with the events prefix
        assert_eq!(
            extract_partition(
                "iceberg/sessions/data/started_at_day=2026-09-14/part-00000.parquet",
                &spec.partition_prefix
            ),
            "started_at_day=2026-09-14"
        );
        assert_ne!(
            extract_partition(
                "iceberg/sessions/data/started_at_day=2026-09-14/part-00000.parquet",
                "ts_day="
            ),
            "started_at_day=2026-09-14"
        );
    }

    /// Mirror of the sessions extraction test for the events table: the
    /// ad_events spec strips its own prefix, and must leave another table's
    /// partition directory untouched rather than silently mis-splitting it.
    #[test]
    fn test_ad_events_partition_value_extraction() {
        let spec = IcebergTableSpec::ad_events();
        assert_eq!(spec.partition_value("ts_day=2026-05-08"), "2026-05-08");
        assert_eq!(
            spec.partition_value("started_at_day=2026-09-14"),
            "started_at_day=2026-09-14"
        );
        assert_eq!(
            extract_partition(
                "iceberg/ad_events/data/ts_day=2026-05-08/part-00000.parquet",
                &spec.partition_prefix
            ),
            "ts_day=2026-05-08"
        );
    }

    #[test]
    fn test_parquet_file_meta_creation() {
        let meta = ParquetFileMeta {
            key: "iceberg/ad_events/data/ts_day=2026-05-08/part-00001.parquet".to_string(),
            size_bytes: 32 * 1024 * 1024, // 32MB
            partition: "ts_day=2026-05-08".to_string(),
            row_count: Some(100000),
        };

        assert_eq!(meta.partition, "ts_day=2026-05-08");
        assert_eq!(meta.size_bytes, 32 * 1024 * 1024);
        assert_eq!(meta.row_count, Some(100000));
    }

    #[test]
    fn test_partition_extraction_from_key() {
        let test_cases = vec![
            (
                "iceberg/ad_events/data/ts_day=2026-05-08/part-001.parquet",
                "ts_day=",
                "ts_day=2026-05-08",
            ),
            (
                "iceberg/sessions/data/started_at_day=2026-09-14/part-002.parquet",
                "started_at_day=",
                "started_at_day=2026-09-14",
            ),
            (
                "events/dt=2026-05-08/hour=14/part-003.parquet",
                "dt=",
                "dt=2026-05-08",
            ),
            ("data/unknown/path.parquet", "ts_day=", "unknown"),
        ];

        for (key, prefix, expected_partition) in test_cases {
            let partition = extract_partition(key, prefix);
            assert_eq!(partition, expected_partition, "Failed for key: {}", key);
        }
    }
}
