//! Iceberg-specific compaction logic for small Parquet files
//!
//! This module handles compaction of small Parquet files in Iceberg table format.
//! It scans for files below a size threshold and merges them into larger, more
//! efficient files suitable for Iceberg tables.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// Import from crate root (declared in main.rs)
use crate::{merge_parquet_files, metadata::generate_iceberg_metadata, S3Ops, S3Client};

/// Iceberg-specific compaction configuration
#[derive(Clone, Debug)]
pub struct IcebergCompactorConfig {
    /// Minimum file size to consider for compaction (bytes)
    pub min_file_size_bytes: usize,
    /// Target file size after compaction (bytes)
    pub target_file_size_bytes: usize,
    /// Minimum number of files to compact together
    pub min_input_files: usize,
    /// Maximum number of files to compact in one job
    pub max_input_files: usize,
    /// Lookback days for compaction
    pub lookback_days: i64,
    /// Source prefix for Parquet files (e.g., "iceberg/ad_events/data")
    pub source_prefix: String,
    /// Table name for logging/metadata
    pub table_name: String,
}

impl Default for IcebergCompactorConfig {
    fn default() -> Self {
        Self {
            min_file_size_bytes: 64 * 1024 * 1024,    // 64MB - compact files smaller than this
            target_file_size_bytes: 512 * 1024 * 1024, // 512MB - target size from Iceberg schema
            min_input_files: 10,                       // Need at least 10 small files
            max_input_files: 1000,                     // Safety limit
            lookback_days: 7,
            source_prefix: "iceberg/ad_events/data".to_string(),
            table_name: "trace.ad_events".to_string(),
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

/// Get metadata about a Parquet file from S3 (HEAD request)
pub async fn get_file_metadata(s3: &Arc<dyn S3Ops>, key: &str) -> Result<ParquetFileMeta> {
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

    // Extract partition from key (e.g., "ts_day=2026-05-08" from ".../ts_day=2026-05-08/...")
    let partition = key
        .split('/')
        .find(|part| part.starts_with("ts_day=") || part.starts_with("dt="))
        .unwrap_or("unknown")
        .to_string();

    Ok(ParquetFileMeta {
        key: key.to_string(),
        size_bytes,
        partition,
        row_count: None, // Would need to read file to get this
    })
}

/// Identify small files that should be compacted
pub async fn find_small_files(
    s3: Arc<dyn S3Ops>,
    config: &IcebergCompactorConfig,
) -> Result<HashMap<String, Vec<ParquetFileMeta>>> {
    info!(
        "Scanning for small files in {} (min size: {} bytes)",
        config.source_prefix, config.min_file_size_bytes
    );

    let mut all_files: Vec<ParquetFileMeta> = Vec::new();

    // List all Parquet files in the source prefix
    let keys = s3.list_objects(&config.source_prefix).await?;

    for key in keys {
        if !key.ends_with(".parquet") {
            continue;
        }

        match get_file_metadata(&s3, &key).await {
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

    info!("Found {} small files to consider for compaction", all_files.len());

    // Group by partition
    let mut by_partition: HashMap<String, Vec<ParquetFileMeta>> = HashMap::new();
    for file in all_files {
        by_partition
            .entry(file.partition.clone())
            .or_insert_with(Vec::new)
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
    partition: &str,
    files: Vec<ParquetFileMeta>,
    config: &IcebergCompactorConfig,
) -> Result<()> {
    let total_input_size: usize = files.iter().map(|f| f.size_bytes).sum();
    let avg_file_size = total_input_size / files.len().max(1);

    info!(
        "Compacting partition '{}': {} files, {} MB total input (avg {} MB per file)",
        partition,
        files.len(),
        total_input_size / 1_048_576,
        avg_file_size / 1_048_576
    );

    // Calculate how many output files we need
    let target_output_size = config.target_file_size_bytes;
    let num_output_files = (total_input_size + target_output_size - 1) / target_output_size;
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
    let mut uploaded_keys = Vec::new();
    for (idx, keys) in output_files.iter().enumerate() {
        info!(
            "Merging group {}/{} ({} files)",
            idx + 1,
            output_files.len(),
            keys.len()
        );

        let merged_data = merge_parquet_files(
            s3.clone(),
            keys.clone(),
            config.target_file_size_bytes / 1_048_576, // Target row group size
        )
        .await?;

        if merged_data.is_empty() {
            warn!("Group {} produced empty output, skipping", idx + 1);
            continue;
        }

        // Generate output key with Iceberg partition structure
        // Format: iceberg/ad_events/data/ts_day=YYYY-MM-DD/part-XXXXX-UUID.parquet
        let output_key = format!(
            "{}/{}/compacted-{:05}.parquet",
            config.source_prefix,
            partition,
            idx
        );

        s3.put_object(&output_key, merged_data).await?;
        uploaded_keys.push(output_key);

        info!(
            "Uploaded compacted file: {} ({} MB)",
            output_key,
            merged_data.len() / 1_048_576
        );
    }

    // Delete original small files
    let original_keys: Vec<String> = files.iter().map(|f| f.key.clone()).collect();
    s3.delete_objects(original_keys).await?;

    info!(
        "Compaction complete for '{}': {} input files -> {} output files",
        partition,
        files.len(),
        uploaded_keys.len()
    );

    Ok(())
}

/// Run Iceberg compaction for configured lookback period
pub async fn run_iceberg_compaction(
    s3: Arc<dyn S3Ops>,
    config: IcebergCompactorConfig,
) -> Result<()> {
    info!(
        "Starting Iceberg compaction for table '{}'",
        config.table_name
    );

    // Find all small files grouped by partition
    let partitions = find_small_files(s3.clone(), &config).await?;

    if partitions.is_empty() {
        info!("No partitions need compaction");
        return Ok(());
    }

    // Compact each partition
    let mut successful_partitions = 0;
    let mut failed_partitions = Vec::new();

    for (partition, files) in partitions {
        // Limit files per job to avoid OOM
        let files: Vec<ParquetFileMeta> = files
            .into_iter()
            .take(config.max_input_files)
            .collect();

        match compact_iceberg_partition(s3.clone(), &partition, files, &config).await {
            Ok(()) => {
                successful_partitions += 1;
            }
            Err(e) => {
                error!("Failed to compact partition '{}': {}", partition, e);
                failed_partitions.push(partition);
            }
        }
    }

    info!(
        "Iceberg compaction complete: {} successful, {} failed",
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
        assert_eq!(config.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(config.min_input_files, 10);
        assert_eq!(config.max_input_files, 1000);
        assert_eq!(config.lookback_days, 7);
        assert_eq!(config.source_prefix, "iceberg/ad_events/data");
        assert_eq!(config.table_name, "trace.ad_events");
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
            ("iceberg/ad_events/data/ts_day=2026-05-08/part-001.parquet", "ts_day=2026-05-08"),
            ("events/dt=2026-05-08/hour=14/part-002.parquet", "dt=2026-05-08"),
            ("data/unknown/path.parquet", "unknown"),
        ];

        for (key, expected_partition) in test_cases {
            let partition = key
                .split('/')
                .find(|part| part.starts_with("ts_day=") || part.starts_with("dt="))
                .unwrap_or("unknown");
            assert_eq!(partition, expected_partition, "Failed for key: {}", key);
        }
    }
}
