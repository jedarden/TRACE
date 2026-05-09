//! Iceberg metadata file generation
//!
//! This module handles generation of Iceberg metadata files:
//! - v1.metadata.json (table metadata)
//! - manifest lists
//! - manifest files (data and delete manifests)
//!
//! Reference: https://iceberg.apache.org/spec/

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Iceberg table format version
pub const ICEBERG_SPEC_VERSION: i32 = 1;

/// Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub schemas: Vec<Schema>,
    pub current_schema_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub default_spec_id: i32,
    pub last_partition_id: i32,
    pub properties: HashMap<String, String>,
    pub snapshots: Vec<Snapshot>,
    pub current_snapshot_id: Option<i64>,
    pub snapshot_log: Vec<SnapshotLogEntry>,
    pub metadata_log: Vec<MetadataLogEntry>,
}

/// Iceberg schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_ident: Option<String>,
    pub fields: Vec<NestedField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
}

/// Nested field in schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub field_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// Partition specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

/// Partition field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: String,
}

/// Snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: SnapshotSummary,
    pub schema_id: Option<i32>,
}

/// Snapshot summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub operation: String,
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

/// Snapshot log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Metadata log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    pub timestamp_ms: i64,
    pub metadata_file: String,
}

/// Manifest file content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: Option<i32>,
    pub existing_files_count: Option<i32>,
    pub deleted_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    pub partitions: Vec<PartitionFieldSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<String>,
}

/// Partition field summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFieldSummary {
    pub contains_null: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains_nan: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<String>,
}

/// Data file entry in manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub content: String,
    pub file_path: String,
    pub file_format: String,
    pub partition: PartitionData,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_sizes: Option<Vec<FieldSize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_counts: Option<Vec<FieldSize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value_counts: Option<Vec<FieldSize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nan_value_counts: Option<Vec<FieldSize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct_counts: Option<Vec<FieldSize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bounds: Option<Vec<FieldBound>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bounds: Option<Vec<FieldBound>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
}

/// Partition data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionData {
    #[serde(flatten)]
    pub fields: HashMap<String, String>,
}

/// Field size (column_sizes, value_counts, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSize {
    pub key: i32,
    pub value: i64,
}

/// Field bound (lower_bounds, upper_bounds)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldBound {
    pub key: i32,
    pub value: String,
}

/// Builder for Iceberg table metadata
pub struct IcebergMetadataBuilder {
    table_uuid: String,
    location: String,
    schema: Schema,
    partition_spec: PartitionSpec,
    properties: HashMap<String, String>,
}

impl IcebergMetadataBuilder {
    /// Create a new metadata builder for ad_events table
    pub fn new_ad_events(table_uuid: String, location: String) -> Self {
        let schema = Self::ad_events_schema();
        let partition_spec = Self::daily_partition_spec();
        let properties = Self::default_properties();

        Self {
            table_uuid,
            location,
            schema,
            partition_spec,
            properties,
        }
    }

    /// Create a new metadata builder for sessions table
    pub fn new_sessions(table_uuid: String, location: String) -> Self {
        let schema = Self::sessions_schema();
        let partition_spec = Self::daily_partition_spec();
        let properties = Self::default_properties();

        Self {
            table_uuid,
            location,
            schema,
            partition_spec,
            properties,
        }
    }

    /// Create a new metadata builder for assets table
    pub fn new_assets(table_uuid: String, location: String) -> Self {
        let schema = Self::assets_schema();
        let partition_spec = Self::network_partition_spec();
        let properties = Self::default_properties();

        Self {
            table_uuid,
            location,
            schema,
            partition_spec,
            properties,
        }
    }

    fn default_properties() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("write.format.default".to_string(), "parquet".to_string());
        props.insert("write.compression-codec".to_string(), "zstd".to_string());
        props.insert("write.target-file-size-bytes".to_string(), "536870912".to_string());
        props.insert("history.expire.min-snapshots-to-keep".to_string(), "10".to_string());
        props
    }

    /// Schema for ad_events table
    fn ad_events_schema() -> Schema {
        Schema {
            schema_id: 0,
            type_ident: None,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "ts".to_string(),
                    required: true,
                    field_type: "timestamptz".to_string(),
                    doc: Some("Event timestamp".to_string()),
                },
                NestedField {
                    id: 2,
                    name: "ip".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Client IP address".to_string()),
                },
                NestedField {
                    id: 3,
                    name: "ua".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("User-Agent string".to_string()),
                },
                NestedField {
                    id: 4,
                    name: "url".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Full URL of the page".to_string()),
                },
                NestedField {
                    id: 5,
                    name: "type".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Event type: pageview, click, scroll, dwell".to_string()),
                },
                NestedField {
                    id: 6,
                    name: "session_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Session identifier".to_string()),
                },
                NestedField {
                    id: 7,
                    name: "user_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("User identifier".to_string()),
                },
                NestedField {
                    id: 8,
                    name: "cookie_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("First-party cookie identifier".to_string()),
                },
                NestedField {
                    id: 9,
                    name: "network".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Ad network: taboola, outbrain, mgid, revcontent, unknown".to_string()),
                },
                NestedField {
                    id: 10,
                    name: "campaign_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Campaign identifier".to_string()),
                },
                NestedField {
                    id: 11,
                    name: "campaign_name".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Campaign name".to_string()),
                },
                NestedField {
                    id: 12,
                    name: "creative_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Creative identifier".to_string()),
                },
                NestedField {
                    id: 13,
                    name: "headline".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Ad headline/text".to_string()),
                },
                NestedField {
                    id: 14,
                    name: "image_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Image identifier".to_string()),
                },
                NestedField {
                    id: 15,
                    name: "item_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Content item ID".to_string()),
                },
                NestedField {
                    id: 16,
                    name: "params".to_string(),
                    required: false,
                    field_type: "map<string, string>".to_string(),
                    doc: Some("Raw query parameters".to_string()),
                },
            ],
            identifier_field_ids: None,
        }
    }

    /// Schema for sessions table
    fn sessions_schema() -> Schema {
        Schema {
            schema_id: 0,
            type_ident: None,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "session_id".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Session identifier".to_string()),
                },
                NestedField {
                    id: 2,
                    name: "user_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("User identifier".to_string()),
                },
                NestedField {
                    id: 3,
                    name: "started_at".to_string(),
                    required: true,
                    field_type: "timestamptz".to_string(),
                    doc: Some("Session start time".to_string()),
                },
                NestedField {
                    id: 4,
                    name: "ended_at".to_string(),
                    required: false,
                    field_type: "timestamptz".to_string(),
                    doc: Some("Session end time".to_string()),
                },
                NestedField {
                    id: 5,
                    name: "pageviews".to_string(),
                    required: false,
                    field_type: "int".to_string(),
                    doc: Some("Number of pageviews".to_string()),
                },
                NestedField {
                    id: 6,
                    name: "clicks".to_string(),
                    required: false,
                    field_type: "int".to_string(),
                    doc: Some("Number of clicks".to_string()),
                },
                NestedField {
                    id: 7,
                    name: "scrolls".to_string(),
                    required: false,
                    field_type: "int".to_string(),
                    doc: Some("Number of scroll events".to_string()),
                },
                NestedField {
                    id: 8,
                    name: "dwells".to_string(),
                    required: false,
                    field_type: "int".to_string(),
                    doc: Some("Number of dwell events".to_string()),
                },
                NestedField {
                    id: 9,
                    name: "entry_url".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Entry page URL".to_string()),
                },
                NestedField {
                    id: 10,
                    name: "exit_url".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Exit page URL".to_string()),
                },
                NestedField {
                    id: 11,
                    name: "network".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Attributed ad network".to_string()),
                },
                NestedField {
                    id: 12,
                    name: "campaign_id".to_string(),
                    required: false,
                    field_type: "string".to_string(),
                    doc: Some("Attributed campaign ID".to_string()),
                },
                NestedField {
                    id: 13,
                    name: "converted".to_string(),
                    required: false,
                    field_type: "boolean".to_string(),
                    doc: Some("Whether session converted".to_string()),
                },
            ],
            identifier_field_ids: Some(vec![1]),
        }
    }

    /// Schema for assets table
    fn assets_schema() -> Schema {
        Schema {
            schema_id: 0,
            type_ident: None,
            fields: vec![
                NestedField {
                    id: 1,
                    name: "asset_id".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Unique asset identifier".to_string()),
                },
                NestedField {
                    id: 2,
                    name: "network".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Ad network".to_string()),
                },
                NestedField {
                    id: 3,
                    name: "type".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Asset type: headline, image, video".to_string()),
                },
                NestedField {
                    id: 4,
                    name: "content".to_string(),
                    required: true,
                    field_type: "string".to_string(),
                    doc: Some("Asset content (text, URL, etc)".to_string()),
                },
                NestedField {
                    id: 5,
                    name: "first_seen".to_string(),
                    required: true,
                    field_type: "timestamptz".to_string(),
                    doc: Some("First seen timestamp".to_string()),
                },
                NestedField {
                    id: 6,
                    name: "last_seen".to_string(),
                    required: true,
                    field_type: "timestamptz".to_string(),
                    doc: Some("Last seen timestamp".to_string()),
                },
                NestedField {
                    id: 7,
                    name: "total_views".to_string(),
                    required: false,
                    field_type: "bigint".to_string(),
                    doc: Some("Total views".to_string()),
                },
                NestedField {
                    id: 8,
                    name: "total_clicks".to_string(),
                    required: false,
                    field_type: "bigint".to_string(),
                    doc: Some("Total clicks".to_string()),
                },
                NestedField {
                    id: 9,
                    name: "campaign_count".to_string(),
                    required: false,
                    field_type: "int".to_string(),
                    doc: Some("Number of campaigns used in".to_string()),
                },
                NestedField {
                    id: 10,
                    name: "metadata".to_string(),
                    required: false,
                    field_type: "map<string, string>".to_string(),
                    doc: Some("Additional metadata".to_string()),
                },
            ],
            identifier_field_ids: Some(vec![1, 2]),
        }
    }

    /// Daily partition spec (by day of ts timestamp)
    fn daily_partition_spec() -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1, // ts field
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: "day".to_string(),
            }],
        }
    }

    /// Network partition spec
    fn network_partition_spec() -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 2, // network field
                field_id: 1000,
                name: "network".to_string(),
                transform: "identity".to_string(),
            }],
        }
    }

    /// Build initial table metadata
    pub fn build_initial(&self) -> Result<IcebergTableMetadata> {
        let now = Utc::now();
        let timestamp_ms = now.timestamp_millis();

        Ok(IcebergTableMetadata {
            format_version: ICEBERG_SPEC_VERSION,
            table_uuid: self.table_uuid.clone(),
            location: self.location.clone(),
            last_sequence_number: 0,
            last_updated_ms: timestamp_ms,
            last_column_id: self.schema.fields.iter().map(|f| f.id).max().unwrap_or(0),
            schemas: vec![self.schema.clone()],
            current_schema_id: 0,
            partition_specs: vec![self.partition_spec.clone()],
            default_spec_id: 0,
            last_partition_id: 1000,
            properties: self.properties.clone(),
            snapshots: vec![],
            current_snapshot_id: None,
            snapshot_log: vec![],
            metadata_log: vec![],
        })
    }

    /// Add a snapshot to existing metadata
    pub fn add_snapshot(
        &self,
        mut metadata: IcebergTableMetadata,
        manifest_list: String,
        data_files: Vec<DataFile>,
        summary: HashMap<String, String>,
    ) -> Result<IcebergTableMetadata> {
        let now = Utc::now();
        let timestamp_ms = now.timestamp_millis();

        // Generate snapshot ID from timestamp
        let snapshot_id = timestamp_ms;
        let parent_snapshot_id = metadata.current_snapshot_id;

        // Calculate summary stats
        let total_records: i64 = data_files.iter().map(|f| f.record_count).sum();
        let total_files = data_files.len() as i64;

        let mut summary_with_stats = summary.clone();
        summary_with_stats.insert("total-records".to_string(), total_records.to_string());
        summary_with_stats.insert("total-data-files".to_string(), total_files.to_string());

        let snapshot = Snapshot {
            snapshot_id,
            parent_snapshot_id,
            sequence_number: metadata.last_sequence_number + 1,
            timestamp_ms,
            manifest_list,
            summary: SnapshotSummary {
                operation: "append".to_string(),
                extra: summary_with_stats,
            },
            schema_id: Some(0),
        };

        // Add snapshot log entry
        metadata.snapshot_log.push(SnapshotLogEntry {
            snapshot_id,
            timestamp_ms,
        });

        // Update metadata
        metadata.snapshots.push(snapshot);
        metadata.current_snapshot_id = Some(snapshot_id);
        metadata.last_sequence_number += 1;
        metadata.last_updated_ms = timestamp_ms;

        Ok(metadata)
    }

    /// Serialize metadata to JSON bytes
    pub fn serialize_metadata(&self) -> Result<Vec<u8>> {
        let metadata = self.build_initial()?;
        let json = serde_json::to_string_pretty(&metadata)
            .context("Failed to serialize Iceberg metadata")?;
        Ok(json.into_bytes())
    }

    /// Serialize updated metadata to JSON bytes
    pub fn serialize_metadata_update(&self, metadata: &IcebergTableMetadata) -> Result<Vec<u8>> {
        let json = serde_json::to_string_pretty(metadata)
            .context("Failed to serialize Iceberg metadata")?;
        Ok(json.into_bytes())
    }
}

/// Create data file entry for Iceberg manifest
pub fn create_data_file(
    file_path: String,
    partition_value: String,
    record_count: i64,
    file_size: i64,
) -> DataFile {
    let mut partition = HashMap::new();
    partition.insert("ts_day".to_string(), partition_value);

    DataFile {
        content: "data".to_string(),
        file_path,
        file_format: "parquet".to_string(),
        partition: PartitionData { fields: partition },
        record_count,
        file_size_in_bytes: file_size,
        column_sizes: None,
        value_counts: None,
        null_value_counts: None,
        nan_value_counts: None,
        distinct_counts: None,
        lower_bounds: None,
        upper_bounds: None,
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
    }
}

/// Generate manifest list content
pub fn generate_manifest_list(manifest_paths: Vec<String>) -> Result<Vec<u8>> {
    let manifest_files: Vec<ManifestFile> = manifest_paths
        .iter()
        .map(|path| ManifestFile {
            manifest_path: path.clone(),
            manifest_length: 0, // Would need actual file size
            partition_spec_id: 0,
            content: Some("data".to_string()),
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: vec![],
            key_metadata: None,
        })
        .collect();

    let json = serde_json::to_string_pretty(&manifest_files)
        .context("Failed to serialize manifest list")?;

    Ok(json.into_bytes())
}

/// Generate Iceberg metadata files for a compaction job
///
/// This function creates or updates Iceberg table metadata after compaction:
/// 1. Loads existing metadata or creates initial metadata
/// 2. Creates data file entries for uploaded Parquet files
/// 3. Generates manifest files
/// 4. Generates manifest list
/// 5. Updates table metadata with new snapshot
/// 6. Uploads all metadata to S3
pub async fn generate_iceberg_metadata(
    s3: std::sync::Arc<dyn crate::S3Ops>,
    table_name: &str,
    table_location: &str,
    partition_value: &str,
    data_files: Vec<String>,
    file_sizes: Vec<usize>,
    record_counts: Vec<i64>,
) -> Result<()> {
    if data_files.is_empty() {
        warn!("No data files provided for Iceberg metadata generation");
        return Ok(());
    }

    let metadata_prefix = format!("{}/metadata", table_location.trim_end_matches('/'));
    let metadata_key = format!("{}/v1.metadata.json", metadata_prefix);

    // Load or create table metadata
    let metadata = load_or_create_metadata(s3.clone(), &metadata_key, table_name, table_location).await?;

    // Create data file entries
    let data_file_entries: Vec<DataFile> = data_files
        .iter()
        .zip(file_sizes.iter())
        .zip(record_counts.iter())
        .map(|((path, size), records)| {
            create_data_file(
                format!("{}/{}", table_location.trim_end_matches('/'), path),
                partition_value.to_string(),
                *records,
                *size as i64,
            )
        })
        .collect();

    // Generate manifest file
    let manifest_path = format!("{}/manifest-{:05}.avro", metadata_prefix, Utc::now().timestamp_millis());
    let manifest_content = generate_manifest_content(&data_file_entries)?;
    s3.put_object(&manifest_path, manifest_content).await
        .context("Failed to upload manifest file")?;

    info!("Uploaded manifest: {}", manifest_path);

    // Generate manifest list
    let manifest_list_path = format!("{}/snap-{:05}-{}.avro", metadata_prefix, Utc::now().timestamp_millis(), Uuid::new_v4());
    let manifest_list_content = generate_manifest_list(vec![manifest_path.clone()])?;
    s3.put_object(&manifest_list_path, manifest_list_content).await
        .context("Failed to upload manifest list")?;

    info!("Uploaded manifest list: {}", manifest_list_path);

    // Build updated metadata with new snapshot
    let builder = match table_name {
        "trace.ad_events" => IcebergMetadataBuilder::new_ad_events(
            metadata.table_uuid.clone(),
            table_location.to_string(),
        ),
        "trace.sessions" => IcebergMetadataBuilder::new_sessions(
            metadata.table_uuid.clone(),
            table_location.to_string(),
        ),
        "trace.assets" => IcebergMetadataBuilder::new_assets(
            metadata.table_uuid.clone(),
            table_location.to_string(),
        ),
        _ => {
            warn!("Unknown table {}, using ad_events schema", table_name);
            IcebergMetadataBuilder::new_ad_events(
                metadata.table_uuid.clone(),
                table_location.to_string(),
            )
        }
    };

    // Add snapshot to metadata
    let mut summary = HashMap::new();
    summary.insert("partition".to_string(), partition_value.to_string());

    let updated_metadata = builder.add_snapshot(
        metadata,
        manifest_list_path,
        data_file_entries,
        summary,
    )?;

    // Serialize and upload updated metadata
    let updated_metadata_json = builder.serialize_metadata_update(&updated_metadata)?;
    s3.put_object(&metadata_key, updated_metadata_json).await
        .context("Failed to upload updated metadata")?;

    info!("Updated table metadata: {}", metadata_key);

    Ok(())
}

/// Load existing metadata from S3 or create new metadata
async fn load_or_create_metadata(
    s3: std::sync::Arc<dyn crate::S3Ops>,
    metadata_key: &str,
    table_name: &str,
    table_location: &str,
) -> Result<IcebergTableMetadata> {
    // Try to load existing metadata
    match s3.get_object(metadata_key).await {
        Ok(data) => {
            let json_str = String::from_utf8(data)
                .context("Metadata is not valid UTF-8")?;
            let metadata: IcebergTableMetadata = serde_json::from_str(&json_str)
                .context("Failed to parse existing metadata")?;
            info!("Loaded existing metadata for {}", table_name);
            Ok(metadata)
        }
        Err(_) => {
            // Create new metadata
            info!("Creating new metadata for {}", table_name);
            let table_uuid = Uuid::new_v4().to_string();
            let builder = match table_name {
                "trace.ad_events" => IcebergMetadataBuilder::new_ad_events(
                    table_uuid.clone(),
                    table_location.to_string(),
                ),
                "trace.sessions" => IcebergMetadataBuilder::new_sessions(
                    table_uuid.clone(),
                    table_location.to_string(),
                ),
                "trace.assets" => IcebergMetadataBuilder::new_assets(
                    table_uuid.clone(),
                    table_location.to_string(),
                ),
                _ => IcebergMetadataBuilder::new_ad_events(
                    table_uuid.clone(),
                    table_location.to_string(),
                ),
            };
            builder.build_initial()
        }
    }
}

/// Generate manifest file content (simplified JSON format for initial implementation)
///
/// Note: Production Iceberg uses Avro for manifests. This is a simplified JSON format
/// that can be used for initial development and testing.
fn generate_manifest_content(data_files: &[DataFile]) -> Result<Vec<u8>> {
    let manifest = serde_json::json!({
        "format-version": "1",
        "partition-spec-id": 0,
        "partition-field-summary": [],
        "data-files": data_files
    });

    let json = serde_json::to_string_pretty(&manifest)
        .context("Failed to serialize manifest")?;

    Ok(json.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ad_events_schema() {
        let builder = IcebergMetadataBuilder::new_ad_events(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/ad_events".to_string(),
        );
        let schema = builder.schema;

        assert_eq!(schema.fields.len(), 16);
        assert_eq!(schema.fields[0].name, "ts");
        assert_eq!(schema.fields[0].required, true);
        assert_eq!(schema.fields[0].field_type, "timestamptz");
    }

    #[test]
    fn test_sessions_schema() {
        let builder = IcebergMetadataBuilder::new_sessions(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/sessions".to_string(),
        );
        let schema = builder.schema;

        assert_eq!(schema.fields.len(), 13);
        assert_eq!(schema.fields[0].name, "session_id");
        assert_eq!(schema.identifier_field_ids, Some(vec![1]));
    }

    #[test]
    fn test_assets_schema() {
        let builder = IcebergMetadataBuilder::new_assets(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/assets".to_string(),
        );
        let schema = builder.schema;

        assert_eq!(schema.fields.len(), 10);
        assert_eq!(schema.fields[0].name, "asset_id");
        assert_eq!(schema.identifier_field_ids, Some(vec![1, 2]));
    }

    #[test]
    fn test_build_initial_metadata() {
        let builder = IcebergMetadataBuilder::new_ad_events(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/ad_events".to_string(),
        );
        let metadata = builder.build_initial().unwrap();

        assert_eq!(metadata.format_version, ICEBERG_SPEC_VERSION);
        assert_eq!(metadata.table_uuid, "test-uuid");
        assert_eq!(metadata.location, "s3://bucket/iceberg/ad_events");
        assert_eq!(metadata.schemas.len(), 1);
        assert_eq!(metadata.partition_specs.len(), 1);
        assert!(metadata.snapshots.is_empty());
        assert!(metadata.current_snapshot_id.is_none());
    }

    #[test]
    fn test_add_snapshot() {
        let builder = IcebergMetadataBuilder::new_ad_events(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/ad_events".to_string(),
        );
        let metadata = builder.build_initial().unwrap();

        let updated = builder
            .add_snapshot(
                metadata,
                "s3://bucket/iceberg/ad_events/metadata/snap-1.avro".to_string(),
                vec![create_data_file(
                    "s3://bucket/iceberg/ad_events/data/ts_day=2026-05-08/part-00000.parquet"
                        .to_string(),
                    "2026-05-08".to_string(),
                    1000,
                    1024,
                )],
                HashMap::new(),
            )
            .unwrap();

        assert_eq!(updated.snapshots.len(), 1);
        assert!(updated.current_snapshot_id.is_some());
        assert_eq!(updated.snapshot_log.len(), 1);
        assert_eq!(updated.last_sequence_number, 1);
    }

    #[test]
    fn test_create_data_file() {
        let data_file = create_data_file(
            "s3://bucket/iceberg/ad_events/data/ts_day=2026-05-08/part-00000.parquet".to_string(),
            "2026-05-08".to_string(),
            1000,
            1024,
        );

        assert_eq!(data_file.content, "data");
        assert_eq!(data_file.file_format, "parquet");
        assert_eq!(data_file.record_count, 1000);
        assert_eq!(data_file.file_size_in_bytes, 1024);
        assert_eq!(
            data_file.partition.fields.get("ts_day"),
            Some(&"2026-05-08".to_string())
        );
    }

    #[test]
    fn test_partition_specs() {
        let builder = IcebergMetadataBuilder::new_ad_events(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/ad_events".to_string(),
        );
        let spec = &builder.partition_spec;

        assert_eq!(spec.spec_id, 0);
        assert_eq!(spec.fields.len(), 1);
        assert_eq!(spec.fields[0].name, "ts_day");
        assert_eq!(spec.fields[0].transform, "day");
        assert_eq!(spec.fields[0].source_id, 1); // ts field
    }

    #[test]
    fn test_serialize_metadata() {
        let builder = IcebergMetadataBuilder::new_ad_events(
            "test-uuid".to_string(),
            "s3://bucket/iceberg/ad_events".to_string(),
        );
        let bytes = builder.serialize_metadata().unwrap();
        let json_str = String::from_utf8(bytes).unwrap();

        assert!(json_str.contains("\"format-version\": 1"));
        assert!(json_str.contains("\"table-uuid\": \"test-uuid\""));
        assert!(json_str.contains("\"location\": \"s3://bucket/iceberg/ad_events\""));
        assert!(json_str.contains("\"ts\""));
        assert!(json_str.contains("\"ts_day\""));
    }
}
