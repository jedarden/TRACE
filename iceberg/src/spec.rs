//! Iceberg spec constants and Avro schemas (format version 2).
//!
//! Reference: https://iceberg.apache.org/spec/
//!
//! TRACE writes format version 2 tables, append-only: data files only, no
//! position/equality delete files. Compaction is expressed as a `replace`
//! snapshot whose manifest carries both ADDED (rewritten) and DELETED
//! (rewritten-away) entries — the same shape Spark's rewrite_data_files
//! produces.

/// Table format version written by this crate.
pub const FORMAT_VERSION: i32 = 2;

/// Reserved metadata property bounding how many snapshots writers keep.
/// Older snapshots (and the data files only they reference) are expired
/// past this count, which is what bounds storage growth without a separate
/// expiry job.
pub const MIN_SNAPSHOTS_TO_KEEP_PROP: &str = "history.expire.min-snapshots-to-keep";
pub const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: usize = 10;

/// Manifest entry status codes (spec §"Manifests").
pub const ENTRY_STATUS_ADDED: i32 = 1;
pub const ENTRY_STATUS_EXISTING: i32 = 0;
pub const ENTRY_STATUS_DELETED: i32 = 2;

/// data_file content codes (spec v2).
pub const FILE_CONTENT_DATA: i64 = 0;
pub const FILE_CONTENT_POSITION_DELETES: i64 = 1;
pub const FILE_CONTENT_EQUALITY_DELETES: i64 = 2;

/// Name of the metadata directory inside a table location.
pub const METADATA_DIR: &str = "metadata";

/// Filesystem-catalog commit hint. The metadata version is the commit
/// point: writers create `vN+1.metadata.json` and only then update the
/// hint, so readers never see a version whose file does not exist yet.
pub const VERSION_HINT_FILE: &str = "version-hint.text";

/// The Avro schema of a manifest list (spec: `manifest_file`). One record
/// per manifest in the snapshot.
pub const MANIFEST_LIST_AVRO_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    { "name": "manifest_path", "type": "string", "field-id": 500 },
    { "name": "manifest_length", "type": "long", "field-id": 501 },
    { "name": "partition_spec_id", "type": "int", "field-id": 502 },
    { "name": "content", "type": "int", "default": 0, "field-id": 517 },
    { "name": "sequence_number", "type": "long", "default": 0, "field-id": 515 },
    { "name": "min_sequence_number", "type": "long", "default": 0, "field-id": 516 },
    { "name": "added_snapshot_id", "type": "long", "field-id": 503 },
    { "name": "added_files_count", "type": ["null", "int"], "default": null, "field-id": 504 },
    { "name": "existing_files_count", "type": ["null", "int"], "default": null, "field-id": 505 },
    { "name": "deleted_files_count", "type": ["null", "int"], "default": null, "field-id": 506 },
    { "name": "partitions", "type": ["null", { "type": "array", "items": {
        "type": "record", "name": "r508",
        "fields": [
          { "name": "contains_null", "type": "boolean", "field-id": 509 },
          { "name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 510 },
          { "name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 511 },
          { "name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 512 }
        ] } } ], "default": null, "field-id": 507 },
    { "name": "added_rows_count", "type": ["null", "long"], "default": null, "field-id": 513 },
    { "name": "existing_rows_count", "type": ["null", "long"], "default": null, "field-id": 514 },
    { "name": "deleted_rows_count", "type": ["null", "long"], "default": null, "field-id": 518 },
    { "name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519 }
  ]
}
"#;

/// The partitioned record embedded in a manifest's `data_file` entries.
/// The partition fields themselves are table-specific and substituted at
/// write time (`partition_struct_schema`), so this template only pins the
/// record name/field-id shared by all TRACE tables.
pub const PARTITION_STRUCT_TEMPLATE: &str = r#"
{
  "type": "record",
  "name": "r102",
  "fields": __PARTITION_FIELDS__
}
"#;

/// Manifest-list (Avro, Object Container File, null codec) bytes for one
/// snapshot given the manifests that belong to it.
pub mod manifest_list {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ManifestListEntry {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub content: i32,
        pub sequence_number: i64,
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        pub added_files_count: i64,
        pub existing_files_count: i64,
        pub deleted_files_count: i64,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        /// Per-partition-field summary, aligned with the table's partition
        /// spec field order.
        pub partitions: Vec<super::PartitionFieldSummary>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

/// Manifest entry payload as read back from an Avro manifest — the fields
/// the writer needs to compute lineage (deleted-entry sequence numbers) and
/// expiry referenced-file sets.
pub struct ManifestEntryRecord {
    pub status: i32,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
}

/// Encode a primitive partition/stat bound as Iceberg single-value bytes
/// (little-endian; UTF-8 for strings; 1 byte for booleans).
pub fn encode_bound_int(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

pub fn encode_bound_string(v: &str) -> Vec<u8> {
    v.as_bytes().to_vec()
}

pub fn encode_bound_bool(v: bool) -> Vec<u8> {
    vec![if v { 1 } else { 0 }]
}
