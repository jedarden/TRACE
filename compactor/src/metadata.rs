//! Metadata records passed to the Apache Iceberg publisher.
//!
//! Iceberg manifests and snapshots are written by `trace-iceberg` using the
//! Apache Iceberg Rust implementation. This module only retains compaction
//! output details; it does not serialize Iceberg metadata itself.

/// A completed Parquet object produced by compaction.
#[derive(Clone, Debug)]
pub struct CompactedDataFile {
    /// Path relative to the Iceberg table location.
    pub path: String,
    pub size_bytes: usize,
    pub record_count: i64,
}
