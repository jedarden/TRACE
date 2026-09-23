//! `trace-replay` — reprocess archived raw logs through the pipeline.
//!
//! The live flusher is one-pass: parse → Parquet → S3 → delete. Raw logs
//! are the documented source of truth precisely so this binary can exist:
//! when parsing, normalization, or sessionization improves, replay the
//! affected hours (or the whole archive) and the derived data is rebuilt
//! from the raw lines — no data loss, no duplicates.
//!
//! ```text
//! trace-replay [RAW_DIR] --from 20260901-00 --to 20260907-23
//! ```
//!
//! Raw inputs are never modified or deleted. Outputs are written under the
//! same bucket/prefix as the live flusher, at deterministic keys, so a
//! re-run overwrites its own previous output instead of stacking duplicates
//! (see `flusher/src/replay.rs` for the full semantics: checkpointing,
//! idempotent keys, the partial-day sessionization guard).
//!
//! Exit status: 0 when every upload succeeded, 1 when any failed (the run
//! still completes what it can — re-run the same command to resume from the
//! checkpoint).

use anyhow::{Context, Result};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use clap::Parser;
use std::path::PathBuf;
use trace_flusher::normalizer::NormalizationMapping;
use trace_flusher::replay::{
    default_network_mapping, run_replay, HourKey, ReplayConfig, ReplaySink, SessionConfig,
};

/// Reprocess raw collector logs through parsing, normalization,
/// sessionization, and asset extraction, with checkpointing and idempotent
/// output keys. Raw inputs are never modified.
#[derive(Debug, Parser)]
#[command(name = "trace-replay", version, about)]
struct Args {
    /// Directory holding the raw-YYYYMMDD-HH.jsonl[.gz|.ready] archive
    /// (default: $TRACE_LOG_DIR or /data/logs)
    raw_dir: Option<PathBuf>,

    /// First hour to replay, inclusive (YYYYMMDD-HH, or YYYYMMDD for the
    /// whole day)
    #[arg(long)]
    from: String,

    /// Last hour to replay, inclusive (YYYYMMDD-HH, or YYYYMMDD for the
    /// whole day)
    #[arg(long)]
    to: String,

    /// Reprocess hours/days the checkpoint already recorded
    #[arg(long)]
    force: bool,

    /// Parse and report what would be uploaded, but upload nothing and
    /// write no checkpoint
    #[arg(long)]
    dry_run: bool,

    /// Skip the sessionization stage (events only)
    #[arg(long)]
    skip_sessions: bool,

    /// Sessionize days even when fewer than 24 hour files are selected —
    /// the day's sessions will then reflect only the selected hours
    #[arg(long)]
    allow_partial_days: bool,

    /// Checkpoint directory (default: <raw_dir>/.replay)
    #[arg(long)]
    checkpoint_dir: Option<PathBuf>,

    /// Network normalization mapping TOML (default: the mapping embedded
    /// from flusher/src/network_mapping.toml)
    #[arg(long)]
    mapping: Option<PathBuf>,
}

/// S3 sink with the same environment configuration as the flusher daemon
/// (TRACE_S3_BUCKET, TRACE_S3_REGION, TRACE_S3_PREFIX, TRACE_S3_ENDPOINT).
struct S3Sink {
    client: Client,
    bucket: String,
}

#[async_trait::async_trait]
impl ReplaySink for S3Sink {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .with_context(|| format!("S3 put failed for {key}"))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let raw_dir = args
        .raw_dir
        .clone()
        .or_else(|| {
            std::env::var("TRACE_LOG_DIR")
                .ok()
                .map(PathBuf::from)
                .or_else(|| Some(PathBuf::from("/data/logs")))
        })
        .context("raw dir must be given or set via TRACE_LOG_DIR")?;

    let from = HourKey::parse_bound(&args.from, false)
        .with_context(|| format!("invalid --from {:?}", args.from))?;
    let to = HourKey::parse_bound(&args.to, true)
        .with_context(|| format!("invalid --to {:?}", args.to))?;

    let mapping = match &args.mapping {
        Some(path) => NormalizationMapping::from_file(path)?,
        None => default_network_mapping()?,
    };

    let checkpoint_dir = args
        .checkpoint_dir
        .clone()
        .unwrap_or_else(|| raw_dir.join(".replay"));
    let checkpoint_path = checkpoint_dir.join("checkpoint.json");

    let s3_bucket = std::env::var("TRACE_S3_BUCKET")
        .context("TRACE_S3_BUCKET must be set (same configuration as the flusher daemon)")?;
    let s3_region = std::env::var("TRACE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let s3_prefix = std::env::var("TRACE_S3_PREFIX").unwrap_or_else(|_| "trace-events".to_string());
    let s3_endpoint = std::env::var("TRACE_S3_ENDPOINT").ok();

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(s3_region))
        .load()
        .await;
    let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);
    if let Some(endpoint) = s3_endpoint {
        builder = builder.endpoint_url(endpoint);
    }
    let sink = S3Sink {
        client: Client::from_conf(builder.build()),
        bucket: s3_bucket,
    };

    let config = ReplayConfig {
        raw_dir,
        from,
        to,
        s3_prefix,
        force: args.force,
        dry_run: args.dry_run,
        skip_sessions: args.skip_sessions,
        allow_partial_days: args.allow_partial_days,
        checkpoint_path,
        mapping,
        session_config: SessionConfig::default(),
    };

    tracing::info!(
        "replaying {}..={} (force={}, dry_run={}, skip_sessions={}, allow_partial_days={})",
        config.from.stem(),
        config.to.stem(),
        config.force,
        config.dry_run,
        config.skip_sessions,
        config.allow_partial_days
    );

    let outcome = run_replay(&config, &sink).await?;
    println!("{}", serde_json::to_string_pretty(&outcome)?);

    if !outcome.ok() {
        std::process::exit(1);
    }
    Ok(())
}
