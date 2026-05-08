use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Event payload received from clients
#[derive(Debug, Deserialize, Serialize)]
struct EventPayload {
    /// Event type: pageview, click, scroll, dwell
    #[serde(default = "default_event_type")]
    r#type: String,
    /// URL of the page (optional in body, will use query param if missing)
    url: Option<String>,
    /// Custom event data
    #[serde(flatten)]
    extra: HashMap<String, serde_json::Value>,
}

fn default_event_type() -> String {
    "pageview".to_string()
}

/// Internal event representation with metadata
#[derive(Debug, Serialize)]
struct Event {
    /// ISO 8601 timestamp
    ts: DateTime<Utc>,
    /// Client IP (optional - may be stripped by proxy/load balancer)
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<String>,
    /// User-Agent header
    #[serde(skip_serializing_if = "Option::is_none")]
    ua: Option<String>,
    /// Full URL with query parameters
    url: String,
    /// All query parameters as a map
    params: HashMap<String, String>,
    /// Event type
    r#type: String,
}

/// Shared state for the collector
#[derive(Clone)]
struct CollectorState {
    /// Base directory for log files
    log_dir: PathBuf,
    /// Current log file buffer
    current: Arc<tokio::sync::Mutex<Option<LogFile>>>,
}

/// Handle to the current log file with rotation tracking
struct LogFile {
    /// Hour bucket this file is for (YYYYMMDD-HH)
    hour_key: String,
    /// Buffered writer for the JSONL file
    writer: BufWriter<File>,
}

impl LogFile {
    /// Open a new log file for the given hour
    fn open(log_dir: &Path, hour_key: &str) -> Result<Self> {
        let path = log_dir.join(format!("events-{}.jsonl", hour_key));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            hour_key: hour_key.to_string(),
            writer: BufWriter::new(file),
        })
    }

    /// Write a single event line
    fn write_line(&mut self, line: &str) -> Result<()> {
        use std::io::Write;
        writeln!(self.writer, "{}", line)?;
        Ok(())
    }

    /// Flush and close the file
    fn flush(mut self) -> Result<()> {
        use std::io::Write;
        self.writer.flush()?;
        Ok(())
    }
}

/// Get the current hour key for file rotation
fn current_hour_key() -> String {
    Utc::now().format("%Y%m%d-%H").to_string()
}

/// Compress a JSONL file to gzip
fn compress_file(src_path: &PathBuf) -> Result<PathBuf> {
    let gz_path = src_path.with_extension("jsonl.gz");

    let src_file = File::open(src_path)?;
    let file_len = src_file.metadata()?.len();
    let gz_file = File::create(&gz_path)?;
    let encoder = GzEncoder::new(gz_file, Compression::default());

    let mut encoder = BufWriter::new(encoder);
    let src_reader = BufReader::new(src_file);
    std::io::copy(&mut src_reader.take(file_len), &mut encoder)?;
    encoder.flush()?;

    // Remove original after successful compression
    std::fs::remove_file(src_path)?;

    Ok(gz_path)
}

/// Rotate and compress the previous hour's log file
async fn rotate_previous_hour(state: &CollectorState) -> Result<()> {
    let current_key = current_hour_key();
    let mut current = state.current.lock().await;

    if let Some(log_file) = current.take() {
        if log_file.hour_key != current_key {
            // Hour has changed, rotate the old file
            let hour_key = log_file.hour_key.clone();
            drop(current); // Release lock before I/O

            info!("Rotating log file for hour: {}", hour_key);

            // Flush the file
            if let Err(e) = log_file.flush() {
                error!("Failed to flush log file: {}", e);
            }

            // Compress the file
            let old_path = state.log_dir.join(format!("events-{}.jsonl", hour_key));
            tokio::spawn(async move {
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    compress_file(&old_path)
                }).await {
                    warn!("Failed to compress log file: {}", e);
                }
            });
        } else {
            *current = Some(log_file);
        }
    }

    Ok(())
}

/// POST /collect - JSON payload endpoint
async fn collect_json(
    State(state): State<CollectorState>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<EventPayload>,
) -> Result<(), CollectError> {
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());

    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let url = payload
        .url
        .or_else(|| {
            payload.extra.get("url").and_then(|v| {
                if let serde_json::Value::String(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "unknown".to_string());

    let params = extract_params(&url);

    let event = Event {
        ts: Utc::now(),
        ip,
        ua,
        url,
        params,
        r#type: payload.r#type,
    };

    write_event(&state, &event).await?;

    Ok(())
}

/// GET /collect - Query string endpoint
async fn collect_query(
    State(state): State<CollectorState>,
    Query(params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<(), CollectError> {
    let ip = headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());

    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let url = params
        .get("url")
        .cloned()
        .or_else(|| {
            params
                .iter()
                .find(|(k, _)| *k == "referrer" || *k == "ref")
                .map(|(_, v)| v.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let event_type = params.get("type").cloned().unwrap_or_else(|| "pageview".to_string());

    let event = Event {
        ts: Utc::now(),
        ip,
        ua,
        url: url.clone(),
        params,
        r#type: event_type,
    };

    write_event(&state, &event).await?;

    Ok(())
}

/// Extract query parameters from URL
fn extract_params(url: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Ok(parsed) = url::Url::parse(url) {
        for (key, value) in parsed.query_pairs() {
            params.insert(key.to_string(), value.to_string());
        }
    }

    params
}

/// Write event to the current log file with rotation
async fn write_event(state: &CollectorState, event: &Event) -> Result<(), CollectError> {
    let current_key = current_hour_key();
    let line = serde_json::to_string(event).map_err(|e| {
        error!("JSON serialization error: {}", e);
        CollectError::Json
    })?;

    let mut current = state.current.lock().await;

    // Check if we need to rotate
    let needs_rotation = current
        .as_ref()
        .map(|f| f.hour_key != current_key)
        .unwrap_or(true);

    if needs_rotation {
        if let Some(log_file) = current.take() {
            let hour_key = log_file.hour_key.clone();
            let log_dir = state.log_dir.clone();

            // Spawn compression task
            tokio::spawn(async move {
                info!("Rotating log file for hour: {}", hour_key);
                if let Err(e) = log_file.flush() {
                    error!("Failed to flush log file: {}", e);
                }
                let old_path = log_dir.join(format!("events-{}.jsonl", hour_key));
                if let Err(e) = tokio::task::spawn_blocking(move || {
                    compress_file(&old_path)
                }).await {
                    warn!("Failed to compress log file: {}", e);
                }
            });
        }

        // Open new file
        match LogFile::open(&state.log_dir, &current_key) {
            Ok(file) => *current = Some(file),
            Err(e) => {
                error!("Failed to open log file: {}", e);
                return Err(CollectError::Io);
            }
        }
    }

    // Write the event line
    if let Some(ref mut file) = *current {
        if let Err(e) = file.write_line(&line) {
            error!("Failed to write event: {}", e);
            return Err(CollectError::Io);
        }
    }

    Ok(())
}

/// Error type for collection failures
#[derive(Debug)]
enum CollectError {
    Json,
    Io,
}

impl IntoResponse for CollectError {
    fn into_response(self) -> Response {
        error!("Collection error: {:?}", self);
        // Still return 204 - we want fire-and-forget semantics
        // Errors are logged but not exposed to clients
        StatusCode::NO_CONTENT.into_response()
    }
}

/// Shutdown signal handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, flushing buffers...");
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_collector=info,tower_http=info,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let log_dir = PathBuf::from(std::env::var("TRACE_LOG_DIR").unwrap_or_else(|_| "/data/logs".to_string()));

    // Create log directory if it doesn't exist
    tokio::fs::create_dir_all(&log_dir).await?;

    let state = CollectorState {
        log_dir,
        current: Arc::new(tokio::sync::Mutex::new(None)),
    };

    // Initialize the first log file
    {
        let mut current = state.current.lock().await;
        let hour_key = current_hour_key();
        *current = Some(LogFile::open(&state.log_dir, &hour_key)?);
    }

    // Clone state for final flush before moving into app
    let flush_state = state.clone();

    // Start rotation checker (runs every 5 minutes)
    let rotation_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(e) = rotate_previous_hour(&rotation_state).await {
                error!("Rotation check failed: {}", e);
            }
        }
    });

    async fn health() -> &'static str {
        "OK"
    }

    let app = axum::Router::new()
        .route("/collect", axum::routing::get(collect_query).post(collect_json))
        .route("/health", axum::routing::get(health))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    info!("TRACE collector listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Final flush on shutdown
    if let Some(log_file) = flush_state.current.lock().await.take() {
        info!("Flushing final log file...");
        if let Err(e) = log_file.flush() {
            error!("Failed to flush on shutdown: {}", e);
        }
    }

    info!("TRACE collector shutdown complete");
    Ok(())
}
