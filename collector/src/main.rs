//! TRACE Collector - Log-First Event Collection
//!
//! Accepts raw traffic signals and appends them to rotating log files.
//! No parsing at collection time - all enrichment happens downstream.
//!
//! Design:
//! - HTTP server accepting raw requests (pageviews, clicks, dwell heartbeats)
//! - Log-first: append raw requests to rotating log files per hour (UTC)
//! - No parsing at collection time
//! - Must handle 100 rps on single core

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Raw HTTP request captured as-is
#[derive(Debug, Serialize)]
struct RawRequest {
    /// ISO 8601 timestamp when request was received
    ts: String,
    /// HTTP method (GET or POST)
    method: String,
    /// Full request path including query string
    path: String,
    /// Request headers (filtered)
    headers: RawHeaders,
    /// Raw query parameters (if GET request)
    query_params: Option<String>,
    /// Raw body (if POST request)
    body: Option<String>,
    /// Client IP (from X-Forwarded-For or X-Real-IP)
    client_ip: Option<String>,
}

/// Headers we capture from the request
#[derive(Debug, Serialize)]
struct RawHeaders {
    user_agent: Option<String>,
    referer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    x_forwarded_for: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    x_real_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accept_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accept_encoding: Option<String>,
}

/// Shared state for log file rotation
#[derive(Clone)]
struct CollectorState {
    /// Base directory for log files
    log_dir: PathBuf,
    /// Current hour bucket for rotation
    current_hour: Arc<Mutex<String>>,
}

/// Get the current hour key for file rotation (UTC)
fn current_hour_key() -> String {
    Utc::now().format("%Y%m%d-%H").to_string()
}

/// Get log file path for the current hour
fn log_file_path(log_dir: &PathBuf, hour_key: &str) -> PathBuf {
    log_dir.join(format!("raw-{}.jsonl", hour_key))
}

/// Extract client IP from headers
fn extract_client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .or_else(|| headers.get("x-real-ip"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
}

/// Extract relevant headers (filtered, not all headers)
fn extract_headers(headers: &HeaderMap) -> RawHeaders {
    RawHeaders {
        user_agent: headers.get("user-agent").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        referer: headers.get("referer").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        x_forwarded_for: headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        x_real_ip: headers.get("x-real-ip").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        accept_language: headers.get("accept-language").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        accept_encoding: headers.get("accept-encoding").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
    }
}

/// Write raw request to log file (append mode, creating if needed)
fn write_raw_request(log_dir: &PathBuf, raw: &RawRequest) -> std::io::Result<()> {
    let hour_key = current_hour_key();
    let path = log_file_path(log_dir, &hour_key);

    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Open file in append mode, create if it doesn't exist
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;

    // Serialize to JSON and write as a single line
    let json_line = serde_json::to_string(raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    writeln!(file, "{}", json_line)?;

    Ok(())
}

/// 1x1 transparent GIF for pixel tracking
const PIXEL_GIF: &[u8] = &[
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00,
    0x01, 0x00, 0x80, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
    0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x04,
    0x01, 0x00, 0x3B
];

/// Response wrapper for pixel GIF
struct PixelResponse;

impl IntoResponse for PixelResponse {
    fn into_response(self) -> Response {
        (
            [(header::CONTENT_TYPE, "image/gif")],
            PIXEL_GIF,
        )
            .into_response()
    }
}

/// GET /p - Query string endpoint (pixel tracking)
async fn collect_get(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
) -> PixelResponse {
    let query_string = uri.query().map(|s| s.to_string());
    let raw = RawRequest {
        ts: Utc::now().to_rfc3339(),
        method: "GET".to_string(),
        path: format!("/p{}", uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("")),
        headers: extract_headers(&headers),
        query_params: query_string,
        body: None,
        client_ip: extract_client_ip(&headers),
    };

    if let Err(e) = write_raw_request(&state.log_dir, &raw) {
        error!("Failed to write request: {}", e);
    }

    PixelResponse
}

/// POST /e - JSON body endpoint
async fn collect_post(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let query_string = uri.query().map(|s| s.to_string());
    let raw = RawRequest {
        ts: Utc::now().to_rfc3339(),
        method: "POST".to_string(),
        path: format!("/e{}", uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("")),
        headers: extract_headers(&headers),
        query_params: query_string,
        body: Some(body),
        client_ip: extract_client_ip(&headers),
    };

    if let Err(e) = write_raw_request(&state.log_dir, &raw) {
        error!("Failed to write request: {}", e);
    }

    StatusCode::NO_CONTENT
}

/// Health check endpoint
async fn health() -> &'static str {
    "OK"
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
    let terminate = std::future::pending::<()>;

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_collector=info,tower_http=info,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let log_dir = PathBuf::from(
        std::env::var("TRACE_LOG_DIR").unwrap_or_else(|_| "/data/logs".to_string())
    );

    // Create log directory if it doesn't exist
    tokio::fs::create_dir_all(&log_dir).await?;

    let state = CollectorState {
        log_dir,
        current_hour: Arc::new(Mutex::new(current_hour_key())),
    };

    // Start rotation checker (runs every minute)
    let rotation_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let mut current = rotation_state.current_hour.lock().await;
            let new_hour = current_hour_key();
            if *current != new_hour {
                *current = new_hour;
                info!("Log rotation: new hour bucket {}", new_hour);
            }
        }
    });

    let app = axum::Router::new()
        .route("/e", axum::routing::post(collect_post))
        .route("/p", axum::routing::get(collect_get))
        .route("/collect", axum::routing::get(collect_get).post(collect_post))
        .route("/health", axum::routing::get(health))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    info!("TRACE collector listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("TRACE collector shutdown complete");
    Ok(())
}
