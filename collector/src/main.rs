//! TRACE Collector - Log-First Event Collection
//!
//! Accepts raw traffic signals and appends them to rotating log files.
//! No parsing at collection time - all enrichment happens downstream.
//!
//! Design:
//! - HTTP server accepting raw requests (pageviews, clicks, dwell heartbeats,
//!   conversions)
//! - Log-first: append raw requests to rotating log files per hour (UTC)
//! - No parsing at collection time
//! - Must handle 100 rps on single core
//!
//! Endpoints:
//! - `POST /e`      - JSON event body (JS tag)
//! - `GET /p`       - query-string pixel (pageviews)
//! - `GET/POST /c`  - conversion pixel / server-to-server postback
//! - `GET/POST /collect` - combined endpoint

mod log_writer;

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Raw HTTP request captured as-is
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
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
    /// Log file writer with buffered writes
    log_writer: Arc<Mutex<log_writer::LogFileWriter>>,
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
        user_agent: headers
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        referer: headers
            .get("referer")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        x_forwarded_for: headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        x_real_ip: headers
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        accept_language: headers
            .get("accept-language")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        accept_encoding: headers
            .get("accept-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
    }
}

/// Write raw request to log file using the buffered writer
async fn write_raw_request(state: &CollectorState, raw: &RawRequest) -> anyhow::Result<()> {
    let json_line = serde_json::to_string(raw)?;
    let mut writer = state.log_writer.lock().await;
    writer.write_line(&json_line)?;
    Ok(())
}

/// 1x1 transparent GIF for pixel tracking
const PIXEL_GIF: &[u8] = &[
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
    0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x04,
    0x01, 0x00, 0x3B,
];

/// Response wrapper for pixel GIF
struct PixelResponse;

impl IntoResponse for PixelResponse {
    fn into_response(self) -> Response {
        ([(header::CONTENT_TYPE, "image/gif")], PIXEL_GIF).into_response()
    }
}

/// GET /p - Query string endpoint (pixel tracking)
async fn collect_get(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
) -> PixelResponse {
    record_request(&state, "GET", &uri, &headers, None).await;
    PixelResponse
}

/// POST /e - JSON body endpoint
async fn collect_post(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    record_request(&state, "POST", &uri, &headers, Some(body)).await;
    StatusCode::NO_CONTENT
}

/// GET /c - Conversion pixel. Query string carries the conversion details
/// (conversion_type, revenue) plus IDs and any passthrough parameters. The
/// flusher defaults hits on /c to type = 'conversion', so the pixel works
/// even without an explicit type parameter.
async fn collect_conversion_get(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
) -> PixelResponse {
    record_request(&state, "GET", &uri, &headers, None).await;
    PixelResponse
}

/// POST /c - Conversion postback for server-to-server calls (ad networks,
/// order webhooks). Body may be JSON or URL-encoded form data; both are
/// stored raw and parsed downstream.
async fn collect_conversion_post(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    record_request(&state, "POST", &uri, &headers, Some(body)).await;
    StatusCode::NO_CONTENT
}

/// Record a raw request to the log. The request target is kept verbatim:
/// `path` stores the URL path and `query_params` the raw query string.
async fn record_request(
    state: &CollectorState,
    method: &str,
    uri: &Uri,
    headers: &HeaderMap,
    body: Option<String>,
) {
    let raw = RawRequest {
        ts: Utc::now().to_rfc3339(),
        method: method.to_string(),
        path: uri.path().to_string(),
        headers: extract_headers(headers),
        query_params: uri.query().map(|s| s.to_string()),
        body,
        client_ip: extract_client_ip(headers),
    };

    if let Err(e) = write_raw_request(state, &raw).await {
        error!("Failed to write request: {}", e);
    }
}

/// Health check endpoint
async fn health() -> &'static str {
    "OK"
}

/// Shutdown signal handler with graceful log file shutdown
async fn shutdown_signal(state: CollectorState) {
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

    info!("Shutdown signal received, flushing and closing log files...");

    // Prepare for shutdown by flushing buffer, closing file, and signaling Flusher
    let mut writer = state.log_writer.lock().await;
    if let Err(e) = writer.prepare_shutdown() {
        error!("Failed to prepare log writer shutdown: {}", e);
    }
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

    let log_dir =
        PathBuf::from(std::env::var("TRACE_LOG_DIR").unwrap_or_else(|_| "/data/logs".to_string()));

    // Create log directory if it doesn't exist
    tokio::fs::create_dir_all(&log_dir).await?;

    // Initialize log file writer with buffered writes
    let log_writer = log_writer::LogFileWriter::new(log_dir.clone())
        .map_err(|e| anyhow::anyhow!("Failed to initialize log writer: {}", e))?;

    let state = CollectorState {
        log_writer: Arc::new(Mutex::new(log_writer)),
    };

    // Start rotation checker (runs every minute)
    let rotation_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let mut writer = rotation_state.log_writer.lock().await;
            match writer.check_rotation() {
                Ok(rotated) => {
                    if rotated {
                        info!("Log rotation completed successfully");
                    }
                }
                Err(e) => {
                    error!("Log rotation failed: {}", e);
                }
            }
        }
    });

    let app = axum::Router::new()
        .route("/e", axum::routing::post(collect_post))
        .route("/p", axum::routing::get(collect_get))
        .route(
            "/c",
            axum::routing::get(collect_conversion_get).post(collect_conversion_post),
        )
        .route(
            "/collect",
            axum::routing::get(collect_get).post(collect_post),
        )
        .route("/health", axum::routing::get(health))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let port = std::env::var("TRACE_PORT").unwrap_or_else(|_| "8080".to_string());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("TRACE collector listening on {}", listener.local_addr()?);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state))
        .await?;

    info!("TRACE collector shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_extract_headers_captures_referer() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Mozilla/5.0"));
        headers.insert(
            "referer",
            HeaderValue::from_static("https://taboola.com/story"),
        );

        let raw = extract_headers(&headers);

        assert_eq!(raw.referer, Some("https://taboola.com/story".to_string()));
        assert_eq!(raw.user_agent, Some("Mozilla/5.0".to_string()));
    }

    #[test]
    fn test_extract_headers_without_referer() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Mozilla/5.0"));

        let raw = extract_headers(&headers);

        assert_eq!(raw.referer, None);
    }

    /// Build collector state writing into a temp dir, and a helper that
    /// reads back the single logged RawRequest after flushing the buffer.
    fn test_state() -> (CollectorState, TempDir) {
        let dir = TempDir::new().unwrap();
        let writer = log_writer::LogFileWriter::new(dir.path().to_path_buf()).unwrap();
        let state = CollectorState {
            log_writer: Arc::new(Mutex::new(writer)),
        };
        (state, dir)
    }

    /// Flush the buffered writer and deserialize the one logged request.
    fn read_logged_request(dir: &TempDir) -> RawRequest {
        let mut log_path = None;
        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("raw-") && name.ends_with(".jsonl") {
                log_path = Some(entry.path());
            }
        }
        let log_path = log_path.expect("no raw log file written");

        let content = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "expected exactly one logged request");
        serde_json::from_str(lines[0]).unwrap()
    }

    /// The conversion pixel logs the request target verbatim so the flusher
    /// can (a) default the event to type=conversion via the /c path and
    /// (b) read revenue and conversion_type from the query string.
    #[tokio::test]
    async fn test_conversion_pixel_request_is_logged() {
        let (state, dir) = test_state();

        let uri = Uri::from_static(
            "/c?type=conversion&conversion_type=purchase&revenue=49.99&sid=sess-1",
        );
        collect_conversion_get(State(state.clone()), uri, HeaderMap::new()).await;

        state.log_writer.lock().await.flush().unwrap();
        let raw = read_logged_request(&dir);

        assert_eq!(raw.method, "GET");
        assert_eq!(raw.path, "/c");
        assert_eq!(
            raw.query_params.as_deref(),
            Some("type=conversion&conversion_type=purchase&revenue=49.99&sid=sess-1")
        );
    }

    /// The conversion postback logs the body raw (JSON or form data) for
    /// downstream parsing.
    #[tokio::test]
    async fn test_conversion_postback_body_is_logged() {
        let (state, dir) = test_state();

        let uri = Uri::from_static("/c");
        let body =
            r#"{"type":"conversion","conversion_type":"purchase","revenue":33.75,"sid":"sess-9"}"#;
        collect_conversion_post(
            State(state.clone()),
            uri,
            HeaderMap::new(),
            body.to_string(),
        )
        .await;

        state.log_writer.lock().await.flush().unwrap();
        let raw = read_logged_request(&dir);

        assert_eq!(raw.method, "POST");
        assert_eq!(raw.path, "/c");
        assert_eq!(raw.body.as_deref(), Some(body));
    }

    /// The JS tag POSTs scroll events to /e as JSON. Collection is
    /// log-first and type-agnostic: the body must be stored verbatim so
    /// the flusher's parser (not the collector) derives type = scroll.
    #[tokio::test]
    async fn test_scroll_event_post_body_is_logged() {
        let (state, dir) = test_state();

        let body = r#"{"type":"scroll","url":"https://example.com/article","ts":"2026-05-08T14:32:00.000Z","sid":"sess-123","pv":"pv-1","scroll_depth":75,"max_scroll_depth":78}"#;
        collect_post(
            State(state.clone()),
            Uri::from_static("/e"),
            HeaderMap::new(),
            body.to_string(),
        )
        .await;

        state.log_writer.lock().await.flush().unwrap();
        let raw = read_logged_request(&dir);

        assert_eq!(raw.method, "POST");
        assert_eq!(raw.path, "/e");
        assert_eq!(raw.body.as_deref(), Some(body));
    }

    /// Regular endpoints keep their recorded shape: the path is the URL
    /// path alone (no doubled prefix) and the query string is separate.
    #[tokio::test]
    async fn test_pixel_and_event_requests_record_path_and_query() {
        let (state, dir) = test_state();

        collect_get(
            State(state.clone()),
            Uri::from_static("/p?url=https%3A%2F%2Fexample.com&type=pageview"),
            HeaderMap::new(),
        )
        .await;

        state.log_writer.lock().await.flush().unwrap();
        let raw = read_logged_request(&dir);

        assert_eq!(raw.path, "/p");
        assert_eq!(
            raw.query_params.as_deref(),
            Some("url=https%3A%2F%2Fexample.com&type=pageview")
        );
        assert_eq!(raw.body, None);
    }
}
