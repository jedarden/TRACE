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
//! - `GET/POST /i`  - impression pixel / server-to-server postback
//! - `GET/POST /collect` - combined endpoint
//!
//! The full HTTP contract (responses, size limit, malformed input, retry
//! and idempotency semantics) is documented in docs/notes/collector-api.md
//! and pinned by the `contract_*` tests below.

mod log_writer;

use axum::{
    extract::{DefaultBodyLimit, State},
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

/// Maximum accepted request body size (2 MiB). Larger bodies are rejected
/// with 413 and never reach the log. This matches axum's `DefaultBodyLimit`
/// default; it is set explicitly so the contract is defined here, not
/// inherited from framework defaults.
const MAX_EVENT_BODY_BYTES: usize = 2 * 1024 * 1024;

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

/// GET /i - Impression pixel. Query string carries the impression details
/// (imp_id, creative/ad identifiers, viewability) plus IDs and any
/// passthrough parameters. The flusher defaults hits on /i to
/// type = 'impression', so the pixel works even without an explicit type
/// parameter.
async fn collect_impression_get(
    State(state): State<CollectorState>,
    uri: Uri,
    headers: HeaderMap,
) -> PixelResponse {
    record_request(&state, "GET", &uri, &headers, None).await;
    PixelResponse
}

/// POST /i - Impression postback for server-to-server calls (ad servers,
/// email renderers). Body may be JSON or URL-encoded form data; both are
/// stored raw and parsed downstream.
async fn collect_impression_post(
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

/// Build the collector router. Extracted from `main` so the contract tests
/// exercise the real routing stack — method routing, body limit, and
/// framework-generated rejections (405/404/400/413) included.
fn build_app(state: CollectorState) -> axum::Router {
    axum::Router::new()
        .route("/e", axum::routing::post(collect_post))
        .route("/p", axum::routing::get(collect_get))
        .route(
            "/c",
            axum::routing::get(collect_conversion_get).post(collect_conversion_post),
        )
        .route(
            "/i",
            axum::routing::get(collect_impression_get).post(collect_impression_post),
        )
        .route(
            "/collect",
            axum::routing::get(collect_get).post(collect_post),
        )
        .route("/health", axum::routing::get(health))
        .layer(DefaultBodyLimit::max(MAX_EVENT_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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

    let app = build_app(state.clone());

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
    use axum::body::Body;
    use axum::http::{HeaderValue, Method, Request};
    use http_body_util::BodyExt;
    use std::fs;
    use tempfile::TempDir;
    use tower::ServiceExt;

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

    /// Flush the buffered writer and return every logged request, in
    /// write order. Contract tests log more than one request per file.
    async fn read_all_logged_requests(state: &CollectorState, dir: &TempDir) -> Vec<RawRequest> {
        state.log_writer.lock().await.flush().unwrap();

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
        content
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Send one request through the real router built by `build_app` — the
    /// same stack `main` serves, including method routing, the body size
    /// limit, and framework-generated rejections (405/404/400/413).
    async fn send(state: &CollectorState, req: Request<Body>) -> axum::response::Response {
        build_app(state.clone()).oneshot(req).await.unwrap()
    }

    /// A POST request ready to send through the router.
    fn post(uri: &str, body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(body.into())
            .unwrap()
    }

    /// A GET request ready to send through the router.
    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
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

    // ------------------------------------------------------------------
    // Contract tests. These pin the public HTTP contract documented in
    // docs/notes/collector-api.md: response codes and bodies per endpoint,
    // the recorded-line format, body size limits, malformed-input and
    // rejected-request behavior, retry/idempotency semantics, and client
    // IP resolution. They go through the real router, not the handlers,
    // so framework-generated responses (405/404/400/413) are covered too.
    // ------------------------------------------------------------------

    /// JS-tag ingestion: POST /collect with the JSON payload the tag sends
    /// returns 204 with an empty body, and records the request verbatim —
    /// path, filtered headers, body, and client IP.
    #[tokio::test]
    async fn contract_post_collect_json_event_returns_204_and_logs_verbatim() {
        let (state, dir) = test_state();

        let body = r#"{"type":"pageview","url":"https://example.com/offer?utm_source=taboola","ts":"2026-09-17T12:00:00.000Z","session_id":"sess-42","user_id":"user-7","referrer":"https://taboola.com/","title":"Offer"}"#;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/collect")
            .header("content-type", "application/json")
            .header("user-agent", "Mozilla/5.0 (compatible; TraceTag/1.0)")
            .header("referer", "https://example.com/prev")
            .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
            .body(Body::from(body.to_string()))
            .unwrap();

        let res = send(&state, req).await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert!(bytes.is_empty(), "204 must carry no body");

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1, "exactly one line per accepted request");
        let raw = &logged[0];
        assert_eq!(raw.method, "POST");
        assert_eq!(raw.path, "/collect");
        assert_eq!(raw.query_params, None);
        assert_eq!(raw.body.as_deref(), Some(body), "body stored verbatim");
        assert_eq!(
            raw.headers.user_agent.as_deref(),
            Some("Mozilla/5.0 (compatible; TraceTag/1.0)")
        );
        assert_eq!(
            raw.headers.referer.as_deref(),
            Some("https://example.com/prev")
        );
        assert_eq!(
            raw.client_ip.as_deref(),
            Some("203.0.113.7"),
            "client IP is the first X-Forwarded-For hop"
        );
        // ts is the collector's receive time in RFC 3339 — the client ts
        // inside the body is data, not the record's timestamp.
        chrono::DateTime::parse_from_rfc3339(&raw.ts)
            .expect("logged ts must be RFC 3339 (server receive time)");
    }

    /// POST /e is the dedicated JS-tag event endpoint (scroll, heartbeat,
    /// click payloads). Same contract as POST /collect.
    #[tokio::test]
    async fn contract_post_e_event_returns_204_and_logs() {
        let (state, dir) = test_state();

        let body = r#"{"type":"scroll","url":"https://example.com/article","ts":"2026-09-17T12:01:00.000Z","sid":"sess-123","pv":"pv-1","scroll_depth":75,"max_scroll_depth":78}"#;
        let res = send(&state, post("/e", body.to_string())).await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].method, "POST");
        assert_eq!(logged[0].path, "/e");
        assert_eq!(logged[0].body.as_deref(), Some(body));
    }

    /// Content-Type is never inspected: the tag labels its payloads
    /// application/json, other embedders deliver beacons as text/plain,
    /// and ad-network postbacks use form encoding — all are accepted,
    /// stored raw, and typed downstream.
    #[tokio::test]
    async fn contract_post_collect_accepts_any_content_type() {
        let (state, dir) = test_state();

        let beacon = r#"{"type":"pageview","url":"https://example.com/"}"#;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/collect")
            .header("content-type", "text/plain;charset=UTF-8")
            .body(Body::from(beacon.to_string()))
            .unwrap();
        let res = send(&state, req).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let postback = "sid=sess-9&conversion_type=purchase&revenue=33.75&utm_source=taboola";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/c")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(postback.to_string()))
            .unwrap();
        let res = send(&state, req).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 2);
        assert_eq!(logged[0].body.as_deref(), Some(beacon));
        assert_eq!(logged[1].body.as_deref(), Some(postback));
    }

    /// A POST may carry a query string alongside its body; both are
    /// recorded (the flusher derives event params from the body only).
    #[tokio::test]
    async fn contract_post_query_string_is_also_recorded() {
        let (state, dir) = test_state();

        let res = send(
            &state,
            post(
                "/collect?src=tag&v=2",
                r#"{"type":"click","sid":"sess-5"}"#.to_string(),
            ),
        )
        .await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].query_params.as_deref(), Some("src=tag&v=2"));
        assert_eq!(
            logged[0].body.as_deref(),
            Some(r#"{"type":"click","sid":"sess-5"}"#)
        );
    }

    /// Pixel ingestion: GET /collect returns 200 with the exact 1x1
    /// transparent GIF and an image/gif content type, and records the raw
    /// query string verbatim (still percent-encoded).
    #[tokio::test]
    async fn contract_get_pixel_returns_gif_and_logs_query_verbatim() {
        let (state, dir) = test_state();

        let query = "url=https%3A%2F%2Fexample.com%2Foffer%3Futm_source%3Dtaboola&type=pageview&sid=sess-42";
        let res = send(&state, get(&format!("/collect?{query}"))).await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/gif",
            "pixel response must be image/gif"
        );
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            &bytes[..],
            PIXEL_GIF,
            "pixel body must be the exact GIF bytes"
        );
        // Pin the constant itself to a real 1x1 GIF89a so the equality
        // above cannot pass on arbitrary bytes.
        assert!(PIXEL_GIF.starts_with(b"GIF89a"));
        assert_eq!(PIXEL_GIF.len(), 35, "1x1 transparent GIF is 35 bytes");

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        let raw = &logged[0];
        assert_eq!(raw.method, "GET");
        assert_eq!(raw.path, "/collect");
        assert_eq!(raw.query_params.as_deref(), Some(query));
        assert_eq!(raw.body, None, "pixels carry no body");
    }

    /// GET /p with no query string at all still succeeds — the pixel
    /// endpoint performs no parameter validation.
    #[tokio::test]
    async fn contract_get_pixel_with_no_query_still_succeeds() {
        let (state, dir) = test_state();

        let res = send(&state, get("/p")).await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/gif"
        );

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].path, "/p");
        assert_eq!(logged[0].query_params, None);
    }

    /// Malformed JSON is accepted with 204 and stored raw — parsing is a
    /// flusher concern; the collector is log-first.
    #[tokio::test]
    async fn contract_malformed_json_body_is_accepted_and_stored_raw() {
        let (state, dir) = test_state();

        let body = r#"{"type":"pageview","url":"https://example.com/  # truncated"#;
        let res = send(&state, post("/collect", body.to_string())).await;

        assert_eq!(
            res.status(),
            StatusCode::NO_CONTENT,
            "malformed payloads are stored, not rejected"
        );

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].body.as_deref(), Some(body));
    }

    /// An empty POST body is accepted and logged as an empty string.
    #[tokio::test]
    async fn contract_empty_post_body_is_accepted() {
        let (state, dir) = test_state();

        let res = send(&state, post("/collect", Body::empty())).await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].body.as_deref(), Some(""));
    }

    /// A body that is not valid UTF-8 is rejected with 400 and discarded —
    /// the recorded line format stores the body as a JSON string.
    #[tokio::test]
    async fn contract_invalid_utf8_body_is_rejected_400_and_not_logged() {
        let (state, dir) = test_state();

        let res = send(&state, post("/collect", vec![0xff, 0xfe, 0x00, 0x01])).await;

        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert!(
            logged.is_empty(),
            "rejected requests must never reach the log"
        );
    }

    /// Bodies over 2 MiB are rejected with 413 and discarded. The limit is
    /// inclusive: a body of exactly the limit is accepted.
    #[tokio::test]
    async fn contract_body_size_limit_is_2mib_inclusive() {
        let (state, dir) = test_state();

        let over = "x".repeat(MAX_EVENT_BODY_BYTES + 1);
        let res = send(&state, post("/collect", over)).await;
        assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            read_all_logged_requests(&state, &dir).await.is_empty(),
            "oversized requests must never reach the log"
        );

        let at = "x".repeat(MAX_EVENT_BODY_BYTES);
        let res = send(&state, post("/collect", at)).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].body.as_ref().unwrap().len(), MAX_EVENT_BODY_BYTES);
    }

    /// Methods an endpoint does not route are 405, and the request is
    /// never logged — rejected requests have no side effects.
    #[tokio::test]
    async fn contract_wrong_method_returns_405_and_is_not_logged() {
        let (state, dir) = test_state();

        for req in [
            post("/p", Body::empty()), // /p is GET-only
            get("/e"),                 // /e is POST-only
            Request::builder()
                .method(Method::PUT)
                .uri("/collect")
                .body(Body::empty())
                .unwrap(),
        ] {
            let res = send(&state, req).await;
            assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
        }

        assert!(
            read_all_logged_requests(&state, &dir).await.is_empty(),
            "rejected requests must never reach the log"
        );
    }

    /// Unknown paths are 404 (and unlogged).
    #[tokio::test]
    async fn contract_unknown_path_returns_404() {
        let (state, dir) = test_state();

        let res = send(&state, get("/definitely-not-an-endpoint")).await;

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert!(
            read_all_logged_requests(&state, &dir).await.is_empty(),
            "rejected requests must never reach the log"
        );
    }

    /// GET /health is the liveness probe: 200, body "OK", nothing logged.
    #[tokio::test]
    async fn contract_health_returns_200_ok() {
        let (state, dir) = test_state();

        let res = send(&state, get("/health")).await;

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], b"OK");
        assert!(
            read_all_logged_requests(&state, &dir).await.is_empty(),
            "health checks must not pollute the log"
        );
    }

    /// Retry semantics: ingestion is append-only and at-least-once. A
    /// client that retries an already-delivered event (sendBeacon replay,
    /// postback retry) produces one additional log line per delivery —
    /// there is no idempotency key and no dedupe at the collector.
    #[tokio::test]
    async fn contract_duplicate_sends_are_logged_once_each() {
        let (state, dir) = test_state();

        let body = r#"{"type":"conversion","conversion_type":"purchase","revenue":"19.99","sid":"sess-77"}"#;
        for _ in 0..3 {
            let res = send(&state, post("/c", body.to_string())).await;
            assert_eq!(res.status(), StatusCode::NO_CONTENT);
        }

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 3, "one line per delivery, no dedupe");
        assert!(logged.iter().all(|r| r.body.as_deref() == Some(body)));
    }

    /// Impression ingestion: GET /i returns the pixel GIF and logs the raw
    /// query string verbatim (imp_id and attribution params ride in the
    /// query — the flusher defaults the event to type=impression via the
    /// path, no explicit type needed).
    #[tokio::test]
    async fn contract_get_impression_pixel_returns_gif_and_logs_query_verbatim() {
        let (state, dir) = test_state();

        let query =
            "imp_id=pv-1%3Acreative-7&creative_id=creative-7&sid=sess-42&utm_source=taboola&utm_campaign=c-77";
        let res = send(&state, get(&format!("/i?{query}"))).await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/gif",
            "impression pixel response must be image/gif"
        );
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], PIXEL_GIF);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].method, "GET");
        assert_eq!(logged[0].path, "/i");
        assert_eq!(logged[0].query_params.as_deref(), Some(query));
        assert_eq!(logged[0].body, None);
    }

    /// Impression ingestion: POST /i accepts a form-encoded ad-server
    /// postback (or JSON) with 204 and stores the body verbatim for the
    /// flusher to parse.
    #[tokio::test]
    async fn contract_post_impression_postback_returns_204_and_logs_body() {
        let (state, dir) = test_state();

        let body = "imp_id=imp-11&sid=sess-9&uid=user-9&creative_id=creative-4&utm_source=mgid&utm_campaign=c-12";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/i")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = send(&state, req).await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].method, "POST");
        assert_eq!(logged[0].path, "/i");
        assert_eq!(logged[0].body.as_deref(), Some(body));
    }

    /// A bare GET /i with no query at all is still accepted — the flusher
    /// types it as an impression with no params, like /c before it.
    #[tokio::test]
    async fn contract_get_impression_pixel_with_no_query_still_succeeds() {
        let (state, dir) = test_state();

        let res = send(&state, get("/i")).await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/gif"
        );

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 1);
        assert_eq!(logged[0].path, "/i");
        assert_eq!(logged[0].query_params, None);
    }

    /// Client IP resolution: first hop of X-Forwarded-For, else X-Real-IP,
    /// else absent (the collector never parses the socket address).
    #[tokio::test]
    async fn contract_client_ip_xff_first_hop_then_x_real_ip_then_absent() {
        let (state, dir) = test_state();

        let req = Request::builder()
            .method(Method::POST)
            .uri("/e")
            .header("x-forwarded-for", "198.51.100.9, 203.0.113.2")
            .body(Body::from("{}".to_string()))
            .unwrap();
        send(&state, req).await;

        let req = Request::builder()
            .method(Method::POST)
            .uri("/e")
            .header("x-real-ip", "192.0.2.55")
            .body(Body::from("{}".to_string()))
            .unwrap();
        send(&state, req).await;

        send(&state, post("/e", "{}".to_string())).await;

        let logged = read_all_logged_requests(&state, &dir).await;
        assert_eq!(logged.len(), 3);
        assert_eq!(logged[0].client_ip.as_deref(), Some("198.51.100.9"));
        assert_eq!(logged[1].client_ip.as_deref(), Some("192.0.2.55"));
        assert_eq!(logged[2].client_ip, None);
    }
}
