use anyhow::Result;
use axum::{
    extract::{Query as QueryParams, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::config::Config;
use crate::duckdb::DuckDBClient;
use crate::queries::{list_reports, render_template_with_client, ReportParams};

#[derive(Clone)]
struct AppState {
    config: Config,
    db: Arc<Mutex<Option<DuckDBClient>>>,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct ReportRequest {
    name: String,
    #[serde(default)]
    format: String,
    #[serde(default)]
    start_date: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    error: Option<String>,
}

#[derive(Serialize)]
struct ReportListResponse {
    reports: Vec<ReportInfo>,
}

#[derive(Serialize)]
struct ReportInfo {
    name: String,
    description: String,
    category: String,
    params: HashMap<String, String>,
}

pub async fn run_server(config: Config, host: &str, port: u16) -> Result<()> {
    let state = AppState {
        config,
        db: Arc::new(Mutex::new(None)),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/query", get(execute_query))
        .route("/api/reports", get(list_reports_api))
        .route("/api/reports/run", get(run_report_api))
        .route("/api/health", get(health_check))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port)).await?;
    info!("Web UI listening on http://{}:{}", host, port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn execute_query(
    State(state): State<AppState>,
    QueryParams(params): QueryParams<QueryRequest>,
) -> impl IntoResponse {
    let mut db_guard = state.db.lock().await;
    if db_guard.is_none() {
        match DuckDBClient::new(&state.config) {
            Ok(client) => *db_guard = Some(client),
            Err(e) => {
                error!("Failed to create DuckDB client: {}", e);
                return Json(QueryResponse {
                    columns: vec![],
                    rows: vec![],
                    error: Some(format!("Database connection failed: {}", e)),
                });
            }
        }
    }

    let db = db_guard.as_ref().unwrap();

    match db.execute_query(&params.sql) {
        Ok(result) => Json(QueryResponse {
            columns: result.columns,
            rows: result.rows,
            error: None,
        }),
        Err(e) => Json(QueryResponse {
            columns: vec![],
            rows: vec![],
            error: Some(format!("Query failed: {}", e)),
        }),
    }
}

async fn list_reports_api() -> Json<ReportListResponse> {
    let reports = list_reports();
    let report_info: Vec<ReportInfo> = reports
        .into_iter()
        .map(|r| ReportInfo {
            name: r.name,
            description: r.description,
            category: format!("{:?}", r.category),
            params: r.default_params,
        })
        .collect();

    Json(ReportListResponse { reports: report_info })
}

async fn run_report_api(
    State(state): State<AppState>,
    QueryParams(params): QueryParams<ReportRequest>,
) -> impl IntoResponse {
    let mut db_guard = state.db.lock().await;
    if db_guard.is_none() {
        match DuckDBClient::new(&state.config) {
            Ok(client) => *db_guard = Some(client),
            Err(e) => {
                error!("Failed to create DuckDB client: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(QueryResponse {
                        columns: vec![],
                        rows: vec![],
                        error: Some(format!("Database connection failed: {}", e)),
                    }),
                );
            }
        }
    }

    let db = db_guard.as_ref().unwrap();

    let report = match crate::queries::get_report(&params.name) {
        Some(r) => r,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(QueryResponse {
                    columns: vec![],
                    rows: vec![],
                    error: Some(format!("Report '{}' not found", params.name)),
                }),
            );
        }
    };

    let report_params = ReportParams {
        start_date: params.start_date,
        end_date: params.end_date,
        ..Default::default()
    };

    let sql = render_template_with_client(&report.sql_template, &report_params, db, &state.config);

    match db.execute_query(&sql) {
        Ok(result) => {
            let format = if params.format == "csv" { "csv" } else { "json" };
            let output_data = match format {
                "csv" => result.to_csv(),
                _ => result.to_json(),
            };
            (StatusCode::OK, output_data)
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(QueryResponse {
                columns: vec![],
                rows: vec![],
                error: Some(format!("Report execution failed: {}", e)),
            }),
        ),
    }
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339(),
    }))
}
