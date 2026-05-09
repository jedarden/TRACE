mod config;
mod duckdb;
mod queries;
mod reporter;
mod s3;
mod session_stitcher;
mod web;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "trace-analytics")]
#[command(about = "DuckDB analytics and report runner for TRACE", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a specific report by name
    Run {
        /// Report name (e.g., daily_summary, ctr_by_campaign, creative_fatigue)
        name: String,
        /// Output format (json, csv, both)
        #[arg(short, long, default_value = "json")]
        format: String,
        /// Output file path (defaults to stdout)
        #[arg(short, long)]
        output: Option<String>,
        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start_date: Option<String>,
        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end_date: Option<String>,
    },
    /// List all available reports
    List,
    /// Run scheduled reports (daemon mode)
    Schedule {
        /// Schedule interval in seconds
        #[arg(short, long, default_value = "86400")]
        interval: u64,
    },
    /// Run daily reports once (for testing or manual execution)
    Daily {
        /// Output format (json, csv, both)
        #[arg(short, long, default_value = "both")]
        format: String,
    },
    /// Execute a raw SQL query
    Query {
        /// SQL query file
        query_file: String,
        /// Output format (json, csv)
        #[arg(short, long, default_value = "json")]
        format: String,
        /// Output file path (defaults to stdout)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Start web UI for ad-hoc queries
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,
        /// Host to bind to
        #[arg(short, long, default_value = "0.0.0.0")]
        host: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_analytics=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            name,
            format,
            output,
            start_date,
            end_date,
        } => {
            let config = config::Config::from_env()?;
            let db = duckdb::DuckDBClient::new(&config)?;

            let params = crate::queries::ReportParams {
                start_date,
                end_date,
                ..Default::default()
            };

            // Handle "both" format
            if format == "both" {
                if output.is_none() {
                    error!("Output path must be specified when using 'both' format");
                    std::process::exit(1);
                }
                let base_path = output.as_ref().unwrap();
                let json_path = format!("{}.json", base_path);
                let csv_path = format!("{}.csv", base_path);

                if let Err(e) = reporter::run_report(&db, &name, "json", Some(&json_path), &params, &config).await {
                    error!("Report execution (JSON) failed: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = reporter::run_report(&db, &name, "csv", Some(&csv_path), &params, &config).await {
                    error!("Report execution (CSV) failed: {}", e);
                    std::process::exit(1);
                }
                info!("Reports saved: {} and {}", json_path, csv_path);
            } else {
                if let Err(e) = reporter::run_report(&db, &name, &format, output.as_deref(), &params, &config).await {
                    error!("Report execution failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::List => {
            let reports = queries::list_reports();
            println!("Available reports:");
            for report in reports {
                println!("  - {}: {}", report.name, report.description);
            }
        }
        Commands::Schedule { interval } => {
            let config = config::Config::from_env()?;
            reporter::run_scheduled_reports(config, interval).await?;
        }
        Commands::Daily { .. } => {
            let config = config::Config::from_env()?;
            if let Err(e) = reporter::run_daily_reports(&config).await {
                error!("Daily reports execution failed: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Query {
            query_file,
            format,
            output,
        } => {
            let config = config::Config::from_env()?;
            let db = duckdb::DuckDBClient::new(&config)?;

            let sql = std::fs::read_to_string(&query_file)?;
            if let Err(e) = reporter::execute_query(&db, &sql, &format, output.as_deref()) {
                error!("Query execution failed: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Serve { port, host } => {
            let config = config::Config::from_env()?;
            info!("Starting web UI on {}:{}", host, port);
            if let Err(e) = web::run_server(config, &host, port).await {
                error!("Web server failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
