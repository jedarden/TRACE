mod config;
mod duckdb;
mod queries;
mod reporter;
mod s3;
mod session_stitcher;

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
        /// Output format (json, csv)
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

            let params = reporter::ReportParams {
                start_date,
                end_date,
            };

            if let Err(e) = reporter::run_report(&db, &name, &format, output.as_deref(), &params, &config) {
                error!("Report execution failed: {}", e);
                std::process::exit(1);
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
    }

    Ok(())
}
