use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{error, info};

use crate::config::Config;
use crate::duckdb::DuckDBClient;
use crate::queries::{get_report, render_template, ReportParams};

pub async fn run_report(
    db: &DuckDBClient,
    name: &str,
    format: &str,
    output: Option<&str>,
    params: &ReportParams,
) -> Result<()> {
    let report = get_report(name)
        .ok_or_else(|| anyhow::anyhow!("Report '{}' not found", name))?;

    info!("Running report: {}", name);

    let sql = render_template(&report.sql_template, params);
    let result = db.execute_query(&sql)?;

    let output_data = match format {
        "csv" => result.to_csv(),
        "json" | _ => result.to_json(),
    };

    match output {
        Some(path) => {
            tokio::fs::write(path, output_data).await?;
            info!("Report saved to: {}", path);
        }
        None => {
            println!("{}", output_data);
        }
    }

    Ok(())
}

pub fn execute_query(
    db: &DuckDBClient,
    sql: &str,
    format: &str,
    output: Option<&str>,
) -> Result<()> {
    let result = db.execute_query(sql)?;

    let output_data = match format {
        "csv" => result.to_csv(),
        "json" | _ => result.to_json(),
    };

    match output {
        Some(path) => {
            std::fs::write(path, output_data)?;
            info!("Query results saved to: {}", path);
        }
        None => {
            println!("{}", output_data);
        }
    }

    Ok(())
}

pub async fn run_scheduled_reports(config: Config, interval_secs: u64) -> Result<()> {
    use tokio::time::{interval, Duration};

    info!("Starting scheduled reports runner (interval: {}s)", interval_secs);

    let mut timer = interval(Duration::from_secs(interval_secs));

    loop {
        timer.tick().await;

        let now: DateTime<Utc> = Utc::now();
        info!("Running scheduled reports at {}", now);

        if let Err(e) = run_daily_reports(&config).await {
            error!("Scheduled reports failed: {}", e);
        }
    }
}

async fn run_daily_reports(config: &Config) -> Result<()> {
    let db = DuckDBClient::new(config)?;

    let params = ReportParams {
        start_date: Some((Utc::now() - chrono::Days::new(7)).format("%Y-%m-%d").to_string()),
        end_date: Some(Utc::now().format("%Y-%m-%d").to_string()),
    };

    let reports = vec!["daily_summary", "ctr_by_campaign", "top_headlines", "network_comparison"];

    for report_name in reports {
        info!("Running scheduled report: {}", report_name);

        if let Err(e) = run_report(&db, report_name, "json", None, &params) {
            error!("Report '{}' failed: {}", report_name, e);
        }
    }

    Ok(())
}
