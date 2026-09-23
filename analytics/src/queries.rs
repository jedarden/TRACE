use crate::config::Config;
use crate::duckdb::DuckDBClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub name: String,
    pub description: String,
    pub category: ReportCategory,
    pub sql_template: String,
    pub default_params: HashMap<String, String>,
    /// Whether this report supports Iceberg tables
    pub supports_iceberg: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReportCategory {
    #[serde(rename = "metrics")]
    Metrics,
    #[serde(rename = "campaign")]
    Campaign,
    #[serde(rename = "asset")]
    Asset,
    #[serde(rename = "network")]
    Network,
    #[serde(rename = "time")]
    Time,
    #[serde(rename = "journey")]
    Journey,
    #[serde(rename = "alert")]
    Alert,
    #[serde(rename = "daily")]
    Daily,
}

pub fn list_reports() -> Vec<Report> {
    vec![
        Report {
            name: "daily_summary".to_string(),
            description: "Daily event summary by type and source".to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/daily_summary.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "daily_sessions".to_string(),
            description: "Daily session summary from the materialized trace.sessions table"
                .to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/daily_sessions.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "ctr_by_campaign".to_string(),
            description: "Click-through rate by campaign".to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/ctr_by_campaign.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "campaign_funnel".to_string(),
            description: "Conversion funnel by campaign".to_string(),
            category: ReportCategory::Campaign,
            sql_template: include_str!("../queries/campaign_funnel.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "top_headlines".to_string(),
            description: "Top performing headlines".to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/top_headlines.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "top_images".to_string(),
            description: "Top performing images".to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/top_images.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "creative_combinations".to_string(),
            description: "Best headline + image combinations".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/creative_combinations.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "asset_performance".to_string(),
            description: "Per-asset performance: assets dimension joined to ad events".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/asset_performance.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "network_comparison".to_string(),
            description: "Compare performance across ad networks".to_string(),
            category: ReportCategory::Network,
            sql_template: include_str!("../queries/network_comparison.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "cross_network_creatives".to_string(),
            description: "Find creatives running on multiple networks".to_string(),
            category: ReportCategory::Network,
            sql_template: include_str!("../queries/cross_network_creatives.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "trending_campaigns".to_string(),
            description: "Campaigns with increasing momentum".to_string(),
            category: ReportCategory::Time,
            sql_template: include_str!("../queries/trending_campaigns.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "creative_fatigue".to_string(),
            description: "Detect declining creative performance".to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/creative_fatigue.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "session_flow".to_string(),
            description: "Common page sequences within sessions".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/session_flow.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "landing_page_performance".to_string(),
            description: "Top landing pages and bounce rate".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/landing_page_performance.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "traffic_spike_detection".to_string(),
            description: "Detect unusual traffic spikes".to_string(),
            category: ReportCategory::Alert,
            sql_template: include_str!("../queries/traffic_spike_detection.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "zero_traffic_alert".to_string(),
            description: "Find campaigns with no recent traffic".to_string(),
            category: ReportCategory::Alert,
            sql_template: include_str!("../queries/zero_traffic_alert.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "hourly_traffic_pattern".to_string(),
            description: "Traffic by hour of day".to_string(),
            category: ReportCategory::Time,
            sql_template: include_str!("../queries/hourly_traffic_pattern.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_first_touch".to_string(),
            description:
                "First-touch attribution: credits initial acquisition source for conversions"
                    .to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_first_touch.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_last_touch".to_string(),
            description: "Last-touch attribution: credits final touchpoint before conversion"
                .to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_last_touch.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_linear".to_string(),
            description: "Linear attribution: distributes credit equally across all touchpoints"
                .to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_linear.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "session_reconstruction".to_string(),
            description: "Reconstruct sessions from events using gap-based sessionization"
                .to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/session_reconstruction.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "user_journey".to_string(),
            description: "Reconstruct complete user journey across all sessions".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/user_journey.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_analysis".to_string(),
            description: "Multi-touch attribution analysis for conversion tracking".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_analysis.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "common_paths".to_string(),
            description: "Most common user paths through the site".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/common_paths.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "session_flow_matrix".to_string(),
            description: "Session flow transition matrix for visualization".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/session_flow_matrix.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "cohort_journey".to_string(),
            description: "User journey by acquisition cohort analysis".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/cohort_journey.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "funnel_with_paths".to_string(),
            description: "Funnel analysis with user journey paths".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/funnel_with_paths.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "drop_off_analysis".to_string(),
            description: "Analyze where users drop off in their journey".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/drop_off_analysis.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "returning_user_analysis".to_string(),
            description: "Analyze returning user behavior by segment".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/returning_user_analysis.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "impression_performance".to_string(),
            description: "Impression volume, unique impressions, CTR, and viewability by campaign"
                .to_string(),
            category: ReportCategory::Daily,
            sql_template: include_str!("../queries/impression_performance.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
    ]
}

pub fn get_report(name: &str) -> Option<Report> {
    list_reports().into_iter().find(|r| r.name == name)
}

/// Get reports configured for daily scheduled execution
pub fn get_daily_reports() -> Vec<Report> {
    list_reports()
        .into_iter()
        .filter(|r| matches!(r.category, ReportCategory::Daily))
        .collect()
}

/// Build the day-partition predicate that accompanies a `ts`/`started_at`
/// range filter so DuckDB's Parquet read path can prune whole directories.
///
/// - `Some(column)`: the backend reads Hive-partitioned directories, and
///   filtering the partition column is the only thing that skips files —
///   returns a date range using either literal dates or the default rolling
///   SQL expressions.
/// - `None`: the backend reads a true Iceberg table with hidden `day()`
///   partitioning; the timestamp range predicate already in the template is
///   what prunes, so this renders as TRUE to keep the SQL valid.
///
/// The dates use the same string form the `{{start_date}}`/`{{end_date}}`
/// substitutions splice into the templates (YYYY-MM-DD from the CLI and the
/// daily runner).
pub fn partition_predicate(column: Option<&str>, start: &str, end: &str) -> String {
    match column {
        Some(col) => format!(
            "({} >= {} AND {} < {})",
            col,
            partition_date_operand(start),
            col,
            partition_date_operand(end)
        ),
        None => "TRUE".to_string(),
    }
}

/// Render either a user-supplied ISO date or one of the SQL expressions used
/// by the default rolling window. Report templates splice both forms into
/// the same partition predicate.
fn partition_date_operand(value: &str) -> String {
    if value == "CURRENT_DATE" || value.starts_with("CURRENT_DATE ") {
        format!("CAST({} AS DATE)", value)
    } else {
        format!("'{}'::DATE", value)
    }
}

/// Lower-bound-only variant of [`partition_predicate`] for templates with a
/// fixed rolling window (`ts >= CURRENT_DATE - INTERVAL 'N days'`): the bound
/// is a SQL expression rather than a spliced date string. Future-dated
/// partitions do not need an upper bound.
pub fn partition_lower_bound_predicate(column: Option<&str>, bound_expr: &str) -> String {
    match column {
        Some(col) => format!("({} >= CAST({} AS DATE))", col, bound_expr),
        None => "TRUE".to_string(),
    }
}

/// Replace every `{{prefix:expr}}` occurrence, passing `expr` (trimmed) to
/// `render`. Hand-rolled rather than a regex dependency: the delimiters are
/// fixed and expressions never contain `}}`. `prefix` is the literal
/// opening `{{name:` — not a pattern; `find` locates it verbatim.
fn regex_replace_all<F: Fn(&str) -> String>(sql: &str, prefix: &str, render: F) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find(prefix) {
        let expr_start = start + prefix.len();
        if let Some(end_rel) = rest[expr_start..].find("}}") {
            out.push_str(&rest[..start]);
            out.push_str(&render(rest[expr_start..expr_start + end_rel].trim()));
            rest = &rest[expr_start + end_rel + 2..];
        } else {
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Render template with Iceberg/Parquet-aware SQL substitution
/// This version uses DuckDBClient to determine the correct table references
pub fn render_template_with_client(
    template: &str,
    params: &ReportParams,
    db: &DuckDBClient,
    config: &Config,
) -> String {
    let mut sql = template.to_string();

    // Replace table references with appropriate Iceberg or Parquet views
    let events_table = db.events_table_sql(config);
    sql = sql.replace("{{events_table}}", &events_table);

    let assets_table = db.assets_table_sql(config);
    sql = sql.replace("{{assets_table}}", &assets_table);

    let sessions_table = db.sessions_table_sql(config);
    sql = sql.replace("{{sessions_table}}", &sessions_table);

    // Replace date parameters, keeping the partition filters in sync with
    // the timestamp ranges they mirror
    let start = params
        .start_date
        .clone()
        .unwrap_or_else(|| "CURRENT_DATE - INTERVAL '30 days'".to_string());
    let end = params
        .end_date
        .clone()
        .unwrap_or_else(|| "CURRENT_DATE".to_string());

    sql = sql.replace(
        "{{ts_partition_filter}}",
        &partition_predicate(db.events_partition_column(config), &start, &end),
    );
    sql = sql.replace(
        "{{sessions_partition_filter}}",
        &partition_predicate(db.sessions_partition_column(config), &start, &end),
    );

    // Expression-bound form for templates with fixed rolling windows:
    // {{ts_partition_filter:CURRENT_DATE - INTERVAL '7 days'}} filters the
    // partition column from that expression onward (TRUE under Iceberg, like
    // the plain form)
    let events_col = db.events_partition_column(config);
    sql = regex_replace_all(&sql, "{{ts_partition_filter:", |expr| {
        partition_lower_bound_predicate(events_col, expr)
    });
    let sessions_col = db.sessions_partition_column(config);
    sql = regex_replace_all(&sql, "{{sessions_partition_filter:", |expr| {
        partition_lower_bound_predicate(sessions_col, expr)
    });

    sql = sql.replace("{{start_date}}", &start);
    sql = sql.replace("{{end_date}}", &end);

    // Legacy S3 path replacement for backward compatibility
    if let Some(s3_path) = &params.s3_path {
        sql = sql.replace("{{s3_path}}", s3_path);
    } else {
        sql = sql.replace("{{s3_path}}", "s3://my-trace-bucket/trace-events");
    }

    sql
}

#[derive(Debug, Clone)]
pub struct ReportParams {
    pub s3_path: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

impl Default for ReportParams {
    fn default() -> Self {
        Self {
            s3_path: None,
            start_date: None,
            end_date: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_reports_not_empty() {
        let reports = list_reports();
        assert!(!reports.is_empty());
        assert!(reports.len() > 20);
    }

    #[test]
    fn test_get_report_existing() {
        let report = get_report("daily_summary");
        assert!(report.is_some());
        let report = report.unwrap();
        assert_eq!(report.name, "daily_summary");
        assert_eq!(report.category, ReportCategory::Daily);
        assert!(report.supports_iceberg);
    }

    #[test]
    fn test_get_report_nonexistent() {
        let report = get_report("nonexistent_report");
        assert!(report.is_none());
    }

    #[test]
    fn test_impression_report_is_registered() {
        let report = get_report("impression_performance").expect("impression report");
        assert!(matches!(report.category, ReportCategory::Daily));
        assert!(report.sql_template.contains("type = 'impression'"));
        assert!(report.sql_template.contains("unique_impressions"));
    }

    #[test]
    fn test_partition_predicate_accepts_default_rolling_window() {
        assert_eq!(
            partition_predicate(
                Some("dt"),
                "CURRENT_DATE - INTERVAL '30 days'",
                "CURRENT_DATE"
            ),
            "(dt >= CAST(CURRENT_DATE - INTERVAL '30 days' AS DATE) AND dt < CAST(CURRENT_DATE AS DATE))"
        );
    }

    #[test]
    fn test_report_categories() {
        let daily_summary = get_report("daily_summary").unwrap();
        assert!(matches!(daily_summary.category, ReportCategory::Daily));

        let ctr_report = get_report("ctr_by_campaign").unwrap();
        assert!(matches!(ctr_report.category, ReportCategory::Daily));

        let top_headlines = get_report("top_headlines").unwrap();
        assert!(matches!(top_headlines.category, ReportCategory::Daily));

        let network_comparison = get_report("network_comparison").unwrap();
        assert!(matches!(
            network_comparison.category,
            ReportCategory::Network
        ));

        let trending = get_report("trending_campaigns").unwrap();
        assert!(matches!(trending.category, ReportCategory::Time));

        let session_flow = get_report("session_flow").unwrap();
        assert!(matches!(session_flow.category, ReportCategory::Journey));

        let traffic_alert = get_report("traffic_spike_detection").unwrap();
        assert!(matches!(traffic_alert.category, ReportCategory::Alert));
    }

    #[test]
    fn test_all_reports_support_iceberg() {
        let reports = list_reports();
        for report in reports {
            assert!(
                report.supports_iceberg,
                "Report {} does not support Iceberg",
                report.name
            );
        }
    }
}
