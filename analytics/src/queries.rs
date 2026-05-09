use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::duckdb::DuckDBClient;
use crate::config::Config;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            category: ReportCategory::Asset,
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
            description: "First-touch attribution: credits initial acquisition source for conversions".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_first_touch.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_last_touch".to_string(),
            description: "Last-touch attribution: credits final touchpoint before conversion".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_last_touch.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "attribution_linear".to_string(),
            description: "Linear attribution: distributes credit equally across all touchpoints".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/attribution_linear.sql").to_string(),
            default_params: HashMap::new(),
            supports_iceberg: true,
        },
        Report {
            name: "session_reconstruction".to_string(),
            description: "Reconstruct sessions from events using gap-based sessionization".to_string(),
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
    ]
}

pub fn get_report(name: &str) -> Option<Report> {
    list_reports()
        .into_iter()
        .find(|r| r.name == name)
}

/// Get reports configured for daily scheduled execution
pub fn get_daily_reports() -> Vec<Report> {
    list_reports()
        .into_iter()
        .filter(|r| matches!(r.category, ReportCategory::Daily))
        .collect()
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

    // Replace date parameters
    if let Some(start) = &params.start_date {
        sql = sql.replace("{{start_date}}", start);
    } else {
        sql = sql.replace("{{start_date}}", "CURRENT_DATE - INTERVAL '30 days'");
    }

    if let Some(end) = &params.end_date {
        sql = sql.replace("{{end_date}}", end);
    } else {
        sql = sql.replace("{{end_date}}", "CURRENT_DATE");
    }

    // Legacy S3 path replacement for backward compatibility
    if let Some(s3_path) = &params.s3_path {
        sql = sql.replace("{{s3_path}}", s3_path);
    } else {
        sql = sql.replace("{{s3_path}}", "s3://my-trace-bucket/trace-events");
    }

    sql
}

/// Legacy template rendering (backward compatible)
/// This version uses the old {{s3_path}} style templates
pub fn render_template(template: &str, params: &ReportParams) -> String {
    let mut sql = template.to_string();

    if let Some(s3_path) = &params.s3_path {
        sql = sql.replace("{{s3_path}}", s3_path);
    } else {
        sql = sql.replace("{{s3_path}}", "s3://my-trace-bucket/trace-events");
    }

    if let Some(start) = &params.start_date {
        sql = sql.replace("{{start_date}}", start);
    } else {
        sql = sql.replace("{{start_date}}", "CURRENT_DATE - INTERVAL '30 days'");
    }

    if let Some(end) = &params.end_date {
        sql = sql.replace("{{end_date}}", end);
    } else {
        sql = sql.replace("{{end_date}}", "CURRENT_DATE");
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
        assert_eq!(report.category, ReportCategory::Metrics);
        assert!(report.supports_iceberg);
    }

    #[test]
    fn test_get_report_nonexistent() {
        let report = get_report("nonexistent_report");
        assert!(report.is_none());
    }

    #[test]
    fn test_render_template_basic() {
        let template = "SELECT * FROM '{{s3_path}}' WHERE ts >= '{{start_date}}' AND ts < '{{end_date}}'";
        let params = ReportParams {
            s3_path: Some("s3://my-bucket/events".to_string()),
            start_date: Some("2026-01-01".to_string()),
            end_date: Some("2026-01-31".to_string()),
        };

        let rendered = render_template(template, &params);
        assert!(rendered.contains("s3://my-bucket/events"));
        assert!(rendered.contains("2026-01-01"));
        assert!(rendered.contains("2026-01-31"));
    }

    #[test]
    fn test_render_template_defaults() {
        let template = "SELECT * FROM '{{s3_path}}' WHERE ts >= '{{start_date}}'";
        let params = ReportParams::default();

        let rendered = render_template(template, &params);
        assert!(rendered.contains("s3://my-trace-bucket/trace-events"));
        assert!(rendered.contains("CURRENT_DATE - INTERVAL '30 days'"));
    }

    #[test]
    fn test_report_categories() {
        let reports = list_reports();

        // Check that reports have the expected categories
        let daily_summary = get_report("daily_summary").unwrap();
        assert!(matches!(daily_summary.category, ReportCategory::Metrics));

        let ctr_report = get_report("ctr_by_campaign").unwrap();
        assert!(matches!(ctr_report.category, ReportCategory::Campaign));

        let top_headlines = get_report("top_headlines").unwrap();
        assert!(matches!(top_headlines.category, ReportCategory::Asset));

        let network_comparison = get_report("network_comparison").unwrap();
        assert!(matches!(network_comparison.category, ReportCategory::Network));

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
