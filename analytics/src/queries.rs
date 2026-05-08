use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub name: String,
    pub description: String,
    pub category: ReportCategory,
    pub sql_template: String,
    pub default_params: HashMap<String, String>,
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
}

pub fn list_reports() -> Vec<Report> {
    vec![
        Report {
            name: "daily_summary".to_string(),
            description: "Daily event summary by type and source".to_string(),
            category: ReportCategory::Metrics,
            sql_template: include_str!("../queries/daily_summary.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "ctr_by_campaign".to_string(),
            description: "Click-through rate by campaign".to_string(),
            category: ReportCategory::Campaign,
            sql_template: include_str!("../queries/ctr_by_campaign.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "campaign_funnel".to_string(),
            description: "Conversion funnel by campaign".to_string(),
            category: ReportCategory::Campaign,
            sql_template: include_str!("../queries/campaign_funnel.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "top_headlines".to_string(),
            description: "Top performing headlines".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/top_headlines.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "top_images".to_string(),
            description: "Top performing images".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/top_images.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "creative_combinations".to_string(),
            description: "Best headline + image combinations".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/creative_combinations.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "network_comparison".to_string(),
            description: "Compare performance across ad networks".to_string(),
            category: ReportCategory::Network,
            sql_template: include_str!("../queries/network_comparison.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "cross_network_creatives".to_string(),
            description: "Find creatives running on multiple networks".to_string(),
            category: ReportCategory::Network,
            sql_template: include_str!("../queries/cross_network_creatives.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "trending_campaigns".to_string(),
            description: "Campaigns with increasing momentum".to_string(),
            category: ReportCategory::Time,
            sql_template: include_str!("../queries/trending_campaigns.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "creative_fatigue".to_string(),
            description: "Detect declining creative performance".to_string(),
            category: ReportCategory::Asset,
            sql_template: include_str!("../queries/creative_fatigue.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "session_flow".to_string(),
            description: "Common page sequences within sessions".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/session_flow.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "landing_page_performance".to_string(),
            description: "Top landing pages and bounce rate".to_string(),
            category: ReportCategory::Journey,
            sql_template: include_str!("../queries/landing_page_performance.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "traffic_spike_detection".to_string(),
            description: "Detect unusual traffic spikes".to_string(),
            category: ReportCategory::Alert,
            sql_template: include_str!("../queries/traffic_spike_detection.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "zero_traffic_alert".to_string(),
            description: "Find campaigns with no recent traffic".to_string(),
            category: ReportCategory::Alert,
            sql_template: include_str!("../queries/zero_traffic_alert.sql").to_string(),
            default_params: HashMap::new(),
        },
        Report {
            name: "hourly_traffic_pattern".to_string(),
            description: "Traffic by hour of day".to_string(),
            category: ReportCategory::Time,
            sql_template: include_str!("../queries/hourly_traffic_pattern.sql").to_string(),
            default_params: HashMap::new(),
        },
    ]
}

pub fn get_report(name: &str) -> Option<Report> {
    list_reports()
        .into_iter()
        .find(|r| r.name == name)
}

pub fn render_template(template: &str, params: &ReportParams) -> String {
    let mut sql = template.to_string();

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

#[derive(Debug, Clone, Default)]
pub struct ReportParams {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}
