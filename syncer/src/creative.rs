//! Creative metadata types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Metadata for a creative from an ad network
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreativeMetadata {
    /// Ad network name (taboola, outbrain, mgid, revcontent)
    pub network: String,

    /// Campaign ID from the ad network
    pub campaign_id: Option<String>,

    /// Campaign name (optional)
    pub campaign_name: Option<String>,

    /// Creative/asset identifier
    pub creative_id: Option<String>,

    /// Headline or title text
    pub headline: Option<String>,

    /// Image URL
    pub image_url: Option<String>,

    /// Landing page URL
    pub landing_page_url: Option<String>,

    /// Item identifier (where available)
    pub item_id: Option<String>,

    /// When this metadata was synced from the API
    pub synced_at: DateTime<Utc>,
}

/// Performance metrics for campaigns and creatives
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerformanceMetrics {
    /// Ad network name (taboola, outbrain, mgid, revcontent)
    pub network: String,

    /// Campaign ID from the ad network
    pub campaign_id: String,

    /// Campaign name (optional)
    pub campaign_name: Option<String>,

    /// Creative/asset identifier (optional - for creative-level metrics)
    pub creative_id: Option<String>,

    /// Date for these metrics
    pub date: chrono::NaiveDate,

    /// Number of impressions
    pub impressions: i64,

    /// Number of clicks
    pub clicks: i64,

    /// Cost/spend in microcurrency (e.g., microdollars)
    pub spend_micros: i64,

    /// Number of conversions (if available)
    pub conversions: Option<i64>,

    /// Click-through rate (calculated, in basis points: 10000 = 100%)
    pub_ctr_bps: Option<i32>,

    /// Cost per click in microcurrency
    pub cpc_micros: Option<i64>,

    /// Cost per thousand impressions in microcurrency
    pub cpm_micros: Option<i64>,

    /// When these metrics were synced from the API
    pub synced_at: DateTime<Utc>,
}

impl PerformanceMetrics {
    /// Calculate CTR in basis points (10000 = 100%)
    pub fn calculate_ctr_bps(impressions: i64, clicks: i64) -> Option<i32> {
        if impressions > 0 {
            Some((clicks * 10000 / impressions) as i32)
        } else {
            None
        }
    }

    /// Calculate CPC in microcurrency
    pub fn calculate_cpc_micros(spend_micros: i64, clicks: i64) -> Option<i64> {
        if clicks > 0 {
            Some(spend_micros / clicks)
        } else {
            None
        }
    }

    /// Calculate CPM in microcurrency
    pub fn calculate_cpm_micros(spend_micros: i64, impressions: i64) -> Option<i64> {
        if impressions > 0 {
            Some(spend_micros * 1000 / impressions)
        } else {
            None
        }
    }

    /// Create a new PerformanceMetrics with calculated derived metrics
    pub fn new(
        network: String,
        campaign_id: String,
        campaign_name: Option<String>,
        creative_id: Option<String>,
        date: chrono::NaiveDate,
        impressions: i64,
        clicks: i64,
        spend_micros: i64,
        conversions: Option<i64>,
        synced_at: DateTime<Utc>,
    ) -> Self {
        let ctr_bps = Self::calculate_ctr_bps(impressions, clicks);
        let cpc_micros = Self::calculate_cpc_micros(spend_micros, clicks);
        let cpm_micros = Self::calculate_cpm_micros(spend_micros, impressions);

        Self {
            network,
            campaign_id,
            campaign_name,
            creative_id,
            date,
            impressions,
            clicks,
            spend_micros,
            conversions,
            ctr_bps,
            cpc_micros,
            cpm_micros,
            synced_at,
        }
    }

    /// Generate a unique key for this metric record
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.network,
            self.date,
            self.campaign_id,
            self.creative_id.as_deref().unwrap_or("aggregate")
        )
    }
}

impl CreativeMetadata {
    /// Generate a unique key for this creative
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.network,
            self.campaign_id.as_deref().unwrap_or(""),
            self.creative_id.as_deref().unwrap_or("")
        )
    }

    /// Check if this creative has complete metadata
    pub fn is_complete(&self) -> bool {
        self.campaign_id.is_some()
            && self.creative_id.is_some()
            && (self.headline.is_some() || self.image_url.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creative_key() {
        let creative = CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: None,
            creative_id: Some("creative456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: None,
            item_id: None,
            synced_at: Utc::now(),
        };

        assert_eq!(creative.key(), "taboola:camp123:creative456");
    }

    #[test]
    fn test_creative_is_complete() {
        let mut creative = CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: None,
            creative_id: Some("creative456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: None,
            item_id: None,
            synced_at: Utc::now(),
        };

        assert!(creative.is_complete());

        // Missing campaign_id
        creative.campaign_id = None;
        assert!(!creative.is_complete());

        // Missing creative_id
        creative.campaign_id = Some("camp123".to_string());
        creative.creative_id = None;
        assert!(!creative.is_complete());

        // Missing both headline and image_url
        creative.creative_id = Some("creative456".to_string());
        creative.headline = None;
        creative.image_url = None;
        assert!(!creative.is_complete());
    }

    #[test]
    fn test_calculate_ctr_bps() {
        assert_eq!(PerformanceMetrics::calculate_ctr_bps(1000, 50), Some(500)); // 5%
        assert_eq!(PerformanceMetrics::calculate_ctr_bps(100, 10), Some(1000)); // 10%
        assert_eq!(PerformanceMetrics::calculate_ctr_bps(0, 10), None); // No impressions
    }

    #[test]
    fn test_calculate_cpc_micros() {
        assert_eq!(PerformanceMetrics::calculate_cpc_micros(1000000, 100), Some(10000)); // $0.01
        assert_eq!(PerformanceMetrics::calculate_cpc_micros(500000, 50), Some(10000)); // $0.01
        assert_eq!(PerformanceMetrics::calculate_cpc_micros(1000000, 0), None); // No clicks
    }

    #[test]
    fn test_calculate_cpm_micros() {
        assert_eq!(PerformanceMetrics::calculate_cpm_micros(1000000, 1000), Some(1000000)); // $1.00
        assert_eq!(PerformanceMetrics::calculate_cpm_micros(500000, 500), Some(1000000)); // $1.00
        assert_eq!(PerformanceMetrics::calculate_cpm_micros(1000000, 0), None); // No impressions
    }

    #[test]
    fn test_performance_metrics_key() {
        let metrics = PerformanceMetrics::new(
            "taboola".to_string(),
            "camp123".to_string(),
            Some("Test Campaign".to_string()),
            Some("creative456".to_string()),
            chrono::NaiveDate::from_ymd_opt(2026, 5, 8).unwrap(),
            1000,
            50,
            1000000,
            Some(5),
            Utc::now(),
        );

        assert_eq!(metrics.key(), "taboola:2026-05-08:camp123:creative456");
    }

    #[test]
    fn test_performance_metrics_aggregate_key() {
        let metrics = PerformanceMetrics::new(
            "taboola".to_string(),
            "camp123".to_string(),
            Some("Test Campaign".to_string()),
            None, // No creative_id - aggregate level
            chrono::NaiveDate::from_ymd_opt(2026, 5, 8).unwrap(),
            1000,
            50,
            1000000,
            Some(5),
            Utc::now(),
        );

        assert_eq!(metrics.key(), "taboola:2026-05-08:camp123:aggregate");
    }

    #[test]
    fn test_performance_metrics_derived_fields() {
        let metrics = PerformanceMetrics::new(
            "taboola".to_string(),
            "camp123".to_string(),
            Some("Test Campaign".to_string()),
            Some("creative456".to_string()),
            chrono::NaiveDate::from_ymd_opt(2026, 5, 8).unwrap(),
            1000,   // impressions
            50,     // clicks
            1000000, // $1.00 in microdollars
            Some(5), // conversions
            Utc::now(),
        );

        assert_eq!(metrics.impressions, 1000);
        assert_eq!(metrics.clicks, 50);
        assert_eq!(metrics.spend_micros, 1000000);
        assert_eq!(metrics.conversions, Some(5));
        assert_eq!(metrics.ctr_bps, Some(500)); // 5% in basis points
        assert_eq!(metrics.cpc_micros, Some(20000)); // $0.02 in microdollars
        assert_eq!(metrics.cpm_micros, Some(1000000)); // $1.00 in microdollars
    }
}
