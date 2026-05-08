//! Cross-network campaign normalization
//!
//! Different ad networks use different parameter names for the same concepts.
//! This module normalizes them into a common schema for unified analysis.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Normalized campaign data across all ad networks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedCampaign {
    /// Detected ad network (taboola, outbrain, mgid, revcontent, unknown)
    pub network: String,
    /// Network's campaign identifier
    pub campaign_id: Option<String>,
    /// Creative/asset identifier (image ID, creative ID, etc.)
    pub creative_id: Option<String>,
    /// Headline or title text
    pub headline: Option<String>,
    /// Image identifier or thumbnail URL
    pub image_id: Option<String>,
    /// Item identifier (where available)
    pub item_id: Option<String>,
}

impl NormalizedCampaign {
    /// Create a new empty normalized campaign with the given network
    fn new(network: &str) -> Self {
        Self {
            network: network.to_string(),
            campaign_id: None,
            creative_id: None,
            headline: None,
            image_id: None,
            item_id: None,
        }
    }

    /// Builder-style setter for campaign_id
    fn with_campaign_id(mut self, value: Option<String>) -> Self {
        self.campaign_id = value;
        self
    }

    /// Builder-style setter for creative_id
    fn with_creative_id(mut self, value: Option<String>) -> Self {
        self.creative_id = value;
        self
    }

    /// Builder-style setter for headline
    fn with_headline(mut self, value: Option<String>) -> Self {
        self.headline = value;
        self
    }

    /// Builder-style setter for image_id
    fn with_image_id(mut self, value: Option<String>) -> Self {
        self.image_id = value;
        self
    }

    /// Builder-style setter for item_id
    fn with_item_id(mut self, value: Option<String>) -> Self {
        self.item_id = value;
        self
    }
}

/// Network detection and parameter mapping
pub struct NetworkNormalizer;

impl NetworkNormalizer {
    /// Detect the ad network from URL parameters
    ///
    /// Priority:
    /// 1. Check utm_source for known network names
    /// 2. Check for network-specific parameter presence
    pub fn detect_network(params: &HashMap<String, String>) -> &str {
        // First check utm_source
        if let Some(source) = params.get("utm_source") {
            let source_lower = source.to_lowercase();
            return match source_lower.as_str() {
                "taboola" | "tb" => "taboola",
                "outbrain" | "ob" => "outbrain",
                "mgid" => "mgid",
                "revcontent" | "rc" => "revcontent",
                "adventory" => "adventory",
                "contentad" => "contentad",
                _ => Self::detect_from_params(params),
            };
        }

        Self::detect_from_params(params)
    }

    /// Detect network from parameter presence (fallback)
    fn detect_from_params(params: &HashMap<String, String>) -> &str {
        let keys: Vec<_> = params.keys().map(|k| k.to_lowercase()).collect();

        // Check for network-specific prefixes
        if keys.iter().any(|k| k.starts_with("tb_")) {
            "taboola"
        } else if keys.iter().any(|k| k.starts_with("ob_")) {
            "outbrain"
        } else if keys.iter().any(|k| k.starts_with("mg_")) {
            "mgid"
        } else if keys.iter().any(|k| k.starts_with("rc_")) {
            "revcontent"
        } else {
            "unknown"
        }
    }

    /// Normalize campaign parameters into a common schema
    pub fn normalize(params: &HashMap<String, String>) -> NormalizedCampaign {
        let network = Self::detect_network(params);

        match network {
            "taboola" => Self::normalize_taboola(params),
            "outbrain" => Self::normalize_outbrain(params),
            "mgid" => Self::normalize_mgid(params),
            "revcontent" => Self::normalize_revcontent(params),
            _ => Self::normalize_generic(params),
        }
    }

    /// Normalize Taboola parameters
    ///
    /// Taboola uses: tb_item, tb_image, tb_headline
    fn normalize_taboola(params: &HashMap<String, String>) -> NormalizedCampaign {
        NormalizedCampaign::new("taboola")
            .with_campaign_id(params.get("utm_campaign").map(|s| s.to_string()))
            .with_item_id(params.get("tb_item").map(|s| s.to_string()))
            .with_creative_id(params.get("tb_image").map(|s| s.to_string()))
            .with_headline(params.get("tb_headline").map(|s| s.to_string()))
            .with_image_id(params.get("tb_image").map(|s| s.to_string()))
    }

    /// Normalize Outbrain parameters
    ///
    /// Outbrain uses: ob_item, ob_creative
    fn normalize_outbrain(params: &HashMap<String, String>) -> NormalizedCampaign {
        NormalizedCampaign::new("outbrain")
            .with_campaign_id(params.get("utm_campaign").map(|s| s.to_string()))
            .with_item_id(params.get("ob_item").map(|s| s.to_string()))
            .with_creative_id(params.get("ob_creative").map(|s| s.to_string()))
            .with_image_id(params.get("ob_creative").map(|s| s.to_string()))
    }

    /// Normalize MGID parameters
    ///
    /// MGID uses: mg_id, mg_title, mg_image
    fn normalize_mgid(params: &HashMap<String, String>) -> NormalizedCampaign {
        NormalizedCampaign::new("mgid")
            .with_campaign_id(params.get("utm_campaign").map(|s| s.to_string()))
            .with_item_id(params.get("mg_id").map(|s| s.to_string()))
            .with_creative_id(params.get("mg_id").map(|s| s.to_string()))
            .with_headline(params.get("mg_title").map(|s| s.to_string()))
            .with_image_id(params.get("mg_image").map(|s| s.to_string()))
    }

    /// Normalize RevContent parameters
    ///
    /// RevContent uses: rc_id, rc_title, rc_thumb
    fn normalize_revcontent(params: &HashMap<String, String>) -> NormalizedCampaign {
        NormalizedCampaign::new("revcontent")
            .with_campaign_id(params.get("utm_campaign").map(|s| s.to_string()))
            .with_item_id(params.get("rc_id").map(|s| s.to_string()))
            .with_creative_id(params.get("rc_id").map(|s| s.to_string()))
            .with_headline(params.get("rc_title").map(|s| s.to_string()))
            .with_image_id(params.get("rc_thumb").map(|s| s.to_string()))
    }

    /// Normalize generic/unknown network parameters
    ///
    /// Falls back to utm_campaign and tries to find any headline/title/image params
    fn normalize_generic(params: &HashMap<String, String>) -> NormalizedCampaign {
        let campaign_id = params.get("utm_campaign").map(|s| s.to_string());
        let item_id = params.get("item").or_else(|| params.get("asset")).map(|s| s.to_string());

        // Try common headline/title keys
        let headline = params.get("headline")
            .or_else(|| params.get("title"))
            .or_else(|| params.get("head"))
            .map(|s| s.to_string());

        // Try common image/thumb keys
        let image_id = params.get("image")
            .or_else(|| params.get("img"))
            .or_else(|| params.get("thumb"))
            .or_else(|| params.get("thumbnail"))
            .map(|s| s.to_string());

        NormalizedCampaign::new("unknown")
            .with_campaign_id(campaign_id)
            .with_item_id(item_id.clone())
            .with_creative_id(item_id)
            .with_headline(headline)
            .with_image_id(image_id)
    }

    /// Check if parameters contain any network-specific data
    pub fn has_campaign_data(params: &HashMap<String, String>) -> bool {
        let network = Self::detect_network(params);
        if network != "unknown" {
            return true;
        }

        // Check for generic campaign indicators
        params.contains_key("utm_campaign")
            || params.contains_key("item")
            || params.contains_key("asset")
    }

    /// Get a consistent creative fingerprint for deduplication
    ///
    /// Combines network + creative_id + headline for a unique identifier
    pub fn creative_fingerprint(normalized: &NormalizedCampaign) -> Option<String> {
        if normalized.creative_id.is_some() || normalized.headline.is_some() {
            Some(format!(
                "{}:{}:{}",
                normalized.network,
                normalized.creative_id.as_deref().unwrap_or(""),
                normalized.headline.as_deref().unwrap_or("")
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_network_taboola() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "taboola");

        params.clear();
        params.insert("tb_image".to_string(), "img123".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "taboola");
    }

    #[test]
    fn test_detect_network_outbrain() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "outbrain".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "outbrain");

        params.clear();
        params.insert("ob_creative".to_string(), "cr456".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "outbrain");
    }

    #[test]
    fn test_detect_network_mgid() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "mgid".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "mgid");

        params.clear();
        params.insert("mg_title".to_string(), "Lose Weight".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "mgid");
    }

    #[test]
    fn test_detect_network_revcontent() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "revcontent".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "revcontent");

        params.clear();
        params.insert("rc_id".to_string(), "rc789".to_string());
        assert_eq!(NetworkNormalizer::detect_network(&params), "revcontent");
    }

    #[test]
    fn test_normalize_taboola() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("utm_campaign".to_string(), "camp123".to_string());
        params.insert("tb_image".to_string(), "img-abc".to_string());
        params.insert("tb_headline".to_string(), "Click Here Now".to_string());
        params.insert("tb_item".to_string(), "item-456".to_string());

        let normalized = NetworkNormalizer::normalize(&params);

        assert_eq!(normalized.network, "taboola");
        assert_eq!(normalized.campaign_id, Some("camp123".to_string()));
        assert_eq!(normalized.creative_id, Some("img-abc".to_string()));
        assert_eq!(normalized.headline, Some("Click Here Now".to_string()));
        assert_eq!(normalized.image_id, Some("img-abc".to_string()));
        assert_eq!(normalized.item_id, Some("item-456".to_string()));
    }

    #[test]
    fn test_normalize_mgid() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "mgid".to_string());
        params.insert("mg_id".to_string(), "mg-789".to_string());
        params.insert("mg_title".to_string(), "Doctors Hate Him".to_string());
        params.insert("mg_image".to_string(), "mg-img-123".to_string());

        let normalized = NetworkNormalizer::normalize(&params);

        assert_eq!(normalized.network, "mgid");
        assert_eq!(normalized.creative_id, Some("mg-789".to_string()));
        assert_eq!(normalized.headline, Some("Doctors Hate Him".to_string()));
        assert_eq!(normalized.image_id, Some("mg-img-123".to_string()));
    }

    #[test]
    fn test_normalize_revcontent() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "revcontent".to_string());
        params.insert("rc_id".to_string(), "rc-999".to_string());
        params.insert("rc_title".to_string(), "One Weird Trick".to_string());
        params.insert("rc_thumb".to_string(), "thumb.jpg".to_string());

        let normalized = NetworkNormalizer::normalize(&params);

        assert_eq!(normalized.network, "revcontent");
        assert_eq!(normalized.creative_id, Some("rc-999".to_string()));
        assert_eq!(normalized.headline, Some("One Weird Trick".to_string()));
        assert_eq!(normalized.image_id, Some("thumb.jpg".to_string()));
    }

    #[test]
    fn test_normalize_outbrain() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "outbrain".to_string());
        params.insert("utm_campaign".to_string(), "ob-camp".to_string());
        params.insert("ob_creative".to_string(), "ob-cr-123".to_string());
        params.insert("ob_item".to_string(), "ob-item-456".to_string());

        let normalized = NetworkNormalizer::normalize(&params);

        assert_eq!(normalized.network, "outbrain");
        assert_eq!(normalized.campaign_id, Some("ob-camp".to_string()));
        assert_eq!(normalized.creative_id, Some("ob-cr-123".to_string()));
        assert_eq!(normalized.item_id, Some("ob-item-456".to_string()));
        assert_eq!(normalized.image_id, Some("ob-cr-123".to_string()));
    }

    #[test]
    fn test_creative_fingerprint() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("tb_image".to_string(), "img123".to_string());
        params.insert("tb_headline".to_string(), "Test Headline".to_string());

        let normalized = NetworkNormalizer::normalize(&params);
        let fingerprint = NetworkNormalizer::creative_fingerprint(&normalized);

        assert_eq!(fingerprint, Some("taboola:img123:Test Headline".to_string()));
    }

    #[test]
    fn test_has_campaign_data() {
        let mut params = HashMap::new();
        assert!(!NetworkNormalizer::has_campaign_data(&params));

        params.insert("utm_campaign".to_string(), "camp123".to_string());
        assert!(NetworkNormalizer::has_campaign_data(&params));

        params.clear();
        params.insert("tb_image".to_string(), "img123".to_string());
        assert!(NetworkNormalizer::has_campaign_data(&params));
    }
}
