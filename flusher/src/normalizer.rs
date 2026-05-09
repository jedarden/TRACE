//! Cross-network normalization module
//!
//! This module provides config-driven normalization of ad network URL parameters
//! into a canonical schema for unified analytics across different ad networks.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Canonical field names for normalized ad tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalField {
    CampaignId,
    AdId,
    CreativeId,
    PublisherId,
    PlacementId,
    Headline,
    ImageId,
    ItemId,
}

impl CanonicalField {
    /// Get the string key for this canonical field
    pub fn as_str(&self) -> &'static str {
        match self {
            CanonicalField::CampaignId => "campaign_id",
            CanonicalField::AdId => "ad_id",
            CanonicalField::CreativeId => "creative_id",
            CanonicalField::PublisherId => "publisher_id",
            CanonicalField::PlacementId => "placement_id",
            CanonicalField::Headline => "headline",
            CanonicalField::ImageId => "image_id",
            CanonicalField::ItemId => "item_id",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "campaign_id" => Some(CanonicalField::CampaignId),
            "ad_id" => Some(CanonicalField::AdId),
            "creative_id" => Some(CanonicalField::CreativeId),
            "publisher_id" => Some(CanonicalField::PublisherId),
            "placement_id" => Some(CanonicalField::PlacementId),
            "headline" => Some(CanonicalField::Headline),
            "image_id" => Some(CanonicalField::ImageId),
            "item_id" => Some(CanonicalField::ItemId),
            _ => None,
        }
    }
}

/// Network configuration from TOML
#[derive(Debug, Clone, Deserialize)]
struct NetworkConfig {
    name: String,
    #[serde(default)]
    utm_source_values: Vec<String>,
    #[serde(default)]
    param_prefix: Option<String>,
    #[serde(default)]
    click_identifiers: Vec<String>,
    #[serde(default)]
    mapping: HashMap<String, Vec<String>>,
}

/// Normalization configuration loaded from TOML
#[derive(Debug, Clone, Deserialize)]
struct NormalizationConfig {
    networks: HashMap<String, NetworkConfig>,
}

/// Normalization mapping table
#[derive(Debug, Clone)]
pub struct NormalizationMapping {
    /// Network detection rules
    networks: HashMap<String, NetworkDetection>,
    /// Parameter mappings per network: network -> canonical field -> param names
    mappings: HashMap<String, HashMap<CanonicalField, Vec<String>>>,
}

/// Network detection rules
#[derive(Debug, Clone)]
struct NetworkDetection {
    name: String,
    utm_source_values: Vec<String>,
    param_prefix: Option<String>,
    click_identifiers: Vec<String>,
}

impl NormalizationMapping {
    /// Load normalization configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: NormalizationConfig = toml::from_str(&content)
            .context("Failed to parse TOML config")?;

        Self::from_config(config)
    }

    /// Create normalization mapping from parsed config
    fn from_config(config: NormalizationConfig) -> Result<Self> {
        let mut networks = HashMap::new();
        let mut mappings = HashMap::new();

        for (key, network_config) in config.networks {
            let name = network_config.name.clone();

            // Build detection rules
            let detection = NetworkDetection {
                name: name.clone(),
                utm_source_values: network_config.utm_source_values,
                param_prefix: network_config.param_prefix,
                click_identifiers: network_config.click_identifiers,
            };
            networks.insert(name.clone(), detection);

            // Build parameter mappings
            let mut field_mappings = HashMap::new();
            for (canonical_field, param_names) in network_config.mapping {
                if let Some(field) = CanonicalField::from_str(&canonical_field) {
                    field_mappings.insert(field, param_names);
                }
            }
            mappings.insert(name, field_mappings);
        }

        Ok(Self {
            networks,
            mappings,
        })
    }

    /// Detect the ad network from URL parameters
    pub fn detect_network(&self, params: &HashMap<String, String>) -> &str {
        // First check utm_source
        if let Some(source) = params.get("utm_source") {
            let source_lower = source.to_lowercase();
            for (name, detection) in &self.networks {
                if detection.utm_source_values.iter().any(|v| v == source_lower.as_str()) {
                    return name;
                }
            }
        }

        // Check for click identifiers (gclid, fbclid, etc.)
        for key in params.keys().map(|k| k.to_lowercase()) {
            for (name, detection) in &self.networks {
                if detection.click_identifiers.iter().any(|id| id == key) {
                    return name;
                }
            }
        }

        // Check for parameter prefixes
        let keys: Vec<_> = params.keys().map(|k| k.to_lowercase()).collect();
        for (name, detection) in &self.networks {
            if let Some(prefix) = &detection.param_prefix {
                if keys.iter().any(|k| k.starts_with(prefix)) {
                    return name;
                }
            }
        }

        "unknown"
    }

    /// Normalize parameters into canonical fields
    pub fn normalize(
        &self,
        params: &HashMap<String, String>,
    ) -> HashMap<CanonicalField, String> {
        let network = self.detect_network(params);
        let mut result = HashMap::new();

        // Get the mapping for this network
        if let Some(field_mappings) = self.mappings.get(network) {
            // Lowercase params for case-insensitive matching
            let params_lower: HashMap<String, (String, String)> = params
                .iter()
                .map(|(k, v)| (k.to_lowercase(), (k.clone(), v.clone())))
                .collect();

            for (field, param_names) in field_mappings {
                for param_name in param_names {
                    let param_lower = param_name.to_lowercase();
                    if let Some((original_key, value)) = params_lower.get(&param_lower) {
                        result.insert(*field, value.clone());
                        break; // Use first match
                    }
                }
            }
        }

        result
    }

    /// Get the network name for a given set of parameters
    pub fn get_network_name(&self, params: &HashMap<String, String>) -> String {
        let network = self.detect_network(params);
        self.networks
            .get(network)
            .map(|d| d.name.clone())
            .unwrap_or_else(|| "unknown".to_string())
    }
}

/// Normalized event data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedData {
    /// Detected ad network
    pub network: String,
    /// Normalized campaign identifier
    pub campaign_id: Option<String>,
    /// Normalized ad identifier
    pub ad_id: Option<String>,
    /// Normalized creative identifier
    pub creative_id: Option<String>,
    /// Normalized publisher identifier
    pub publisher_id: Option<String>,
    /// Normalized placement identifier
    pub placement_id: Option<String>,
    /// Normalized headline/title
    pub headline: Option<String>,
    /// Normalized image identifier
    pub image_id: Option<String>,
    /// Normalized item identifier
    pub item_id: Option<String>,
}

impl NormalizedData {
    /// Apply normalization to a set of URL parameters
    pub fn from_params(
        params: &HashMap<String, String>,
        mapping: &NormalizationMapping,
    ) -> Self {
        let normalized = mapping.normalize(params);
        let network = mapping.get_network_name(params);

        Self {
            network,
            campaign_id: normalized.get(&CanonicalField::CampaignId).cloned(),
            ad_id: normalized.get(&CanonicalField::AdId).cloned(),
            creative_id: normalized.get(&CanonicalField::CreativeId).cloned(),
            publisher_id: normalized.get(&CanonicalField::PublisherId).cloned(),
            placement_id: normalized.get(&CanonicalField::PlacementId).cloned(),
            headline: normalized.get(&CanonicalField::Headline).cloned(),
            image_id: normalized.get(&CanonicalField::ImageId).cloned(),
            item_id: normalized.get(&CanonicalField::ItemId).cloned(),
        }
    }

    /// Merge normalized data with existing values (existing takes precedence)
    pub fn merge_with_existing(self, existing: &NormalizedData) -> Self {
        Self {
            network: if existing.network != "unknown" {
                existing.network.clone()
            } else {
                self.network
            },
            campaign_id: existing.campaign_id.clone().or(self.campaign_id),
            ad_id: existing.ad_id.clone().or(self.ad_id),
            creative_id: existing.creative_id.clone().or(self.creative_id),
            publisher_id: existing.publisher_id.clone().or(self.publisher_id),
            placement_id: existing.placement_id.clone().or(self.placement_id),
            headline: existing.headline.clone().or(self.headline),
            image_id: existing.image_id.clone().or(self.image_id),
            item_id: existing.item_id.clone().or(self.item_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_mapping() -> NormalizationMapping {
        let toml_content = r#"
[networks]
[networks.taboola]
name = "taboola"
utm_source_values = ["taboola", "tb"]
param_prefix = "tb_"

[networks.taboola.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["tb_item"]
creative_id = ["tb_image"]
publisher_id = ["tb_publisher"]
placement_id = ["tb_placement"]
headline = ["tb_headline"]
image_id = ["tb_image"]
item_id = ["tb_item"]

[networks.outbrain]
name = "outbrain"
utm_source_values = ["outbrain", "ob"]
param_prefix = "ob_"

[networks.outbrain.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["ob_item"]
creative_id = ["ob_creative"]
publisher_id = ["ob_publisher"]
placement_id = ["ob_placement"]
headline = []
image_id = ["ob_creative"]
item_id = ["ob_item"]

[networks.mgid]
name = "mgid"
utm_source_values = ["mgid"]
param_prefix = "mg_"

[networks.mgid.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["mg_id"]
creative_id = ["mg_id"]
publisher_id = ["mg_site"]
placement_id = ["mg_placement"]
headline = ["mg_title"]
image_id = ["mg_image"]
item_id = ["mg_id"]

[networks.revcontent]
name = "revcontent"
utm_source_values = ["revcontent", "rc"]
param_prefix = "rc_"

[networks.revcontent.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["rc_id"]
creative_id = ["rc_id"]
publisher_id = ["rc_site"]
placement_id = ["rc_placement"]
headline = ["rc_title"]
image_id = ["rc_thumb"]
item_id = ["rc_id"]

[networks.googleads]
name = "googleads"
utm_source_values = ["google", "googleads"]
click_identifiers = ["gclid", "gclsrc"]

[networks.googleads.mapping]
campaign_id = ["utm_campaign", "campaignid"]
ad_id = ["adgroupid"]
creative_id = ["adgroupid", "utm_content"]
publisher_id = ["siteid"]
placement_id = ["placement"]
headline = ["headline", "keyword"]
image_id = ["imageid"]
item_id = ["adgroupid"]

[networks.meta]
name = "meta"
utm_source_values = ["facebook", "instagram", "meta"]
click_identifiers = ["fbclid", "igshid"]

[networks.meta.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["ad_id"]
creative_id = ["ad_id", "utm_content"]
publisher_id = ["siteid"]
placement_id = ["placementid"]
headline = ["headline", "ad_name"]
image_id = ["imageid"]
item_id = ["ad_id"]

[networks.generic]
name = "unknown"

[networks.generic.mapping]
campaign_id = ["utm_campaign"]
ad_id = ["item"]
creative_id = ["creative"]
publisher_id = ["publisher"]
placement_id = ["placement"]
headline = ["headline"]
image_id = ["image"]
item_id = ["item"]
"#;

        let config: NormalizationConfig = toml::from_str(toml_content).unwrap();
        NormalizationMapping::from_config(config).unwrap()
    }

    #[test]
    fn test_detect_network_taboola_utm_source() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        assert_eq!(mapping.detect_network(&params), "taboola");
    }

    #[test]
    fn test_detect_network_taboola_params() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("tb_image".to_string(), "img123".to_string());
        assert_eq!(mapping.detect_network(&params), "taboola");
    }

    #[test]
    fn test_detect_network_outbrain() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "outbrain".to_string());
        assert_eq!(mapping.detect_network(&params), "outbrain");
    }

    #[test]
    fn test_detect_network_mgid() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "mgid".to_string());
        assert_eq!(mapping.detect_network(&params), "mgid");
    }

    #[test]
    fn test_detect_network_revcontent() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("rc_id".to_string(), "rc123".to_string());
        assert_eq!(mapping.detect_network(&params), "revcontent");
    }

    #[test]
    fn test_detect_network_googleads_gclid() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("gclid".to_string(), "test123".to_string());
        assert_eq!(mapping.detect_network(&params), "googleads");
    }

    #[test]
    fn test_detect_network_meta_fbclid() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("fbclid".to_string(), "test456".to_string());
        assert_eq!(mapping.detect_network(&params), "meta");
    }

    #[test]
    fn test_normalize_taboola() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("utm_campaign".to_string(), "camp123".to_string());
        params.insert("tb_image".to_string(), "img-abc".to_string());
        params.insert("tb_headline".to_string(), "Click Here Now".to_string());
        params.insert("tb_item".to_string(), "item-456".to_string());
        params.insert("tb_publisher".to_string(), "pub-789".to_string());
        params.insert("tb_placement".to_string(), "placement-123".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "taboola");
        assert_eq!(normalized.campaign_id, Some("camp123".to_string()));
        assert_eq!(normalized.creative_id, Some("img-abc".to_string()));
        assert_eq!(normalized.headline, Some("Click Here Now".to_string()));
        assert_eq!(normalized.ad_id, Some("item-456".to_string()));
        assert_eq!(normalized.publisher_id, Some("pub-789".to_string()));
        assert_eq!(normalized.placement_id, Some("placement-123".to_string()));
    }

    #[test]
    fn test_normalize_mgid() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "mgid".to_string());
        params.insert("mg_id".to_string(), "mg-789".to_string());
        params.insert("mg_title".to_string(), "Doctors Hate Him".to_string());
        params.insert("mg_image".to_string(), "mg-img-123".to_string());
        params.insert("mg_site".to_string(), "example.com".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "mgid");
        assert_eq!(normalized.creative_id, Some("mg-789".to_string()));
        assert_eq!(normalized.headline, Some("Doctors Hate Him".to_string()));
        assert_eq!(normalized.image_id, Some("mg-img-123".to_string()));
        assert_eq!(normalized.ad_id, Some("mg-789".to_string()));
        assert_eq!(normalized.publisher_id, Some("example.com".to_string()));
    }

    #[test]
    fn test_normalize_googleads() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("gclid".to_string(), "test123".to_string());
        params.insert("campaignid".to_string(), "camp456".to_string());
        params.insert("adgroupid".to_string(), "adgroup789".to_string());
        params.insert("keyword".to_string(), "best running shoes".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "googleads");
        assert_eq!(normalized.campaign_id, Some("camp456".to_string()));
        assert_eq!(normalized.ad_id, Some("adgroup789".to_string()));
        assert_eq!(normalized.creative_id, Some("adgroup789".to_string()));
        assert_eq!(normalized.headline, Some("best running shoes".to_string()));
    }

    #[test]
    fn test_normalize_meta() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("fbclid".to_string(), "test456".to_string());
        params.insert("utm_campaign".to_string(), "summer_sale".to_string());
        params.insert("ad_id".to_string(), "ad2385123".to_string());
        params.insert("ad_name".to_string(), "Best Product".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "meta");
        assert_eq!(normalized.campaign_id, Some("summer_sale".to_string()));
        assert_eq!(normalized.ad_id, Some("ad2385123".to_string()));
        assert_eq!(normalized.creative_id, Some("ad2385123".to_string()));
        assert_eq!(normalized.headline, Some("Best Product".to_string()));
    }

    #[test]
    fn test_normalize_unknown() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("utm_campaign".to_string(), "unknown_camp".to_string());
        params.insert("item".to_string(), "item123".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "unknown");
        assert_eq!(normalized.campaign_id, Some("unknown_camp".to_string()));
        assert_eq!(normalized.ad_id, Some("item123".to_string()));
    }

    #[test]
    fn test_case_insensitive_matching() {
        let mapping = create_test_mapping();
        let mut params = HashMap::new();
        params.insert("UTM_SOURCE".to_string(), "TABOOLA".to_string());
        params.insert("TB_IMAGE".to_string(), "img123".to_string());
        params.insert("TB_HEADLINE".to_string(), "Test Headline".to_string());

        let normalized = NormalizedData::from_params(&params, &mapping);

        assert_eq!(normalized.network, "taboola");
        assert_eq!(normalized.creative_id, Some("img123".to_string()));
        assert_eq!(normalized.headline, Some("Test Headline".to_string()));
    }

    #[test]
    fn test_merge_with_existing() {
        let mapping = create_test_mapping();

        // Create new normalized data
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("tb_image".to_string(), "new-img".to_string());
        let new_data = NormalizedData::from_params(&params, &mapping);

        // Create existing data with some fields already set
        let existing_data = NormalizedData {
            network: "taboola".to_string(),
            campaign_id: Some("existing-camp".to_string()),
            ad_id: None,
            creative_id: Some("existing-img".to_string()),
            publisher_id: None,
            placement_id: None,
            headline: Some("Existing Headline".to_string()),
            image_id: None,
            item_id: None,
        };

        // Merge - existing should take precedence
        let merged = new_data.merge_with_existing(&existing_data);

        assert_eq!(merged.campaign_id, Some("existing-camp".to_string()));
        assert_eq!(merged.creative_id, Some("existing-img".to_string()));
        assert_eq!(merged.headline, Some("Existing Headline".to_string()));
        // Fields only in new data should come through
        assert_eq!(merged.ad_id, None); // tb_item not in params
    }
}
