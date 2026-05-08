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
}
