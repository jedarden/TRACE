//! Campaign hierarchy data structures
//!
//! Represents the hierarchical organization of advertising accounts:
//! Account → Campaign → AdGroup → Creative

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Full account hierarchy from an ad network
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountHierarchy {
    /// Ad network name (taboola, outbrain, mgid, revcontent, googleads, meta)
    pub network: String,

    /// Account identifier
    pub account_id: String,

    /// Account name (optional)
    pub account_name: Option<String>,

    /// Campaigns under this account
    pub campaigns: Vec<CampaignHierarchy>,

    /// When this hierarchy was synced from the API
    pub synced_at: DateTime<Utc>,
}

/// Campaign-level hierarchy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CampaignHierarchy {
    /// Campaign identifier
    pub campaign_id: String,

    /// Campaign name
    pub campaign_name: Option<String>,

    /// Campaign status (active, paused, archived, etc.)
    pub status: Option<String>,

    /// Ad groups under this campaign (empty for networks without ad groups)
    pub ad_groups: Vec<AdGroupHierarchy>,

    /// Creatives directly under this campaign (for networks without ad groups)
    pub creatives: Vec<CreativeHierarchy>,
}

/// Ad-group level hierarchy
///
/// Some networks (Google Ads, Meta) use ad groups as an intermediate level
/// between campaigns and individual creatives.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdGroupHierarchy {
    /// Ad group identifier
    pub ad_group_id: String,

    /// Ad group name
    pub ad_group_name: Option<String>,

    /// Ad group status
    pub status: Option<String>,

    /// Targeting criteria (optional)
    pub targeting: Option<String>,

    /// Creatives under this ad group
    pub creatives: Vec<CreativeHierarchy>,
}

/// Creative-level hierarchy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreativeHierarchy {
    /// Creative identifier
    pub creative_id: String,

    /// Headline or title text
    pub headline: Option<String>,

    /// Image URL or identifier
    pub image_url: Option<String>,

    /// Landing page URL
    pub landing_page_url: Option<String>,

    /// Item identifier (network-specific)
    pub item_id: Option<String>,

    /// Creative status
    pub status: Option<String>,
}

impl AccountHierarchy {
    /// Generate a unique key for this account hierarchy
    pub fn key(&self) -> String {
        format!("{}:{}", self.network, self.account_id)
    }

    /// Get all campaigns flattened
    pub fn all_campaigns(&self) -> Vec<&CampaignHierarchy> {
        self.campaigns.iter().collect()
    }

    /// Get all ad groups flattened across all campaigns
    pub fn all_ad_groups(&self) -> Vec<&AdGroupHierarchy> {
        self.campaigns
            .iter()
            .flat_map(|c| c.ad_groups.iter())
            .collect()
    }

    /// Get all creatives flattened across all campaigns and ad groups
    pub fn all_creatives(&self) -> Vec<CreativeRef> {
        let mut creatives = Vec::new();

        for campaign in &self.campaigns {
            // Creatives directly under campaign
            for creative in &campaign.creatives {
                creatives.push(CreativeRef {
                    campaign_id: campaign.campaign_id.clone(),
                    ad_group_id: None,
                    creative: creative.clone(),
                });
            }

            // Creatives under ad groups
            for ad_group in &campaign.ad_groups {
                for creative in &ad_group.creatives {
                    creatives.push(CreativeRef {
                        campaign_id: campaign.campaign_id.clone(),
                        ad_group_id: Some(ad_group.ad_group_id.clone()),
                        creative: creative.clone(),
                    });
                }
            }
        }

        creatives
    }
}

impl CampaignHierarchy {
    /// Generate a unique key for this campaign
    pub fn key(&self) -> String {
        format!("{}", self.campaign_id)
    }

    /// Get total number of creatives in this campaign
    pub fn creative_count(&self) -> usize {
        let direct_count = self.creatives.len();
        let ad_group_count: usize = self.ad_groups.iter().map(|ag| ag.creatives.len()).sum();
        direct_count + ad_group_count
    }
}

impl AdGroupHierarchy {
    /// Generate a unique key for this ad group
    pub fn key(&self) -> String {
        format!("{}", self.ad_group_id)
    }
}

impl CreativeHierarchy {
    /// Generate a unique key for this creative
    pub fn key(&self) -> String {
        format!("{}", self.creative_id)
    }
}

/// Reference to a creative with its parent hierarchy context
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreativeRef {
    /// Parent campaign ID
    pub campaign_id: String,

    /// Parent ad group ID (if any)
    pub ad_group_id: Option<String>,

    /// The creative
    #[serde(flatten)]
    pub creative: CreativeHierarchy,
}

/// Convert legacy CreativeMetadata to hierarchy format
impl From<CreativeHierarchy> for crate::creative::CreativeMetadata {
    fn from(creative: CreativeHierarchy) -> Self {
        Self {
            network: String::new(), // Must be set by caller
            campaign_id: None,      // Must be set by caller
            campaign_name: None,
            creative_id: Some(creative.creative_id),
            headline: creative.headline,
            image_url: creative.image_url,
            landing_page_url: creative.landing_page_url,
            item_id: creative.item_id,
            synced_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_hierarchy_key() {
        let hierarchy = AccountHierarchy {
            network: "taboola".to_string(),
            account_id: "acc-123".to_string(),
            account_name: Some("Test Account".to_string()),
            campaigns: vec![],
            synced_at: Utc::now(),
        };

        assert_eq!(hierarchy.key(), "taboola:acc-123");
    }

    #[test]
    fn test_campaign_creative_count() {
        let campaign = CampaignHierarchy {
            campaign_id: "camp-1".to_string(),
            campaign_name: Some("Test Campaign".to_string()),
            status: Some("active".to_string()),
            ad_groups: vec![
                AdGroupHierarchy {
                    ad_group_id: "ag-1".to_string(),
                    ad_group_name: Some("Ad Group 1".to_string()),
                    status: None,
                    targeting: None,
                    creatives: vec![],
                },
                AdGroupHierarchy {
                    ad_group_id: "ag-2".to_string(),
                    ad_group_name: Some("Ad Group 2".to_string()),
                    status: None,
                    targeting: None,
                    creatives: vec![
                        CreativeHierarchy {
                            creative_id: "cr-1".to_string(),
                            headline: None,
                            image_url: None,
                            landing_page_url: None,
                            item_id: None,
                            status: None,
                        },
                        CreativeHierarchy {
                            creative_id: "cr-2".to_string(),
                            headline: None,
                            image_url: None,
                            landing_page_url: None,
                            item_id: None,
                            status: None,
                        },
                    ],
                },
            ],
            creatives: vec![
                CreativeHierarchy {
                    creative_id: "cr-3".to_string(),
                    headline: None,
                    image_url: None,
                    landing_page_url: None,
                    item_id: None,
                    status: None,
                },
            ],
        };

        // 1 direct creative + 2 in ad group = 3 total
        assert_eq!(campaign.creative_count(), 3);
    }

    #[test]
    fn test_all_creatives_flattened() {
        let hierarchy = AccountHierarchy {
            network: "googleads".to_string(),
            account_id: "acc-123".to_string(),
            account_name: None,
            campaigns: vec![
                CampaignHierarchy {
                    campaign_id: "camp-1".to_string(),
                    campaign_name: None,
                    status: None,
                    ad_groups: vec![AdGroupHierarchy {
                        ad_group_id: "ag-1".to_string(),
                        ad_group_name: None,
                        status: None,
                        targeting: None,
                        creatives: vec![
                            CreativeHierarchy {
                                creative_id: "cr-1".to_string(),
                                headline: None,
                                image_url: None,
                                landing_page_url: None,
                                item_id: None,
                                status: None,
                            },
                        ],
                    }],
                    creatives: vec![],
                },
            ],
            synced_at: Utc::now(),
        };

        let all_creatives = hierarchy.all_creatives();
        assert_eq!(all_creatives.len(), 1);
        assert_eq!(all_creatives[0].campaign_id, "camp-1");
        assert_eq!(all_creatives[0].ad_group_id, Some("ag-1".to_string()));
        assert_eq!(all_creatives[0].creative.creative_id, "cr-1");
    }

    #[test]
    fn test_hierarchy_with_direct_creatives() {
        // Networks without ad groups (Taboola, Outbrain, etc.)
        let hierarchy = AccountHierarchy {
            network: "taboola".to_string(),
            account_id: "acc-123".to_string(),
            account_name: None,
            campaigns: vec![CampaignHierarchy {
                campaign_id: "camp-1".to_string(),
                campaign_name: Some("Taboola Campaign".to_string()),
                status: Some("active".to_string()),
                ad_groups: vec![],
                creatives: vec![
                    CreativeHierarchy {
                        creative_id: "cr-1".to_string(),
                        headline: Some("Headline 1".to_string()),
                        image_url: Some("https://example.com/img1.jpg".to_string()),
                        landing_page_url: None,
                        item_id: None,
                        status: None,
                    },
                    CreativeHierarchy {
                        creative_id: "cr-2".to_string(),
                        headline: Some("Headline 2".to_string()),
                        image_url: Some("https://example.com/img2.jpg".to_string()),
                        landing_page_url: None,
                        item_id: None,
                        status: None,
                    },
                ],
            }],
            synced_at: Utc::now(),
        };

        let all_creatives = hierarchy.all_creatives();
        assert_eq!(all_creatives.len(), 2);
        assert_eq!(all_creatives[0].ad_group_id, None); // No ad group
        assert_eq!(all_creatives[0].campaign_id, "camp-1");
    }

    #[test]
    fn test_all_ad_groups() {
        let hierarchy = AccountHierarchy {
            network: "meta".to_string(),
            account_id: "acc-123".to_string(),
            account_name: None,
            campaigns: vec![
                CampaignHierarchy {
                    campaign_id: "camp-1".to_string(),
                    campaign_name: None,
                    status: None,
                    ad_groups: vec![
                        AdGroupHierarchy {
                            ad_group_id: "adset-1".to_string(),
                            ad_group_name: None,
                            status: None,
                            targeting: None,
                            creatives: vec![],
                        },
                        AdGroupHierarchy {
                            ad_group_id: "adset-2".to_string(),
                            ad_group_name: None,
                            status: None,
                            targeting: None,
                            creatives: vec![],
                        },
                    ],
                    creatives: vec![],
                },
                CampaignHierarchy {
                    campaign_id: "camp-2".to_string(),
                    campaign_name: None,
                    status: None,
                    ad_groups: vec![AdGroupHierarchy {
                        ad_group_id: "adset-3".to_string(),
                        ad_group_name: None,
                        status: None,
                        targeting: None,
                        creatives: vec![],
                    }],
                    creatives: vec![],
                },
            ],
            synced_at: Utc::now(),
        };

        let all_ad_groups = hierarchy.all_ad_groups();
        assert_eq!(all_ad_groups.len(), 3);
    }
}
