//! Ad network API clients
//!
//! Each client fetches creative metadata from its respective ad network API.

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use crate::creative::CreativeMetadata;

/// Result of an API sync operation
#[derive(Debug)]
#[allow(dead_code)]
pub struct ApiSyncResult {
    pub creatives: Vec<CreativeMetadata>,
    pub next_page_token: Option<String>,
}

/// Trait for ad network API clients
#[async_trait]
pub trait ApiClient: Send + Sync {
    /// Fetch creative metadata from the API
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult>;

    /// Get the network name for this client
    fn network_name(&self) -> &str;
}

/// Taboola API client
///
/// Taboola Backstage API documentation: https://developers.taboola.com/
pub struct TaboolaClient {
    api_key: String,
    base_url: String,
    account_id: Option<String>,
}

impl TaboolaClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://backstage.taboola.com".to_string(),
            account_id: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_account_id(mut self, account_id: String) -> Self {
        self.account_id = Some(account_id);
        self
    }

    /// Fetch campaigns and their creatives from Taboola
    async fn fetch_campaigns(&self) -> Result<Vec<TaboolaCampaign>> {
        let client = reqwest::Client::new();

        // Taboola API endpoint for fetching campaigns
        let account = self.account_id.as_deref().unwrap_or("me");
        let url = format!("{}/backstage/api/1.0/{}/campaigns/", self.base_url, account);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .query(&[("include", "active")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Taboola API error: {}", response.status()));
        }

        let data: TaboolaCampaignsResponse = response.json().await?;
        Ok(data.results)
    }
}

#[derive(Debug, Deserialize)]
struct TaboolaCampaignsResponse {
    results: Vec<TaboolaCampaign>,
}

#[derive(Debug, Deserialize)]
struct TaboolaCampaign {
    id: String,
    name: String,
    #[serde(default)]
    items: Vec<TaboolaItem>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TaboolaItem {
    id: String,
    name: String,
    thumbnail_url: Option<String>,
    url: Option<String>,
    title: Option<String>,
}

#[async_trait]
impl ApiClient for TaboolaClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let creatives: Vec<CreativeMetadata> = campaigns
            .into_iter()
            .flat_map(|campaign| {
                campaign
                    .items
                    .into_iter()
                    .map(move |item| CreativeMetadata {
                        network: "taboola".to_string(),
                        campaign_id: Some(campaign.id.clone()),
                        campaign_name: Some(campaign.name.clone()),
                        creative_id: Some(item.id.clone()),
                        headline: item.title,
                        image_url: item.thumbnail_url,
                        landing_page_url: item.url,
                        item_id: Some(item.id),
                        synced_at: Utc::now(),
                    })
            })
            .collect();

        Ok(ApiSyncResult {
            creatives,
            next_page_token: None,
        })
    }

    fn network_name(&self) -> &str {
        "taboola"
    }
}

/// Outbrain API client
///
/// Outbrain Amplify API documentation: https://www.outbrain.com/amplify/help/advertisers/api
pub struct OutbrainClient {
    api_key: String,
    base_url: String,
}

impl OutbrainClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://api.outbrain.com".to_string(),
        }
    }

    async fn fetch_campaigns(&self) -> Result<Vec<OutbrainCampaign>> {
        let client = reqwest::Client::new();

        let url = format!("{}/amplify/v0.1/users/me/campaigns", self.base_url);

        let response = client
            .get(&url)
            .header("OB-TOKEN", &self.api_key)
            .query(&[("status", "ACTIVE")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Outbrain API error: {}", response.status()));
        }

        let campaigns: Vec<OutbrainCampaign> = response.json().await?;
        Ok(campaigns)
    }
}

#[derive(Debug, Deserialize)]
struct OutbrainCampaign {
    id: String,
    name: String,
    links: Vec<OutbrainLink>,
}

#[derive(Debug, Deserialize)]
struct OutbrainLink {
    id: String,
    url: Option<String>,
    #[serde(rename = "imageUrl")]
    image_url: Option<String>,
    metadata: Option<OutbrainMetadata>,
}

#[derive(Debug, Deserialize)]
struct OutbrainMetadata {
    title: Option<String>,
}

#[async_trait]
impl ApiClient for OutbrainClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let creatives: Vec<CreativeMetadata> = campaigns
            .into_iter()
            .flat_map(|campaign| {
                campaign
                    .links
                    .into_iter()
                    .map(move |link| CreativeMetadata {
                        network: "outbrain".to_string(),
                        campaign_id: Some(campaign.id.clone()),
                        campaign_name: Some(campaign.name.clone()),
                        creative_id: Some(link.id.clone()),
                        headline: link.metadata.as_ref().and_then(|m| m.title.clone()),
                        image_url: link.image_url,
                        landing_page_url: link.url,
                        item_id: Some(link.id),
                        synced_at: Utc::now(),
                    })
            })
            .collect();

        Ok(ApiSyncResult {
            creatives,
            next_page_token: None,
        })
    }

    fn network_name(&self) -> &str {
        "outbrain"
    }
}

/// MGID API client
///
/// MGID API documentation: https://mgid.com/
pub struct MgidClient {
    api_key: String,
    base_url: String,
}

impl MgidClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://api.mgid.com".to_string(),
        }
    }

    async fn fetch_campaigns(&self) -> Result<Vec<MgidCampaign>> {
        let client = reqwest::Client::new();

        let url = format!("{}/v1/campaigns", self.base_url);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .query(&[("status", "active")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("MGID API error: {}", response.status()));
        }

        let data: MgidResponse = response.json().await?;
        Ok(data.data)
    }
}

#[derive(Debug, Deserialize)]
struct MgidResponse {
    data: Vec<MgidCampaign>,
}

#[derive(Debug, Deserialize)]
struct MgidCampaign {
    id: String,
    name: String,
    teasers: Vec<MgidTeaser>,
}

#[derive(Debug, Deserialize)]
struct MgidTeaser {
    id: String,
    title: Option<String>,
    image: Option<String>,
    url: Option<String>,
}

#[async_trait]
impl ApiClient for MgidClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let creatives: Vec<CreativeMetadata> = campaigns
            .into_iter()
            .flat_map(|campaign| {
                campaign
                    .teasers
                    .into_iter()
                    .map(move |teaser| CreativeMetadata {
                        network: "mgid".to_string(),
                        campaign_id: Some(campaign.id.clone()),
                        campaign_name: Some(campaign.name.clone()),
                        creative_id: Some(teaser.id.clone()),
                        headline: teaser.title,
                        image_url: teaser.image,
                        landing_page_url: teaser.url,
                        item_id: Some(teaser.id),
                        synced_at: Utc::now(),
                    })
            })
            .collect();

        Ok(ApiSyncResult {
            creatives,
            next_page_token: None,
        })
    }

    fn network_name(&self) -> &str {
        "mgid"
    }
}

/// RevContent API client
///
/// RevContent API documentation: https://revcontent.com/
pub struct RevcontentClient {
    api_key: String,
    base_url: String,
}

impl RevcontentClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://api.revcontent.com".to_string(),
        }
    }

    async fn fetch_campaigns(&self) -> Result<Vec<RevcontentCampaign>> {
        let client = reqwest::Client::new();

        let url = format!("{}/v1/campaigns", self.base_url);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .query(&[("status", "active")])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "RevContent API error: {}",
                response.status()
            ));
        }

        let data: RevcontentResponse = response.json().await?;
        Ok(data.campaigns)
    }
}

#[derive(Debug, Deserialize)]
struct RevcontentResponse {
    campaigns: Vec<RevcontentCampaign>,
}

#[derive(Debug, Deserialize)]
struct RevcontentCampaign {
    id: String,
    name: String,
    widgets: Vec<RevcontentWidget>,
}

#[derive(Debug, Deserialize)]
struct RevcontentWidget {
    id: String,
    title: Option<String>,
    thumbnail: Option<String>,
    url: Option<String>,
}

#[async_trait]
impl ApiClient for RevcontentClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let creatives: Vec<CreativeMetadata> = campaigns
            .into_iter()
            .flat_map(|campaign| {
                campaign
                    .widgets
                    .into_iter()
                    .map(move |widget| CreativeMetadata {
                        network: "revcontent".to_string(),
                        campaign_id: Some(campaign.id.clone()),
                        campaign_name: Some(campaign.name.clone()),
                        creative_id: Some(widget.id.clone()),
                        headline: widget.title,
                        image_url: widget.thumbnail,
                        landing_page_url: widget.url,
                        item_id: Some(widget.id),
                        synced_at: Utc::now(),
                    })
            })
            .collect();

        Ok(ApiSyncResult {
            creatives,
            next_page_token: None,
        })
    }

    fn network_name(&self) -> &str {
        "revcontent"
    }
}

/// Demo client for testing without real API keys
///
/// Generates sample creative data for testing and development.
#[allow(dead_code)]
pub struct DemoClient {
    initialized: bool,
}

impl DemoClient {
    pub fn new() -> Self {
        Self { initialized: false }
    }

    fn generate_demo_creatives(&self) -> Vec<CreativeMetadata> {
        vec![
            CreativeMetadata {
                network: "taboola".to_string(),
                campaign_id: Some("demo-camp-001".to_string()),
                campaign_name: Some("Demo Campaign 1".to_string()),
                creative_id: Some("demo-creative-001".to_string()),
                headline: Some("Doctors Hate This One Weird Trick".to_string()),
                image_url: Some("https://example.com/img1.jpg".to_string()),
                landing_page_url: Some("https://example.com/landing1".to_string()),
                item_id: Some("demo-item-001".to_string()),
                synced_at: Utc::now(),
            },
            CreativeMetadata {
                network: "outbrain".to_string(),
                campaign_id: Some("demo-camp-002".to_string()),
                campaign_name: Some("Demo Campaign 2".to_string()),
                creative_id: Some("demo-creative-002".to_string()),
                headline: Some("Lose 30 Pounds In 30 Days".to_string()),
                image_url: Some("https://example.com/img2.jpg".to_string()),
                landing_page_url: Some("https://example.com/landing2".to_string()),
                item_id: Some("demo-item-002".to_string()),
                synced_at: Utc::now(),
            },
            CreativeMetadata {
                network: "mgid".to_string(),
                campaign_id: Some("demo-camp-003".to_string()),
                campaign_name: Some("Demo Campaign 3".to_string()),
                creative_id: Some("demo-creative-003".to_string()),
                headline: Some("You Won't Believe What Happens Next".to_string()),
                image_url: Some("https://example.com/img3.jpg".to_string()),
                landing_page_url: Some("https://example.com/landing3".to_string()),
                item_id: Some("demo-item-003".to_string()),
                synced_at: Utc::now(),
            },
            CreativeMetadata {
                network: "revcontent".to_string(),
                campaign_id: Some("demo-camp-004".to_string()),
                campaign_name: Some("Demo Campaign 4".to_string()),
                creative_id: Some("demo-creative-004".to_string()),
                headline: Some("This Simple Trick Will Change Your Life".to_string()),
                image_url: Some("https://example.com/img4.jpg".to_string()),
                landing_page_url: Some("https://example.com/landing4".to_string()),
                item_id: Some("demo-item-004".to_string()),
                synced_at: Utc::now(),
            },
        ]
    }
}

#[async_trait]
impl ApiClient for DemoClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
        Ok(ApiSyncResult {
            creatives: self.generate_demo_creatives(),
            next_page_token: None,
        })
    }

    fn network_name(&self) -> &str {
        "demo"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_demo_client() {
        let mut client = DemoClient::new();
        let result = client.fetch_creatives().await.unwrap();

        assert_eq!(result.creatives.len(), 4);
        assert_eq!(client.network_name(), "demo");

        // Check Taboola creative
        let taboola = &result.creatives[0];
        assert_eq!(taboola.network, "taboola");
        assert_eq!(
            taboola.headline,
            Some("Doctors Hate This One Weird Trick".to_string())
        );
    }

    #[test]
    fn test_network_names() {
        let taboola = TaboolaClient::new("test".to_string());
        assert_eq!(taboola.network_name(), "taboola");

        let outbrain = OutbrainClient::new("test".to_string());
        assert_eq!(outbrain.network_name(), "outbrain");

        let mgid = MgidClient::new("test".to_string());
        assert_eq!(mgid.network_name(), "mgid");

        let revcontent = RevcontentClient::new("test".to_string());
        assert_eq!(revcontent.network_name(), "revcontent");
    }
}
