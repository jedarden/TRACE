//! Ad network API clients
//!
//! Each client fetches creative metadata from its respective ad network API.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use serde::Deserialize;

use crate::creative::{CreativeMetadata, PerformanceMetrics};
use crate::hierarchy::AccountHierarchy;

/// Result of an API sync operation
#[derive(Debug)]
#[allow(dead_code)]
pub struct ApiSyncResult {
    pub creatives: Vec<CreativeMetadata>,
    pub next_page_token: Option<String>,
}

/// Result of a metrics sync operation
#[derive(Debug)]
pub struct MetricsSyncResult {
    pub metrics: Vec<PerformanceMetrics>,
    pub next_page_token: Option<String>,
}

/// Result of a hierarchy sync operation
#[derive(Debug)]
pub struct HierarchySyncResult {
    pub hierarchies: Vec<AccountHierarchy>,
    pub next_page_token: Option<String>,
}

/// Trait for ad network API clients
#[async_trait]
pub trait ApiClient: Send + Sync {
    /// Fetch creative metadata from the API
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult>;

    /// Fetch performance metrics for a date range
    async fn fetch_metrics(
        &mut self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<MetricsSyncResult>;

    /// Fetch account hierarchy from the API
    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult>;

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

    async fn fetch_metrics(
        &mut self,
        _start_date: NaiveDate,
        _end_date: NaiveDate,
    ) -> Result<MetricsSyncResult> {
        // Placeholder implementation - Taboola API metrics endpoint would be called here
        // For now, return empty metrics
        Ok(MetricsSyncResult {
            metrics: vec![],
            next_page_token: None,
        })
    }

    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let hierarchy = AccountHierarchy {
            network: "taboola".to_string(),
            account_id: self
                .account_id
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            account_name: None,
            campaigns: campaigns
                .into_iter()
                .map(|campaign| crate::hierarchy::CampaignHierarchy {
                    campaign_id: campaign.id,
                    campaign_name: Some(campaign.name),
                    status: Some("active".to_string()),
                    ad_groups: vec![],
                    creatives: campaign
                        .items
                        .into_iter()
                        .map(|item| crate::hierarchy::CreativeHierarchy {
                            creative_id: item.id.clone(),
                            headline: item.title,
                            image_url: item.thumbnail_url,
                            landing_page_url: item.url,
                            item_id: Some(item.id),
                            status: None,
                        })
                        .collect(),
                })
                .collect(),
            synced_at: Utc::now(),
        };

        Ok(HierarchySyncResult {
            hierarchies: vec![hierarchy],
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

    async fn fetch_metrics(
        &mut self,
        _start_date: NaiveDate,
        _end_date: NaiveDate,
    ) -> Result<MetricsSyncResult> {
        // Placeholder implementation - Outbrain API metrics endpoint would be called here
        Ok(MetricsSyncResult {
            metrics: vec![],
            next_page_token: None,
        })
    }

    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let hierarchy = AccountHierarchy {
            network: "outbrain".to_string(),
            account_id: "default".to_string(),
            account_name: None,
            campaigns: campaigns
                .into_iter()
                .map(|campaign| crate::hierarchy::CampaignHierarchy {
                    campaign_id: campaign.id,
                    campaign_name: Some(campaign.name),
                    status: Some("active".to_string()),
                    ad_groups: vec![],
                    creatives: campaign
                        .links
                        .into_iter()
                        .map(|link| crate::hierarchy::CreativeHierarchy {
                            creative_id: link.id.clone(),
                            headline: link.metadata.as_ref().and_then(|m| m.title.clone()),
                            image_url: link.image_url,
                            landing_page_url: link.url,
                            item_id: Some(link.id),
                            status: None,
                        })
                        .collect(),
                })
                .collect(),
            synced_at: Utc::now(),
        };

        Ok(HierarchySyncResult {
            hierarchies: vec![hierarchy],
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

    async fn fetch_metrics(
        &mut self,
        _start_date: NaiveDate,
        _end_date: NaiveDate,
    ) -> Result<MetricsSyncResult> {
        // Placeholder implementation - MGID API metrics endpoint would be called here
        Ok(MetricsSyncResult {
            metrics: vec![],
            next_page_token: None,
        })
    }

    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let hierarchy = AccountHierarchy {
            network: "mgid".to_string(),
            account_id: "default".to_string(),
            account_name: None,
            campaigns: campaigns
                .into_iter()
                .map(|campaign| crate::hierarchy::CampaignHierarchy {
                    campaign_id: campaign.id,
                    campaign_name: Some(campaign.name),
                    status: Some("active".to_string()),
                    ad_groups: vec![],
                    creatives: campaign
                        .teasers
                        .into_iter()
                        .map(|teaser| crate::hierarchy::CreativeHierarchy {
                            creative_id: teaser.id.clone(),
                            headline: teaser.title,
                            image_url: teaser.image,
                            landing_page_url: teaser.url,
                            item_id: Some(teaser.id),
                            status: None,
                        })
                        .collect(),
                })
                .collect(),
            synced_at: Utc::now(),
        };

        Ok(HierarchySyncResult {
            hierarchies: vec![hierarchy],
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

    async fn fetch_metrics(
        &mut self,
        _start_date: NaiveDate,
        _end_date: NaiveDate,
    ) -> Result<MetricsSyncResult> {
        // Placeholder implementation - Revcontent API metrics endpoint would be called here
        Ok(MetricsSyncResult {
            metrics: vec![],
            next_page_token: None,
        })
    }

    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
        let campaigns = self.fetch_campaigns().await?;

        let hierarchy = AccountHierarchy {
            network: "revcontent".to_string(),
            account_id: "default".to_string(),
            account_name: None,
            campaigns: campaigns
                .into_iter()
                .map(|campaign| crate::hierarchy::CampaignHierarchy {
                    campaign_id: campaign.id,
                    campaign_name: Some(campaign.name),
                    status: Some("active".to_string()),
                    ad_groups: vec![],
                    creatives: campaign
                        .widgets
                        .into_iter()
                        .map(|widget| crate::hierarchy::CreativeHierarchy {
                            creative_id: widget.id.clone(),
                            headline: widget.title,
                            image_url: widget.thumbnail,
                            landing_page_url: widget.url,
                            item_id: Some(widget.id),
                            status: None,
                        })
                        .collect(),
                })
                .collect(),
            synced_at: Utc::now(),
        };

        Ok(HierarchySyncResult {
            hierarchies: vec![hierarchy],
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

    async fn fetch_metrics(
        &mut self,
        _start_date: NaiveDate,
        _end_date: NaiveDate,
    ) -> Result<MetricsSyncResult> {
        Ok(MetricsSyncResult {
            metrics: vec![],
            next_page_token: None,
        })
    }

    async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
        let hierarchy = AccountHierarchy {
            network: "demo".to_string(),
            account_id: "demo-account".to_string(),
            account_name: Some("Demo Account".to_string()),
            campaigns: self
                .generate_demo_creatives()
                .into_iter()
                .map(|c| {
                    let campaign_id = c
                        .campaign_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    let campaign_name = c.campaign_name.clone();
                    crate::hierarchy::CampaignHierarchy {
                        campaign_id,
                        campaign_name,
                        status: Some("active".to_string()),
                        ad_groups: vec![],
                        creatives: vec![crate::hierarchy::CreativeHierarchy {
                            creative_id: c.creative_id.unwrap_or_else(|| "unknown".to_string()),
                            headline: c.headline,
                            image_url: c.image_url,
                            landing_page_url: c.landing_page_url,
                            item_id: c.item_id,
                            status: None,
                        }],
                    }
                })
                .collect(),
            synced_at: Utc::now(),
        };

        Ok(HierarchySyncResult {
            hierarchies: vec![hierarchy],
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

    // --- Creative-metadata fetching against a mocked HTTP API ---
    //
    // Each real client is pointed at a local mock server speaking that
    // network's documented response shape, so the request path, auth
    // header, and creative deserialization are all exercised end to end.

    #[tokio::test]
    async fn test_taboola_fetch_creatives_parses_campaigns_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/backstage/api/1.0/me/campaigns/")
            .match_query(mockito::Matcher::UrlEncoded(
                "include".into(),
                "active".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"results": [{"id": "camp-1", "name": "Summer Push", "items": [
                    {"id": "item-9", "name": "Item Nine",
                     "thumbnail_url": "https://img.taboola/img.jpg",
                     "url": "https://advertiser/landing", "title": "Nine Tricks"}
                ]}]}"#,
            )
            .create_async()
            .await;

        let mut client = TaboolaClient {
            api_key: "key-123".to_string(),
            base_url: server.url(),
            account_id: None,
        };
        let result = client.fetch_creatives().await.unwrap();
        mock.assert_async().await;

        assert_eq!(result.creatives.len(), 1);
        let creative = &result.creatives[0];
        assert_eq!(creative.network, "taboola");
        assert_eq!(creative.campaign_id.as_deref(), Some("camp-1"));
        assert_eq!(creative.campaign_name.as_deref(), Some("Summer Push"));
        assert_eq!(creative.creative_id.as_deref(), Some("item-9"));
        assert_eq!(creative.headline.as_deref(), Some("Nine Tricks"));
        assert_eq!(
            creative.image_url.as_deref(),
            Some("https://img.taboola/img.jpg")
        );
        assert_eq!(
            creative.landing_page_url.as_deref(),
            Some("https://advertiser/landing")
        );
        assert_eq!(creative.item_id.as_deref(), Some("item-9"));
    }

    #[tokio::test]
    async fn test_taboola_fetch_creatives_api_error_is_an_err() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::Any)
            .with_status(401)
            .with_body(r#"{"message": "bad credentials"}"#)
            .create_async()
            .await;

        let mut client = TaboolaClient {
            api_key: "wrong-key".to_string(),
            base_url: server.url(),
            account_id: None,
        };

        let err = client.fetch_creatives().await.unwrap_err();
        assert!(err.to_string().contains("Taboola API error"));
        assert!(
            err.to_string().contains("401"),
            "expected the mocked 401, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_outbrain_fetch_creatives_parses_amplify_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/amplify/v0.1/users/me/campaigns")
            .match_query(mockito::Matcher::UrlEncoded(
                "status".into(),
                "ACTIVE".into(),
            ))
            .match_header("OB-TOKEN", "key-123")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id": "ob-camp", "name": "OB Campaign", "links": [
                    {"id": "ob-link-1", "url": "https://advertiser/ob",
                     "imageUrl": "https://img.outbrain/img.jpg",
                     "metadata": {"title": "OB Headline"}}
                ]}]"#,
            )
            .create_async()
            .await;

        let mut client = OutbrainClient {
            api_key: "key-123".to_string(),
            base_url: server.url(),
        };
        let result = client.fetch_creatives().await.unwrap();
        mock.assert_async().await;

        assert_eq!(result.creatives.len(), 1);
        let creative = &result.creatives[0];
        assert_eq!(creative.network, "outbrain");
        assert_eq!(creative.campaign_id.as_deref(), Some("ob-camp"));
        assert_eq!(creative.headline.as_deref(), Some("OB Headline"));
        assert_eq!(
            creative.image_url.as_deref(),
            Some("https://img.outbrain/img.jpg")
        );
        assert_eq!(
            creative.landing_page_url.as_deref(),
            Some("https://advertiser/ob")
        );
        assert_eq!(creative.item_id.as_deref(), Some("ob-link-1"));
    }

    #[tokio::test]
    async fn test_mgid_fetch_creatives_parses_teasers_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/campaigns")
            .match_query(mockito::Matcher::UrlEncoded(
                "status".into(),
                "active".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"data": [{"id": "mgid-camp", "name": "MGID Camp", "teasers": [
                    {"id": "t-1", "title": "MGID Headline",
                     "image": "https://img.mgid/img.jpg", "url": "https://advertiser/mgid"}
                ]}]}"#,
            )
            .create_async()
            .await;

        let mut client = MgidClient {
            api_key: "key-123".to_string(),
            base_url: server.url(),
        };
        let result = client.fetch_creatives().await.unwrap();
        mock.assert_async().await;

        assert_eq!(result.creatives.len(), 1);
        let creative = &result.creatives[0];
        assert_eq!(creative.network, "mgid");
        assert_eq!(creative.headline.as_deref(), Some("MGID Headline"));
        assert_eq!(
            creative.image_url.as_deref(),
            Some("https://img.mgid/img.jpg")
        );
        assert_eq!(
            creative.landing_page_url.as_deref(),
            Some("https://advertiser/mgid")
        );
        assert_eq!(creative.item_id.as_deref(), Some("t-1"));
    }

    #[tokio::test]
    async fn test_revcontent_fetch_creatives_parses_widgets_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/campaigns")
            .match_query(mockito::Matcher::UrlEncoded(
                "status".into(),
                "active".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"campaigns": [{"id": "rc-camp", "name": "RC Camp", "widgets": [
                    {"id": "w-1", "title": "RC Headline",
                     "thumbnail": "https://img.rc/img.jpg", "url": "https://advertiser/rc"}
                ]}]}"#,
            )
            .create_async()
            .await;

        let mut client = RevcontentClient {
            api_key: "key-123".to_string(),
            base_url: server.url(),
        };
        let result = client.fetch_creatives().await.unwrap();
        mock.assert_async().await;

        assert_eq!(result.creatives.len(), 1);
        let creative = &result.creatives[0];
        assert_eq!(creative.network, "revcontent");
        assert_eq!(creative.headline.as_deref(), Some("RC Headline"));
        assert_eq!(
            creative.image_url.as_deref(),
            Some("https://img.rc/img.jpg")
        );
        assert_eq!(
            creative.landing_page_url.as_deref(),
            Some("https://advertiser/rc")
        );
        assert_eq!(creative.item_id.as_deref(), Some("w-1"));
    }
}
