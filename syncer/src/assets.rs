//! Assets dimension table: creative metadata exploded into per-asset rows
//!
//! A creative from an ad network API bundles several assets — a headline,
//! an image, and a landing page. The `trace.assets` dimension table
//! (analytics/schemas/assets_iceberg.sql, partitioned by network and type)
//! stores them at asset grain so per-asset performance reporting can join
//! against ad_events.
//!
//! This module explodes synced `CreativeMetadata` into `AssetRecord` rows,
//! deduplicates them on the documented asset id convention
//! (`{network}:{type}:{content}`), and persists them as Parquet partitioned
//! by `network=<network>/type=<type>/`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::creative::CreativeMetadata;
use crate::s3_store::AssetStore;

/// Asset type values written to the `type` column (partition column)
pub const TYPE_HEADLINE: &str = "headline";
pub const TYPE_IMAGE: &str = "image";
pub const TYPE_LANDING_PAGE: &str = "landing_page";

/// A single creative asset in the assets dimension table
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct AssetRecord {
    /// Documented asset id convention: `{network}:{type}:{content}`
    pub asset_id: String,
    /// Ad network name (taboola, outbrain, mgid, revcontent)
    pub network: String,
    /// Asset classification: headline, image, landing_page
    pub asset_type: String,
    /// Text for headlines, URL for images and landing pages
    pub content: String,

    /// Join keys back to the creative and campaign the asset was synced from
    pub creative_id: Option<String>,
    pub campaign_id: Option<String>,
    pub campaign_name: Option<String>,
    pub item_id: Option<String>,

    /// First and most recent time this asset was observed in a sync
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub synced_at: DateTime<Utc>,
}

impl AssetRecord {
    /// Build an asset row for a piece of creative content
    pub fn new(
        network: &str,
        asset_type: &str,
        content: String,
        creative: &CreativeMetadata,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            // The id convention documented in assets_iceberg.sql
            asset_id: format!("{}:{}:{}", network, asset_type, content),
            network: network.to_string(),
            asset_type: asset_type.to_string(),
            content,
            creative_id: creative.creative_id.clone(),
            campaign_id: creative.campaign_id.clone(),
            campaign_name: creative.campaign_name.clone(),
            item_id: creative.item_id.clone(),
            first_seen: observed_at,
            last_seen: observed_at,
            synced_at: observed_at,
        }
    }

    /// Merge a re-observation of the same asset: keep the earliest
    /// first_seen, advance last_seen, and backfill join keys the earlier
    /// observation was missing.
    pub fn merge(&mut self, other: AssetRecord) {
        self.first_seen = self.first_seen.min(other.first_seen);
        self.last_seen = self.last_seen.max(other.last_seen);
        self.synced_at = other.synced_at;

        if self.creative_id.is_none() {
            self.creative_id = other.creative_id;
        }
        if self.campaign_id.is_none() {
            self.campaign_id = other.campaign_id;
        }
        if self.campaign_name.is_none() {
            self.campaign_name = other.campaign_name;
        }
        if self.item_id.is_none() {
            self.item_id = other.item_id;
        }
    }

    /// Dedup key for the in-memory registry
    pub fn key(&self) -> &str {
        &self.asset_id
    }
}

/// Explode one creative into its constituent assets (headline, image,
/// landing page). Assets with no content produce no row.
pub fn explode_creative(creative: &CreativeMetadata) -> Vec<AssetRecord> {
    let mut assets = Vec::with_capacity(3);

    if let Some(headline) = creative
        .headline
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
    {
        assets.push(AssetRecord::new(
            &creative.network,
            TYPE_HEADLINE,
            headline.to_string(),
            creative,
            creative.synced_at,
        ));
    }

    if let Some(image_url) = creative
        .image_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
    {
        assets.push(AssetRecord::new(
            &creative.network,
            TYPE_IMAGE,
            image_url.to_string(),
            creative,
            creative.synced_at,
        ));
    }

    if let Some(landing_page) = creative
        .landing_page_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
    {
        assets.push(AssetRecord::new(
            &creative.network,
            TYPE_LANDING_PAGE,
            landing_page.to_string(),
            creative,
            creative.synced_at,
        ));
    }

    assets
}

/// In-memory registry of asset dimension rows, backed by an asset store
pub struct AssetRegistry {
    store: Box<dyn AssetStore>,
    assets: HashMap<String, AssetRecord>,
}

impl AssetRegistry {
    /// Create a new registry with the given store
    pub fn new(store: impl AssetStore + 'static) -> Self {
        Self {
            store: Box::new(store),
            assets: HashMap::new(),
        }
    }

    /// Load previously synced assets so first_seen survives restarts and
    /// last_seen history accumulates across sync runs
    pub async fn load(&mut self) -> anyhow::Result<usize> {
        let loaded = self.store.load_assets().await?;
        let count = loaded.len();
        for asset in loaded {
            match self.assets.get_mut(asset.key()) {
                Some(existing) => existing.merge(asset),
                None => {
                    self.assets.insert(asset.key().to_string(), asset);
                }
            }
        }
        Ok(count)
    }

    /// Merge freshly synced creatives into the dimension
    pub async fn sync_from_creatives(&mut self, creatives: Vec<CreativeMetadata>) -> usize {
        let mut added = 0;
        for creative in creatives {
            for asset in explode_creative(&creative) {
                match self.assets.get_mut(asset.key()) {
                    Some(existing) => existing.merge(asset),
                    None => {
                        self.assets.insert(asset.key().to_string(), asset);
                        added += 1;
                    }
                }
            }
        }
        added
    }

    /// Get an asset by its documented id
    pub fn get(&self, asset_id: &str) -> Option<&AssetRecord> {
        self.assets.get(asset_id)
    }

    /// Total number of distinct assets in the registry
    pub fn len(&self) -> usize {
        self.assets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assets.is_empty()
    }

    /// Persist the dimension to the store, partitioned by (network, type)
    pub async fn persist(&self) -> anyhow::Result<()> {
        self.store
            .store_assets(self.assets.values().cloned().collect())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creative(
        network: &str,
        headline: Option<&str>,
        image: Option<&str>,
        lp: Option<&str>,
    ) -> CreativeMetadata {
        CreativeMetadata {
            network: network.to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: Some("Test Campaign".to_string()),
            creative_id: Some("cr456".to_string()),
            headline: headline.map(|s| s.to_string()),
            image_url: image.map(|s| s.to_string()),
            landing_page_url: lp.map(|s| s.to_string()),
            item_id: Some("item789".to_string()),
            synced_at: Utc::now(),
        }
    }

    #[test]
    fn test_explode_creative_full() {
        let assets = explode_creative(&creative(
            "taboola",
            Some("Test Headline"),
            Some("https://example.com/img.jpg"),
            Some("https://example.com/landing"),
        ));

        assert_eq!(assets.len(), 3);
        let types: Vec<&str> = assets.iter().map(|a| a.asset_type.as_str()).collect();
        assert_eq!(types, vec![TYPE_HEADLINE, TYPE_IMAGE, TYPE_LANDING_PAGE]);

        let headline = &assets[0];
        assert_eq!(headline.asset_id, "taboola:headline:Test Headline");
        assert_eq!(headline.content, "Test Headline");
        assert_eq!(headline.creative_id.as_deref(), Some("cr456"));
        assert_eq!(headline.campaign_id.as_deref(), Some("camp123"));
    }

    #[test]
    fn test_explode_creative_partial() {
        // Only a headline — image and landing page produce no rows
        let assets = explode_creative(&creative("mgid", Some("Doctors Hate Him"), None, None));
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].asset_type, TYPE_HEADLINE);

        // A creative with no asset content at all produces no rows
        let empty = explode_creative(&creative("mgid", None, None, None));
        assert!(empty.is_empty());
    }

    #[test]
    fn test_explode_creative_blank_content_skipped() {
        let assets = explode_creative(&creative("outbrain", Some("   "), Some(""), None));
        assert!(assets.is_empty());
    }

    #[test]
    fn test_merge_advances_last_seen_keeps_first_seen() {
        let first = AssetRecord::new(
            "taboola",
            TYPE_HEADLINE,
            "Same Headline".to_string(),
            &creative("taboola", Some("Same Headline"), None, None),
            DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        let later = AssetRecord::new(
            "taboola",
            TYPE_HEADLINE,
            "Same Headline".to_string(),
            &creative("taboola", Some("Same Headline"), None, None),
            DateTime::parse_from_rfc3339("2026-09-10T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );

        let mut merged = first;
        merged.merge(later);

        assert_eq!(
            merged.first_seen,
            DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(
            merged.last_seen,
            DateTime::parse_from_rfc3339("2026-09-10T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn test_merge_backfills_missing_join_keys() {
        let mut existing = AssetRecord::new(
            "taboola",
            TYPE_IMAGE,
            "https://example.com/img.jpg".to_string(),
            // First observation had no campaign info
            &CreativeMetadata {
                campaign_id: None,
                campaign_name: None,
                ..creative("taboola", None, Some("https://example.com/img.jpg"), None)
            },
            Utc::now(),
        );
        assert!(existing.campaign_id.is_none());

        let reobserved = AssetRecord::new(
            "taboola",
            TYPE_IMAGE,
            "https://example.com/img.jpg".to_string(),
            &creative("taboola", None, Some("https://example.com/img.jpg"), None),
            Utc::now(),
        );
        existing.merge(reobserved);

        assert_eq!(existing.campaign_id.as_deref(), Some("camp123"));
        assert_eq!(existing.creative_id.as_deref(), Some("cr456"));
    }

    /// In-memory asset store for registry tests
    ///
    /// Clone shares the backing buffer, so a cloned store sees the same
    /// persisted rows — the reload tests rely on this to read back what
    /// the first registry persisted.
    #[derive(Clone)]
    struct MemoryAssetStore {
        assets: std::sync::Arc<tokio::sync::RwLock<Vec<AssetRecord>>>,
    }

    impl MemoryAssetStore {
        fn new() -> Self {
            Self {
                assets: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl AssetStore for MemoryAssetStore {
        async fn store_assets(&self, assets: Vec<AssetRecord>) -> anyhow::Result<()> {
            *self.assets.write().await = assets;
            Ok(())
        }

        async fn load_assets(&self) -> anyhow::Result<Vec<AssetRecord>> {
            Ok(self.assets.read().await.clone())
        }
    }

    #[tokio::test]
    async fn test_registry_sync_dedups_and_persists() {
        let store = MemoryAssetStore::new();
        let mut registry = AssetRegistry::new(store.clone());

        let synced = registry
            .sync_from_creatives(vec![
                creative(
                    "taboola",
                    Some("Headline A"),
                    Some("https://img/a.jpg"),
                    None,
                ),
                creative(
                    "taboola",
                    Some("Headline A"),
                    Some("https://img/a.jpg"),
                    None,
                ),
                creative("outbrain", Some("Headline A"), None, None),
            ])
            .await;

        // The repeated taboola creative dedups to one headline + one image;
        // outbrain's identical headline text is a distinct asset (network grain)
        assert_eq!(synced, 3);
        assert_eq!(registry.len(), 3);
        assert!(registry.get("taboola:headline:Headline A").is_some());
        assert!(registry.get("outbrain:headline:Headline A").is_some());

        registry.persist().await.unwrap();

        // A fresh registry loads the persisted dimension
        let mut reloaded = AssetRegistry::new(store);
        let count = reloaded.load().await.unwrap();
        assert_eq!(count, 3);
        assert!(reloaded.get("taboola:image:https://img/a.jpg").is_some());
    }

    #[tokio::test]
    async fn test_registry_reload_merges_history() {
        let store = MemoryAssetStore::new();

        let mut first = AssetRegistry::new(store.clone());
        first
            .sync_from_creatives(vec![creative("taboola", Some("Headline A"), None, None)])
            .await;
        first.persist().await.unwrap();

        // Second run (e.g. process restart) re-syncs the same asset
        let mut second = AssetRegistry::new(store);
        let loaded = second.load().await.unwrap();
        assert_eq!(loaded, 1);
        let added = second
            .sync_from_creatives(vec![creative("taboola", Some("Headline A"), None, None)])
            .await;
        assert_eq!(added, 0, "re-syncing a known asset adds no new row");
        assert_eq!(second.len(), 1);
    }
}
