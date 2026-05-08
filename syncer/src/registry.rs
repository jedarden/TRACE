//! Creative registry for managing creative metadata

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::creative::CreativeMetadata;
use crate::s3_store::CreativeStore;

/// In-memory registry of creative metadata
pub struct CreativeRegistry {
    store: Box<dyn CreativeStore>,
    creatives: Arc<RwLock<HashMap<String, CreativeMetadata>>>,
}

impl CreativeRegistry {
    /// Create a new registry with the given store
    pub fn new(store: impl CreativeStore + 'static) -> Self {
        Self {
            store: Box::new(store),
            creatives: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a creative to the registry
    pub async fn add_creative(&mut self, creative: CreativeMetadata) -> anyhow::Result<()> {
        let key = creative.key();
        let mut creatives = self.creatives.write().await;
        creatives.insert(key, creative);
        Ok(())
    }

    /// Get a creative by network, campaign, and creative ID
    pub async fn get_creative(
        &self,
        network: &str,
        campaign_id: &str,
        creative_id: &str,
    ) -> Option<CreativeMetadata> {
        let key = format!("{}:{}:{}", network, campaign_id, creative_id);
        let creatives = self.creatives.read().await;
        creatives.get(&key).cloned()
    }

    /// Get all creatives for a network
    pub async fn get_network_creatives(&self, network: &str) -> Vec<CreativeMetadata> {
        let creatives = self.creatives.read().await;
        creatives
            .values()
            .filter(|c| c.network == network)
            .cloned()
            .collect()
    }

    /// Get all creatives for a campaign
    pub async fn get_campaign_creatives(
        &self,
        network: &str,
        campaign_id: &str,
    ) -> Vec<CreativeMetadata> {
        let creatives = self.creatives.read().await;
        creatives
            .values()
            .filter(|c| c.network == network && c.campaign_id.as_deref() == Some(campaign_id))
            .cloned()
            .collect()
    }

    /// Get the total number of creatives in the registry
    pub async fn len(&self) -> usize {
        let creatives = self.creatives.read().await;
        creatives.len()
    }

    /// Persist the registry to the store
    pub async fn persist(&self) -> anyhow::Result<()> {
        let creatives = self.creatives.read().await;
        let creative_vec: Vec<CreativeMetadata> = creatives.values().cloned().collect();
        self.store.store(creative_vec).await?;
        Ok(())
    }

    /// Load the registry from the store
    pub async fn load(&mut self) -> anyhow::Result<()> {
        let creatives = self.store.load().await?;
        let mut registry = self.creatives.write().await;
        registry.clear();

        for creative in creatives {
            let key = creative.key();
            registry.insert(key, creative);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_store::MockCreativeStore;

    #[tokio::test]
    async fn test_registry_add_get() {
        let store = MockCreativeStore::new();
        let mut registry = CreativeRegistry::new(store);

        let creative = CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: None,
            creative_id: Some("creative456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: None,
            landing_page_url: None,
            item_id: None,
            synced_at: chrono::Utc::now(),
        };

        registry.add_creative(creative.clone()).await.unwrap();

        let retrieved = registry
            .get_creative("taboola", "camp123", "creative456")
            .await;

        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().headline,
            Some("Test Headline".to_string())
        );
    }

    #[tokio::test]
    async fn test_registry_len() {
        let store = MockCreativeStore::new();
        let mut registry = CreativeRegistry::new(store);

        assert_eq!(registry.len().await, 0);

        let creative = CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: None,
            creative_id: Some("creative456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: None,
            landing_page_url: None,
            item_id: None,
            synced_at: chrono::Utc::now(),
        };

        registry.add_creative(creative).await.unwrap();
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn test_registry_get_network_creatives() {
        let store = MockCreativeStore::new();
        let mut registry = CreativeRegistry::new(store);

        let c1 = CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp1".to_string()),
            campaign_name: None,
            creative_id: Some("cr1".to_string()),
            headline: Some("Headline 1".to_string()),
            image_url: None,
            landing_page_url: None,
            item_id: None,
            synced_at: chrono::Utc::now(),
        };

        let c2 = CreativeMetadata {
            network: "outbrain".to_string(),
            campaign_id: Some("camp2".to_string()),
            campaign_name: None,
            creative_id: Some("cr2".to_string()),
            headline: Some("Headline 2".to_string()),
            image_url: None,
            landing_page_url: None,
            item_id: None,
            synced_at: chrono::Utc::now(),
        };

        registry.add_creative(c1).await.unwrap();
        registry.add_creative(c2).await.unwrap();

        let taboola_creatives = registry.get_network_creatives("taboola").await;
        assert_eq!(taboola_creatives.len(), 1);
        assert_eq!(taboola_creatives[0].network, "taboola");

        let outbrain_creatives = registry.get_network_creatives("outbrain").await;
        assert_eq!(outbrain_creatives.len(), 1);
        assert_eq!(outbrain_creatives[0].network, "outbrain");
    }
}
