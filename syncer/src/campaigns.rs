//! Campaign dimension rows materialized from account hierarchies.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::hierarchy::AccountHierarchy;
use crate::s3_store::CampaignStore;

#[derive(Debug, Clone, PartialEq)]
pub struct CampaignRecord {
    pub network: String,
    pub campaign_id: String,
    pub campaign_name: Option<String>,
    pub status: Option<String>,
    pub account_id: String,
    pub account_name: Option<String>,
    pub synced_at: DateTime<Utc>,
}

impl CampaignRecord {
    fn merge(&mut self, other: Self) {
        if other.synced_at < self.synced_at {
            return;
        }
        if other.campaign_name.is_some() {
            self.campaign_name = other.campaign_name;
        }
        if other.status.is_some() {
            self.status = other.status;
        }
        if other.account_name.is_some() {
            self.account_name = other.account_name;
        }
        self.account_id = other.account_id;
        self.synced_at = other.synced_at;
    }
}

pub struct CampaignRegistry {
    store: Box<dyn CampaignStore>,
    campaigns: HashMap<(String, String), CampaignRecord>,
    pending: HashMap<(String, String), CampaignRecord>,
}

impl CampaignRegistry {
    pub fn new(store: impl CampaignStore + 'static) -> Self {
        Self {
            store: Box::new(store),
            campaigns: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    pub async fn load(&mut self) -> anyhow::Result<usize> {
        let rows = self.store.load_campaigns().await?;
        let count = rows.len();
        for row in rows {
            self.upsert(row);
        }
        Ok(count)
    }

    pub fn sync_from_hierarchy(&mut self, hierarchy: &AccountHierarchy) {
        for campaign in &hierarchy.campaigns {
            let row = CampaignRecord {
                network: hierarchy.network.clone(),
                campaign_id: campaign.campaign_id.clone(),
                campaign_name: campaign.campaign_name.clone(),
                status: campaign.status.clone(),
                account_id: hierarchy.account_id.clone(),
                account_name: hierarchy.account_name.clone(),
                synced_at: hierarchy.synced_at,
            };
            let key = (row.network.clone(), row.campaign_id.clone());
            let previous = self.campaigns.get(&key).cloned();
            self.upsert(row);
            let current = self
                .campaigns
                .get(&key)
                .expect("campaign was just upserted");
            if previous.as_ref() != Some(current) {
                self.pending.insert(key, current.clone());
            }
        }
    }

    fn upsert(&mut self, row: CampaignRecord) {
        let key = (row.network.clone(), row.campaign_id.clone());
        match self.campaigns.get_mut(&key) {
            Some(existing) => existing.merge(row),
            None => {
                self.campaigns.insert(key, row);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.campaigns.len()
    }

    pub async fn persist(&self) -> anyhow::Result<()> {
        self.store
            .store_campaigns(self.campaigns.values().cloned().collect())
            .await
    }

    pub async fn publish_revisions(
        &mut self,
        config: &trace_iceberg::publisher::PublisherConfig,
    ) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.store
            .publish_campaign_revisions(self.pending.values().cloned().collect(), config)
            .await?;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchy::CampaignHierarchy;

    #[derive(Clone, Default)]
    struct MemoryCampaignStore(std::sync::Arc<tokio::sync::RwLock<Vec<CampaignRecord>>>);

    #[async_trait::async_trait]
    impl CampaignStore for MemoryCampaignStore {
        async fn store_campaigns(&self, rows: Vec<CampaignRecord>) -> anyhow::Result<()> {
            *self.0.write().await = rows;
            Ok(())
        }

        async fn load_campaigns(&self) -> anyhow::Result<Vec<CampaignRecord>> {
            Ok(self.0.read().await.clone())
        }

        async fn publish_campaign_revisions(
            &self,
            _rows: Vec<CampaignRecord>,
            _config: &trace_iceberg::publisher::PublisherConfig,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn hierarchy(network: &str, name: Option<&str>, timestamp: DateTime<Utc>) -> AccountHierarchy {
        AccountHierarchy {
            network: network.into(),
            account_id: "account-1".into(),
            account_name: Some("Account".into()),
            campaigns: vec![CampaignHierarchy {
                campaign_id: "campaign-1".into(),
                campaign_name: name.map(str::to_string),
                status: Some("active".into()),
                ad_groups: vec![],
                creatives: vec![],
            }],
            synced_at: timestamp,
        }
    }

    #[tokio::test]
    async fn keys_by_network_and_campaign_and_preserves_known_fields() {
        let store = MemoryCampaignStore::default();
        let mut registry = CampaignRegistry::new(store.clone());
        let now = Utc::now();
        registry.sync_from_hierarchy(&hierarchy("taboola", Some("Original"), now));
        registry.sync_from_hierarchy(&hierarchy("mgid", Some("MGID"), now));
        registry.sync_from_hierarchy(&hierarchy(
            "taboola",
            None,
            now + chrono::Duration::seconds(1),
        ));
        registry.persist().await.unwrap();

        let mut rows = store.load_campaigns().await.unwrap();
        rows.sort_by(|a, b| a.network.cmp(&b.network));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].campaign_name.as_deref(), Some("Original"));
        assert_eq!(rows[1].account_id, "account-1");
        assert_eq!(rows[1].status.as_deref(), Some("active"));

        let mut restarted = CampaignRegistry::new(store);
        assert_eq!(restarted.load().await.unwrap(), 2);
        assert_eq!(restarted.len(), 2);
        assert!(restarted.pending.is_empty());
        restarted.sync_from_hierarchy(&hierarchy(
            "taboola",
            None,
            now + chrono::Duration::seconds(1),
        ));
        assert!(
            restarted.pending.is_empty(),
            "unchanged rows need no revision"
        );
    }
}
