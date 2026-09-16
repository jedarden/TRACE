mod api_client;
mod assets;
mod creative;
mod hierarchy;
mod registry;
mod s3_store;
mod session_hierarchy;

use anyhow::Result;
use clap::Parser;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api_client::{ApiClient, ApiSyncResult, HierarchySyncResult, MetricsSyncResult};
use assets::AssetRegistry;
use registry::{CreativeRegistry, HierarchyRegistry, MetricsRegistry};
use s3_store::{HierarchyStore, S3CreativeStore};

/// TRACE Creative Syncer
///
/// Fetches creative metadata and performance metrics from ad network APIs and stores it for attribution.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Run once and exit (default: continuous sync mode)
    #[arg(short, long)]
    once: bool,

    /// Sync interval in seconds (default: 3600 = 1 hour)
    #[arg(short, long, default_value_t = 3600)]
    interval: u64,

    /// Networks to sync (default: all)
    #[arg(short, long, value_delimiter = ',')]
    networks: Option<String>,

    /// Sync mode: creatives, metrics, or both (default: creatives)
    #[arg(short, long, default_value = "creatives")]
    mode: String,

    /// Metrics sync: number of days back to fetch (default: 7)
    #[arg(short, long, default_value_t = 7)]
    days_back: u32,

    /// Metrics sync: fetch yesterday's metrics (sets days_back=1)
    #[arg(long, default_value_t = false)]
    yesterday: bool,

    /// Sync hierarchy from ad network APIs
    #[arg(long, default_value_t = false)]
    hierarchy: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "trace_syncer=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    // Load configuration from environment
    let s3_bucket = std::env::var("TRACE_S3_BUCKET").expect("TRACE_S3_BUCKET must be set");
    let s3_region = std::env::var("TRACE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let s3_prefix = std::env::var("TRACE_S3_PREFIX").unwrap_or_else(|_| "trace-events".to_string());

    // API credentials (optional - APIs can be rate limited without auth)
    let taboola_api_key = std::env::var("TABOOLA_API_KEY").ok();
    let outbrain_api_key = std::env::var("OUTBRAIN_API_KEY").ok();
    let mgid_api_key = std::env::var("MGID_API_KEY").ok();
    let revcontent_api_key = std::env::var("REVCONTENT_API_KEY").ok();

    // Initialize S3 store
    let store = S3CreativeStore::new(s3_bucket, s3_region, s3_prefix).await?;

    // Initialize registry
    let mut registry = CreativeRegistry::new(store.clone());
    let mut metrics_registry = MetricsRegistry::new(store.clone());
    let mut hierarchy_registry = HierarchyRegistry::new(store.clone());
    let mut asset_registry = AssetRegistry::new(store.clone());

    // Load previously synced assets so first_seen history survives restarts
    match asset_registry.load().await {
        Ok(count) => info!("Loaded {} existing assets from previous syncs", count),
        Err(e) => warn!("Could not load existing assets (starting empty): {}", e),
    }

    // Determine sync mode
    let sync_creatives = args.mode == "creatives" || args.mode == "both";
    let sync_metrics = args.mode == "metrics" || args.mode == "both";
    let sync_hierarchy = args.hierarchy;

    // Determine metrics date range
    let days_back = if args.yesterday { 1 } else { args.days_back };
    let end_date = chrono::Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(days_back as i64);

    // Determine which networks to sync
    let networks_to_sync = args.networks.as_ref().map(|ns| {
        ns.split(',')
            .map(|s| s.trim().to_lowercase())
            .collect::<Vec<_>>()
    });

    // Initialize API clients
    let mut clients: Vec<Box<dyn ApiClient>> = vec![];

    if let Some(ref networks) = networks_to_sync {
        if networks.contains(&"taboola".to_string()) {
            if let Some(key) = &taboola_api_key {
                clients.push(Box::new(api_client::TaboolaClient::new(key.clone())));
            }
        }
        if networks.contains(&"outbrain".to_string()) {
            if let Some(key) = &outbrain_api_key {
                clients.push(Box::new(api_client::OutbrainClient::new(key.clone())));
            }
        }
        if networks.contains(&"mgid".to_string()) {
            if let Some(key) = &mgid_api_key {
                clients.push(Box::new(api_client::MgidClient::new(key.clone())));
            }
        }
        if networks.contains(&"revcontent".to_string()) {
            if let Some(key) = &revcontent_api_key {
                clients.push(Box::new(api_client::RevcontentClient::new(key.clone())));
            }
        }
    } else {
        // Add all clients with API keys
        if let Some(key) = taboola_api_key {
            clients.push(Box::new(api_client::TaboolaClient::new(key)));
        }
        if let Some(key) = outbrain_api_key {
            clients.push(Box::new(api_client::OutbrainClient::new(key)));
        }
        if let Some(key) = mgid_api_key {
            clients.push(Box::new(api_client::MgidClient::new(key)));
        }
        if let Some(key) = revcontent_api_key {
            clients.push(Box::new(api_client::RevcontentClient::new(key)));
        }
    }

    if clients.is_empty() {
        info!("No API clients configured. Set TABOOLA_API_KEY, OUTBRAIN_API_KEY, MGID_API_KEY, or REVCONTENT_API_KEY.");
        info!("Running in demo mode with sample data.");

        // Add demo client that generates sample data
        clients.push(Box::new(api_client::DemoClient::new()));
    }

    info!(
        "TRACE creative syncer starting with {} API clients",
        clients.len()
    );
    info!("Sync mode: {}", args.mode);
    if sync_metrics {
        info!("Metrics date range: {} to {}", start_date, end_date);
    }

    if args.once {
        // Run once and exit
        if sync_creatives {
            run_sync(&mut registry, &mut asset_registry, &mut clients).await?;
        }
        if sync_metrics {
            run_metrics_sync(&mut metrics_registry, &mut clients, start_date, end_date).await?;
        }
        if sync_hierarchy {
            run_hierarchy_sync(&mut hierarchy_registry, &mut clients).await?;
        }
    } else {
        // Continuous sync mode
        let mut timer = interval(Duration::from_secs(args.interval));
        timer.tick().await; // Skip first immediate tick

        loop {
            if sync_creatives {
                run_sync(&mut registry, &mut asset_registry, &mut clients).await?;
            }
            if sync_metrics {
                run_metrics_sync(&mut metrics_registry, &mut clients, start_date, end_date).await?;
            }
            if sync_hierarchy {
                run_hierarchy_sync(&mut hierarchy_registry, &mut clients).await?;
            }
            timer.tick().await;
        }
    }

    Ok(())
}

async fn run_sync(
    registry: &mut CreativeRegistry,
    asset_registry: &mut AssetRegistry,
    clients: &mut [Box<dyn ApiClient>],
) -> Result<()> {
    info!("Starting creative sync...");

    let mut total_fetched = 0;
    let mut total_errors = 0;

    for client in clients.iter_mut() {
        info!("Syncing from {}...", client.network_name());

        match client.fetch_creatives().await {
            Ok(ApiSyncResult { creatives, .. }) => {
                info!(
                    "Fetched {} creatives from {}",
                    creatives.len(),
                    client.network_name()
                );
                total_fetched += creatives.len();

                // Explode creatives into the asset dimension before ownership
                // of each creative moves into the creative registry
                let new_assets = asset_registry.sync_from_creatives(creatives.clone()).await;
                if new_assets > 0 {
                    info!(
                        "Asset dimension: {} new assets from {}",
                        new_assets,
                        client.network_name()
                    );
                }

                // Add to registry
                for creative in creatives {
                    registry.add_creative(creative).await?;
                }
            }
            Err(e) => {
                error!("Failed to fetch from {}: {}", client.network_name(), e);
                total_errors += 1;
            }
        }
    }

    // Persist registry to S3
    info!("Persisting registry to S3...");
    registry.persist().await?;

    // Persist the asset dimension, partitioned by (network, type). No-op
    // when empty: store_assets writes one file per non-empty partition.
    info!("Persisting asset dimension to S3...");
    asset_registry.persist().await?;

    info!(
        "Sync complete: {} creatives fetched, {} assets in dimension, {} errors",
        total_fetched,
        asset_registry.len(),
        total_errors
    );

    Ok(())
}

async fn run_metrics_sync(
    registry: &mut MetricsRegistry,
    clients: &mut [Box<dyn ApiClient>],
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
) -> Result<()> {
    info!(
        "Starting metrics sync from {} to {}...",
        start_date, end_date
    );

    let mut total_fetched = 0;
    let mut total_errors = 0;

    for client in clients.iter_mut() {
        info!("Fetching metrics from {}...", client.network_name());

        match client.fetch_metrics(start_date, end_date).await {
            Ok(MetricsSyncResult { metrics, .. }) => {
                info!(
                    "Fetched {} metrics from {}",
                    metrics.len(),
                    client.network_name()
                );
                total_fetched += metrics.len();

                // Add to registry
                for metric in metrics {
                    registry.add_metrics(metric).await?;
                }
            }
            Err(e) => {
                error!(
                    "Failed to fetch metrics from {}: {}",
                    client.network_name(),
                    e
                );
                total_errors += 1;
            }
        }
    }

    // Persist metrics to S3
    info!("Persisting metrics to S3...");
    registry.persist().await?;

    info!(
        "Metrics sync complete: {} metrics fetched, {} errors",
        total_fetched, total_errors
    );

    Ok(())
}

async fn run_hierarchy_sync(
    registry: &mut HierarchyRegistry,
    clients: &mut [Box<dyn ApiClient>],
) -> Result<()> {
    info!("Starting hierarchy sync...");

    let mut total_fetched = 0;
    let mut total_errors = 0;

    for client in clients.iter_mut() {
        info!("Fetching hierarchy from {}...", client.network_name());

        match client.fetch_hierarchy().await {
            Ok(HierarchySyncResult { hierarchies, .. }) => {
                info!(
                    "Fetched {} hierarchies from {}",
                    hierarchies.len(),
                    client.network_name()
                );
                total_fetched += hierarchies.len();

                // Add to registry and persist each hierarchy
                for hierarchy in hierarchies {
                    registry.add_hierarchy(hierarchy.clone()).await?;
                    registry.persist_hierarchy(&hierarchy).await?;
                }
            }
            Err(e) => {
                error!(
                    "Failed to fetch hierarchy from {}: {}",
                    client.network_name(),
                    e
                );
                total_errors += 1;
            }
        }
    }

    info!(
        "Hierarchy sync complete: {} hierarchies fetched, {} errors",
        total_fetched, total_errors
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::creative::CreativeMetadata;
    use s3_store::{AssetStore, MockCreativeStore};

    /// In-memory asset store capturing what run_sync persists
    ///
    /// Clone shares the backing buffer, so a clone observes the same
    /// persisted rows — the test relies on this to snapshot after run_sync.
    #[derive(Clone)]
    struct MemoryAssetStore {
        assets: std::sync::Arc<tokio::sync::RwLock<Vec<assets::AssetRecord>>>,
    }

    impl MemoryAssetStore {
        fn new() -> Self {
            Self {
                assets: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            }
        }

        async fn snapshot(&self) -> Vec<assets::AssetRecord> {
            self.assets.read().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl AssetStore for MemoryAssetStore {
        async fn store_assets(&self, assets: Vec<assets::AssetRecord>) -> anyhow::Result<()> {
            *self.assets.write().await = assets;
            Ok(())
        }

        async fn load_assets(&self) -> anyhow::Result<Vec<assets::AssetRecord>> {
            Ok(self.assets.read().await.clone())
        }
    }

    /// In-memory asset store capturing the partitioned Parquet objects
    ///
    /// Serializes exactly the way S3CreativeStore does — through
    /// `assets_partition_files` — so tests assert the real
    /// network=<network>/type=<type>/ object layout; only the S3 upload is
    /// stubbed out.
    #[derive(Clone)]
    struct PartitionedCaptureStore {
        objects: std::sync::Arc<tokio::sync::RwLock<Vec<(String, Vec<u8>)>>>,
    }

    impl PartitionedCaptureStore {
        fn new() -> Self {
            Self {
                objects: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
            }
        }

        async fn objects(&self) -> Vec<(String, Vec<u8>)> {
            self.objects.read().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl AssetStore for PartitionedCaptureStore {
        async fn store_assets(&self, assets: Vec<assets::AssetRecord>) -> anyhow::Result<()> {
            *self.objects.write().await = s3_store::assets_partition_files("trace-events", assets)?;
            Ok(())
        }

        async fn load_assets(&self) -> anyhow::Result<Vec<assets::AssetRecord>> {
            let objects = self.objects.read().await;
            let mut assets = Vec::new();
            for (_, data) in objects.iter() {
                assets.extend(s3_store::parquet_to_assets(data)?);
            }
            Ok(assets)
        }
    }

    /// Stub API client returning canned creatives for its network, or a
    /// fetch failure when built with [`StubClient::failing`] — drives
    /// run_sync deterministically without network access.
    struct StubClient {
        network: String,
        creatives: Vec<CreativeMetadata>,
        fail_fetch: bool,
    }

    impl StubClient {
        fn new(network: &str, creatives: Vec<CreativeMetadata>) -> Self {
            Self {
                network: network.to_string(),
                creatives,
                fail_fetch: false,
            }
        }

        fn failing(network: &str) -> Self {
            Self {
                network: network.to_string(),
                creatives: vec![],
                fail_fetch: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl ApiClient for StubClient {
        async fn fetch_creatives(&mut self) -> Result<ApiSyncResult> {
            if self.fail_fetch {
                return Err(anyhow::anyhow!("{} API unavailable", self.network));
            }
            Ok(ApiSyncResult {
                creatives: self.creatives.clone(),
                next_page_token: None,
            })
        }

        async fn fetch_metrics(
            &mut self,
            _start_date: chrono::NaiveDate,
            _end_date: chrono::NaiveDate,
        ) -> Result<MetricsSyncResult> {
            Ok(MetricsSyncResult {
                metrics: vec![],
                next_page_token: None,
            })
        }

        async fn fetch_hierarchy(&mut self) -> Result<HierarchySyncResult> {
            Ok(HierarchySyncResult {
                hierarchies: vec![],
                next_page_token: None,
            })
        }

        fn network_name(&self) -> &str {
            &self.network
        }
    }

    /// A creative fixture with the full metadata a network API returns
    fn creative(
        network: &str,
        creative_id: &str,
        headline: Option<&str>,
        image: Option<&str>,
        landing_page: Option<&str>,
    ) -> CreativeMetadata {
        CreativeMetadata {
            network: network.to_string(),
            campaign_id: Some(format!("camp-{creative_id}")),
            campaign_name: Some(format!("Campaign {creative_id}")),
            creative_id: Some(creative_id.to_string()),
            headline: headline.map(|s| s.to_string()),
            image_url: image.map(|s| s.to_string()),
            landing_page_url: landing_page.map(|s| s.to_string()),
            item_id: Some(format!("item-{creative_id}")),
            synced_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_run_sync_feeds_and_persists_asset_dimension() {
        let mut creative_registry = CreativeRegistry::new(MockCreativeStore::new());
        let asset_store = MemoryAssetStore::new();
        let mut asset_registry = AssetRegistry::new(asset_store.clone());
        let mut clients: Vec<Box<dyn ApiClient>> = vec![Box::new(api_client::DemoClient::new())];

        run_sync(&mut creative_registry, &mut asset_registry, &mut clients)
            .await
            .unwrap();

        // Demo creatives carry headline + image + landing page, so the
        // dimension must have been fed — this is the wiring that turns a
        // creative sync into an assets table
        assert!(
            !asset_registry.is_empty(),
            "run_sync must populate the asset dimension from synced creatives"
        );

        let persisted = asset_store.snapshot().await;
        assert!(
            !persisted.is_empty(),
            "run_sync must persist the asset dimension"
        );
        assert_eq!(persisted.len(), asset_registry.len());

        // Every documented asset type is represented
        let types: Vec<&str> = persisted.iter().map(|a| a.asset_type.as_str()).collect();
        assert!(types.contains(&assets::TYPE_HEADLINE));
        assert!(types.contains(&assets::TYPE_IMAGE));
        assert!(types.contains(&assets::TYPE_LANDING_PAGE));
    }

    /// The full fetch -> explode -> partitioned write path: stubbed network
    /// clients feed run_sync, and the sync run produces real Parquet objects
    /// under the network=<network>/type=<type>/ layout.
    #[tokio::test]
    async fn test_run_sync_writes_partitioned_parquet_end_to_end() {
        let mut creative_registry = CreativeRegistry::new(MockCreativeStore::new());
        let asset_store = PartitionedCaptureStore::new();
        let mut asset_registry = AssetRegistry::new(asset_store.clone());

        let mut clients: Vec<Box<dyn ApiClient>> = vec![
            Box::new(StubClient::new(
                "taboola",
                vec![
                    creative(
                        "taboola",
                        "t-1",
                        Some("Taboola Headline"),
                        Some("https://img/taboola.jpg"),
                        Some("https://land/taboola"),
                    ),
                    creative(
                        "taboola",
                        "t-2",
                        Some("Second Headline"),
                        // Same image as t-1: dedups to one image asset
                        Some("https://img/taboola.jpg"),
                        None,
                    ),
                ],
            )),
            Box::new(StubClient::new(
                "mgid",
                vec![creative(
                    "mgid",
                    "m-1",
                    Some("MGID Headline"),
                    Some("https://img/mgid.jpg"),
                    Some("https://land/mgid"),
                )],
            )),
        ];

        run_sync(&mut creative_registry, &mut asset_registry, &mut clients)
            .await
            .unwrap();

        // One Parquet object per (network, type) partition, Hive-style
        let objects = asset_store.objects().await;
        let mut keys: Vec<&str> = objects.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "trace-events/assets/network=mgid/type=headline/assets.parquet",
                "trace-events/assets/network=mgid/type=image/assets.parquet",
                "trace-events/assets/network=mgid/type=landing_page/assets.parquet",
                "trace-events/assets/network=taboola/type=headline/assets.parquet",
                "trace-events/assets/network=taboola/type=image/assets.parquet",
                "trace-events/assets/network=taboola/type=landing_page/assets.parquet",
            ]
        );

        // The taboola headline partition holds both headlines (Parquet row
        // order follows registry map iteration, so compare sorted)
        let taboola_headlines = objects
            .iter()
            .find(|(k, _)| k.contains("network=taboola/type=headline"))
            .map(|(_, data)| s3_store::parquet_to_assets(data).unwrap())
            .unwrap();
        assert_eq!(taboola_headlines.len(), 2);
        let mut contents: Vec<&str> = taboola_headlines
            .iter()
            .map(|a| a.content.as_str())
            .collect();
        contents.sort_unstable();
        assert_eq!(contents, vec!["Second Headline", "Taboola Headline"]);

        // Join keys survive the roundtrip into Parquet
        let mgid_headlines = objects
            .iter()
            .find(|(k, _)| k.contains("network=mgid/type=headline"))
            .map(|(_, data)| s3_store::parquet_to_assets(data).unwrap())
            .unwrap();
        assert_eq!(mgid_headlines.len(), 1);
        let mgid_headline = &mgid_headlines[0];
        assert_eq!(mgid_headline.asset_id, "mgid:headline:MGID Headline");
        assert_eq!(mgid_headline.creative_id.as_deref(), Some("m-1"));
        assert_eq!(mgid_headline.campaign_id.as_deref(), Some("camp-m-1"));
        assert_eq!(mgid_headline.campaign_name.as_deref(), Some("Campaign m-1"));
        assert_eq!(mgid_headline.item_id.as_deref(), Some("item-m-1"));

        // The shared image deduped to one row
        let taboola_images = objects
            .iter()
            .find(|(k, _)| k.contains("network=taboola/type=image"))
            .map(|(_, data)| s3_store::parquet_to_assets(data).unwrap())
            .unwrap();
        assert_eq!(taboola_images.len(), 1);

        // A fresh registry reloads every row across all six partitions —
        // the first_seen-survival path depends on reading back what a
        // sync run wrote
        let mut reloaded = AssetRegistry::new(asset_store);
        assert_eq!(reloaded.load().await.unwrap(), 7);
    }

    /// A creative-fetch failure on one network is logged and that network
    /// is skipped — the rest of the run completes.
    #[tokio::test]
    async fn test_run_sync_skips_failing_network_and_syncs_the_rest() {
        let mut creative_registry = CreativeRegistry::new(MockCreativeStore::new());
        let asset_store = PartitionedCaptureStore::new();
        let mut asset_registry = AssetRegistry::new(asset_store.clone());

        // The failing client comes FIRST: if run_sync aborted on a fetch
        // failure, taboola behind it would never be synced
        let mut clients: Vec<Box<dyn ApiClient>> = vec![
            Box::new(StubClient::failing("outbrain")),
            Box::new(StubClient::new(
                "taboola",
                vec![creative(
                    "taboola",
                    "t-1",
                    Some("Taboola Headline"),
                    Some("https://img/taboola.jpg"),
                    Some("https://land/taboola"),
                )],
            )),
        ];

        run_sync(&mut creative_registry, &mut asset_registry, &mut clients)
            .await
            .expect("a per-network fetch failure must not abort the run");

        // The healthy network's creatives and assets made it through
        assert_eq!(
            creative_registry
                .get_network_creatives("taboola")
                .await
                .len(),
            1
        );
        assert!(creative_registry
            .get_network_creatives("outbrain")
            .await
            .is_empty());

        let keys: Vec<String> = asset_store
            .objects()
            .await
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(keys.len(), 3, "taboola writes its three partitions");
        assert!(
            keys.iter().all(|k| k.contains("network=taboola/")),
            "the failed network contributes no partitions: {keys:?}"
        );
        assert!(!asset_registry.is_empty());
    }
}
