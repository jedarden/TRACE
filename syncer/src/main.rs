mod api_client;
mod creative;
mod registry;
mod s3_store;

use anyhow::Result;
use clap::Parser;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api_client::{ApiClient, ApiSyncResult};
use registry::CreativeRegistry;
use s3_store::S3CreativeStore;

/// TRACE Creative Syncer
///
/// Fetches creative metadata from ad network APIs and stores it for attribution.
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
    let mut registry = CreativeRegistry::new(store);

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

    if args.once {
        // Run once and exit
        run_sync(&mut registry, &mut clients).await?;
    } else {
        // Continuous sync mode
        let mut timer = interval(Duration::from_secs(args.interval));
        timer.tick().await; // Skip first immediate tick

        loop {
            run_sync(&mut registry, &mut clients).await?;
            timer.tick().await;
        }
    }

    Ok(())
}

async fn run_sync(
    registry: &mut CreativeRegistry,
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

    info!(
        "Sync complete: {} creatives fetched, {} errors",
        total_fetched, total_errors
    );

    Ok(())
}
