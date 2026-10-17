//! S3 storage for creative metadata

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Region, Client};
use aws_smithy_types::byte_stream::ByteStream;
use chrono::DateTime;
use parquet::arrow::arrow_writer::ArrowWriter;

use crate::assets::AssetRecord;
use crate::campaigns::CampaignRecord;
use crate::creative::{CreativeMetadata, PerformanceMetrics};
use crate::hierarchy::AccountHierarchy;

/// Trait for creative metadata storage
#[async_trait]
pub trait CreativeStore: Send + Sync {
    /// Store creative metadata
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()>;

    /// Load creative metadata
    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>>;
}

/// Trait for performance metrics storage
#[async_trait]
pub trait MetricsStore: Send + Sync {
    /// Store performance metrics
    async fn store_metrics(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<()>;

    /// Load performance metrics for a date range
    async fn load_metrics(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<PerformanceMetrics>>;
}

/// Trait for asset dimension storage
///
/// Assets are persisted partitioned by (network, type) — the layout
/// documented for the trace.assets Iceberg table in
/// analytics/schemas/assets_iceberg.sql.
#[async_trait]
pub trait AssetStore: Send + Sync {
    /// Store asset dimension rows, one Parquet file per (network, type) partition
    async fn store_assets(&self, assets: Vec<AssetRecord>) -> anyhow::Result<()>;

    /// Load all asset dimension rows across every partition
    async fn load_assets(&self) -> anyhow::Result<Vec<AssetRecord>>;
}

/// Storage for the current campaign dimension, partitioned by network.
#[async_trait]
pub trait CampaignStore: Send + Sync {
    async fn store_campaigns(&self, campaigns: Vec<CampaignRecord>) -> anyhow::Result<()>;
    async fn load_campaigns(&self) -> anyhow::Result<Vec<CampaignRecord>>;
    async fn publish_campaign_revisions(
        &self,
        revisions: Vec<CampaignRecord>,
        config: &trace_iceberg::publisher::PublisherConfig,
    ) -> anyhow::Result<()>;
}

/// Trait for hierarchy storage
#[async_trait]
pub trait HierarchyStore: Send + Sync {
    /// Store account hierarchy
    async fn store_hierarchy(&self, hierarchy: &AccountHierarchy) -> anyhow::Result<()>;

    /// Load account hierarchy for a specific network and account
    async fn load_hierarchy(
        &self,
        network: &str,
        account_id: &str,
    ) -> anyhow::Result<Option<AccountHierarchy>>;

    /// List all available hierarchies
    async fn list_hierarchies(&self) -> anyhow::Result<Vec<(String, String)>>;
}

/// Get the S3 key for an assets partition (Hive-style, matching the
/// documented trace.assets layout: network=<network>/type=<type>/)
pub(crate) fn assets_partition_key(key_prefix: &str, network: &str, asset_type: &str) -> String {
    format!(
        "{}/assets/network={}/type={}/assets.parquet",
        key_prefix, network, asset_type
    )
}

/// Partition assets by (network, type) and serialize each partition to
/// Parquet, returning (S3 key, Parquet bytes) pairs — the exact objects
/// `store_assets` uploads. One file per non-empty partition, laid out
/// Hive-style as network=<network>/type=<type>/.
pub(crate) fn assets_partition_files(
    key_prefix: &str,
    assets: Vec<AssetRecord>,
) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let mut by_partition: std::collections::HashMap<(String, String), Vec<AssetRecord>> =
        std::collections::HashMap::new();

    for asset in assets {
        by_partition
            .entry((asset.network.clone(), asset.asset_type.clone()))
            .or_default()
            .push(asset);
    }

    let mut files = Vec::with_capacity(by_partition.len());
    for ((network, asset_type), partition_assets) in by_partition {
        let key = assets_partition_key(key_prefix, &network, &asset_type);
        let data = assets_to_parquet(partition_assets)?;
        files.push((key, data));
    }

    Ok(files)
}

pub(crate) fn campaigns_partition_key(key_prefix: &str, network: &str) -> String {
    format!(
        "{}/iceberg/campaigns/data/network={}/campaigns.parquet",
        key_prefix, network
    )
}

pub(crate) fn campaigns_partition_files(
    key_prefix: &str,
    campaigns: Vec<CampaignRecord>,
) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let mut by_network: std::collections::HashMap<String, Vec<CampaignRecord>> =
        std::collections::HashMap::new();
    for campaign in campaigns {
        by_network
            .entry(campaign.network.clone())
            .or_default()
            .push(campaign);
    }
    by_network
        .into_iter()
        .map(|(network, rows)| {
            Ok((
                campaigns_partition_key(key_prefix, &network),
                campaigns_to_parquet(rows)?,
            ))
        })
        .collect()
}

/// Immutable objects only: never use the mutable `campaigns.parquet` key in
/// an Iceberg manifest. Each invocation gets fresh UUID names per network.
pub(crate) fn campaign_revision_files(
    key_prefix: &str,
    revisions: Vec<CampaignRecord>,
) -> anyhow::Result<Vec<(String, Vec<u8>, String)>> {
    let mut by_network: std::collections::HashMap<String, Vec<CampaignRecord>> =
        std::collections::HashMap::new();
    for revision in revisions {
        by_network
            .entry(revision.network.clone())
            .or_default()
            .push(revision);
    }
    by_network
        .into_iter()
        .map(|(network, rows)| {
            let key = format!(
                "{}/iceberg/campaigns/data/network={}/revision-{}.parquet",
                key_prefix,
                network,
                uuid::Uuid::new_v4()
            );
            Ok((key, campaigns_to_parquet(rows)?, network))
        })
        .collect()
}

fn campaign_revision_object(
    config: &trace_iceberg::publisher::PublisherConfig,
    key_prefix: &str,
    key: &str,
    bytes: &[u8],
    network: &str,
) -> anyhow::Result<trace_iceberg::publisher::ParquetObject> {
    let (_, record_count) = trace_iceberg::types::schema_from_parquet(bytes)?;
    let relative_key = key
        .strip_prefix(&format!("{}/", key_prefix))
        .ok_or_else(|| anyhow::anyhow!("Campaign revision key is outside S3 prefix"))?;
    anyhow::ensure!(
        relative_key.starts_with(&format!(
            "iceberg/campaigns/data/network={network}/revision-"
        )) && relative_key.ends_with(".parquet"),
        "Iceberg campaign object must reference an immutable network revision"
    );
    Ok(trace_iceberg::publisher::ParquetObject {
        location: config.object_uri(relative_key),
        size_bytes: bytes.len() as u64,
        record_count: record_count as u64,
        partition_day: None,
        partition_network: Some(network.to_owned()),
    })
}

pub(crate) fn campaigns_to_parquet(campaigns: Vec<CampaignRecord>) -> anyhow::Result<Vec<u8>> {
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("network", DataType::Utf8, false),
        Field::new("campaign_name", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("account_id", DataType::Utf8, false),
        Field::new("account_name", DataType::Utf8, true),
        Field::new(
            "synced_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.campaign_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.network.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.campaign_name.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.status.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.account_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                campaigns
                    .iter()
                    .map(|c| c.account_name.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(
                campaigns
                    .iter()
                    .map(|c| c.synced_at.timestamp_millis())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

pub(crate) fn parquet_to_campaigns(data: &[u8]) -> anyhow::Result<Vec<CampaignRecord>> {
    use arrow::array::{Array, StringArray, TimestampMillisecondArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let string_col = |index: usize| -> anyhow::Result<&StringArray> {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Campaign Parquet column {} must be UTF-8", index))
        };
        let ids = string_col(0)?;
        let networks = string_col(1)?;
        let names = string_col(2)?;
        let statuses = string_col(3)?;
        let account_ids = string_col(4)?;
        let account_names = string_col(5)?;
        let synced_at = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Campaign Parquet synced_at must be a timestamp"))?;
        for i in 0..batch.num_rows() {
            rows.push(CampaignRecord {
                campaign_id: ids.value(i).to_owned(),
                network: networks.value(i).to_owned(),
                campaign_name: names.is_valid(i).then(|| names.value(i).to_owned()),
                status: statuses.is_valid(i).then(|| statuses.value(i).to_owned()),
                account_id: account_ids.value(i).to_owned(),
                account_name: account_names
                    .is_valid(i)
                    .then(|| account_names.value(i).to_owned()),
                synced_at: DateTime::from_timestamp_millis(synced_at.value(i))
                    .ok_or_else(|| anyhow::anyhow!("Campaign synced_at is out of range"))?,
            });
        }
    }
    Ok(rows)
}

/// Convert asset dimension rows to Parquet format
pub(crate) fn assets_to_parquet(assets: Vec<AssetRecord>) -> anyhow::Result<Vec<u8>> {
    use arrow::array::{StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let asset_ids: Vec<String> = assets.iter().map(|a| a.asset_id.clone()).collect();
    let networks: Vec<String> = assets.iter().map(|a| a.network.clone()).collect();
    let asset_types: Vec<String> = assets.iter().map(|a| a.asset_type.clone()).collect();
    let contents: Vec<String> = assets.iter().map(|a| a.content.clone()).collect();
    let creative_ids: Vec<Option<String>> = assets.iter().map(|a| a.creative_id.clone()).collect();
    let campaign_ids: Vec<Option<String>> = assets.iter().map(|a| a.campaign_id.clone()).collect();
    let campaign_names: Vec<Option<String>> =
        assets.iter().map(|a| a.campaign_name.clone()).collect();
    let item_ids: Vec<Option<String>> = assets.iter().map(|a| a.item_id.clone()).collect();
    let first_seen: Vec<i64> = assets
        .iter()
        .map(|a| a.first_seen.timestamp_millis())
        .collect();
    let last_seen: Vec<i64> = assets
        .iter()
        .map(|a| a.last_seen.timestamp_millis())
        .collect();
    let synced_at: Vec<i64> = assets
        .iter()
        .map(|a| a.synced_at.timestamp_millis())
        .collect();

    let schema = Schema::new(vec![
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("network", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("creative_id", DataType::Utf8, true),
        Field::new("campaign_id", DataType::Utf8, true),
        Field::new("campaign_name", DataType::Utf8, true),
        Field::new("item_id", DataType::Utf8, true),
        Field::new(
            "first_seen",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "last_seen",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "synced_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(asset_ids)),
            Arc::new(StringArray::from(networks)),
            Arc::new(StringArray::from(asset_types)),
            Arc::new(StringArray::from(contents)),
            Arc::new(StringArray::from(creative_ids)),
            Arc::new(StringArray::from(campaign_ids)),
            Arc::new(StringArray::from(campaign_names)),
            Arc::new(StringArray::from(item_ids)),
            Arc::new(TimestampMillisecondArray::from(first_seen)),
            Arc::new(TimestampMillisecondArray::from(last_seen)),
            Arc::new(TimestampMillisecondArray::from(synced_at)),
        ],
    )?;

    let mut buffer = Vec::new();
    let props = parquet::file::properties::WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer)
}

/// Convert Parquet data to asset dimension rows
pub(crate) fn parquet_to_assets(data: &[u8]) -> anyhow::Result<Vec<AssetRecord>> {
    use arrow::array::{Array, StringArray, TimestampMillisecondArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = Bytes::from(data.to_vec());
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

    let mut assets = Vec::new();

    for batch in reader {
        let batch = batch?;

        let asset_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast asset_id column"))?;

        let networks = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast network column"))?;

        let asset_types = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast type column"))?;

        let contents = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast content column"))?;

        let creative_ids = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast creative_id column"))?;

        let campaign_ids = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_id column"))?;

        let campaign_names = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_name column"))?;

        let item_ids = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast item_id column"))?;

        let first_seen = batch
            .column(8)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast first_seen column"))?;

        let last_seen = batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast last_seen column"))?;

        let synced_at = batch
            .column(10)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast synced_at column"))?;

        for i in 0..batch.num_rows() {
            assets.push(AssetRecord {
                asset_id: asset_ids.value(i).to_string(),
                network: networks.value(i).to_string(),
                asset_type: asset_types.value(i).to_string(),
                content: contents.value(i).to_string(),
                creative_id: creative_ids
                    .is_valid(i)
                    .then(|| creative_ids.value(i).to_string()),
                campaign_id: campaign_ids
                    .is_valid(i)
                    .then(|| campaign_ids.value(i).to_string()),
                campaign_name: campaign_names
                    .is_valid(i)
                    .then(|| campaign_names.value(i).to_string()),
                item_id: item_ids.is_valid(i).then(|| item_ids.value(i).to_string()),
                first_seen: DateTime::from_timestamp_millis(first_seen.value(i)).unwrap(),
                last_seen: DateTime::from_timestamp_millis(last_seen.value(i)).unwrap(),
                synced_at: DateTime::from_timestamp_millis(synced_at.value(i)).unwrap(),
            });
        }
    }

    Ok(assets)
}

/// S3-backed creative store
#[derive(Clone)]
pub struct S3CreativeStore {
    client: Client,
    bucket: String,
    key_prefix: String,
}

impl S3CreativeStore {
    /// Create a new S3 creative store
    pub async fn new(bucket: String, region: String, key_prefix: String) -> anyhow::Result<Self> {
        let region = Region::new(region);
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let client = Client::new(&aws_config);

        Ok(Self {
            client,
            bucket,
            key_prefix,
        })
    }

    /// Get the S3 key for the creative registry
    fn registry_key(&self) -> String {
        format!("{}/creative-registry.parquet", self.key_prefix)
    }

    /// Get the S3 key for performance metrics for a specific date
    fn metrics_key(&self, date: chrono::NaiveDate) -> String {
        format!("{}/metrics/metrics-{}.parquet", self.key_prefix, date)
    }

    /// Get the S3 key for hierarchy data
    fn hierarchy_key(&self, network: &str, account_id: &str) -> String {
        format!(
            "{}/hierarchy/{}-{}.json",
            self.key_prefix, network, account_id
        )
    }

    /// Get the S3 prefix covering every assets partition
    fn assets_prefix(&self) -> String {
        format!("{}/assets/", self.key_prefix)
    }

    fn campaigns_prefix(&self) -> String {
        format!("{}/iceberg/campaigns/data/", self.key_prefix)
    }

    /// Convert performance metrics to Parquet format
    fn metrics_to_parquet(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<Vec<u8>> {
        use arrow::array::{Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let networks: Vec<String> = metrics.iter().map(|m| m.network.clone()).collect();
        let campaign_ids: Vec<String> = metrics.iter().map(|m| m.campaign_id.clone()).collect();
        let campaign_names: Vec<Option<String>> =
            metrics.iter().map(|m| m.campaign_name.clone()).collect();
        let creative_ids: Vec<Option<String>> =
            metrics.iter().map(|m| m.creative_id.clone()).collect();
        let dates: Vec<i32> = metrics
            .iter()
            .map(|m| m.date.format("%Y%m%d").to_string().parse::<i32>().unwrap())
            .collect();
        let impressions: Vec<i64> = metrics.iter().map(|m| m.impressions).collect();
        let clicks: Vec<i64> = metrics.iter().map(|m| m.clicks).collect();
        let spend_micros: Vec<i64> = metrics.iter().map(|m| m.spend_micros).collect();
        let conversions: Vec<Option<i64>> = metrics.iter().map(|m| m.conversions).collect();
        let ctr_bps: Vec<Option<i32>> = metrics.iter().map(|m| m.ctr_bps).collect();
        let cpc_micros: Vec<Option<i64>> = metrics.iter().map(|m| m.cpc_micros).collect();
        let cpm_micros: Vec<Option<i64>> = metrics.iter().map(|m| m.cpm_micros).collect();
        let synced_at: Vec<i64> = metrics
            .iter()
            .map(|m| m.synced_at.timestamp_millis())
            .collect();

        let schema = Schema::new(vec![
            Field::new("network", DataType::Utf8, false),
            Field::new("campaign_id", DataType::Utf8, false),
            Field::new("campaign_name", DataType::Utf8, true),
            Field::new("creative_id", DataType::Utf8, true),
            Field::new("date", DataType::Int32, false),
            Field::new("impressions", DataType::Int64, false),
            Field::new("clicks", DataType::Int64, false),
            Field::new("spend_micros", DataType::Int64, false),
            Field::new("conversions", DataType::Int64, true),
            Field::new("ctr_bps", DataType::Int32, true),
            Field::new("cpc_micros", DataType::Int64, true),
            Field::new("cpm_micros", DataType::Int64, true),
            Field::new(
                "synced_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(networks)),
                Arc::new(StringArray::from(campaign_ids)),
                Arc::new(StringArray::from(campaign_names)),
                Arc::new(StringArray::from(creative_ids)),
                Arc::new(Int32Array::from(dates)),
                Arc::new(Int64Array::from(impressions)),
                Arc::new(Int64Array::from(clicks)),
                Arc::new(Int64Array::from(spend_micros)),
                Arc::new(Int64Array::from(conversions)),
                Arc::new(Int32Array::from(ctr_bps)),
                Arc::new(Int64Array::from(cpc_micros)),
                Arc::new(Int64Array::from(cpm_micros)),
                Arc::new(TimestampMillisecondArray::from(synced_at)),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(buffer)
    }

    /// Convert Parquet data to performance metrics
    fn parquet_to_metrics(&self, data: &[u8]) -> anyhow::Result<Vec<PerformanceMetrics>> {
        use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::from(data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let mut metrics = Vec::new();

        for batch in reader {
            let batch = batch?;

            let networks = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast network column"))?;

            let campaign_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_id column"))?;

            let campaign_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_name column"))?;

            let creative_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast creative_id column"))?;

            let dates = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast date column"))?;

            let impressions = batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast impressions column"))?;

            let clicks = batch
                .column(6)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast clicks column"))?;

            let spend_micros = batch
                .column(7)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast spend_micros column"))?;

            let conversions = batch
                .column(8)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast conversions column"))?;

            let ctr_bps = batch
                .column(9)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast ctr_bps column"))?;

            let cpc_micros = batch
                .column(10)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast cpc_micros column"))?;

            let cpm_micros = batch
                .column(11)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast cpm_micros column"))?;

            let synced_at = batch
                .column(12)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast synced_at column"))?;

            for i in 0..batch.num_rows() {
                let date_str = format!("{:08}", dates.value(i));
                let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")?;

                metrics.push(PerformanceMetrics {
                    network: networks.value(i).to_string(),
                    campaign_id: campaign_ids.value(i).to_string(),
                    campaign_name: campaign_names
                        .is_valid(i)
                        .then(|| campaign_names.value(i).to_string()),
                    creative_id: creative_ids
                        .is_valid(i)
                        .then(|| creative_ids.value(i).to_string()),
                    date,
                    impressions: impressions.value(i),
                    clicks: clicks.value(i),
                    spend_micros: spend_micros.value(i),
                    conversions: conversions.is_valid(i).then(|| conversions.value(i)),
                    ctr_bps: ctr_bps.is_valid(i).then(|| ctr_bps.value(i) as i32),
                    cpc_micros: cpc_micros.is_valid(i).then(|| cpc_micros.value(i)),
                    cpm_micros: cpm_micros.is_valid(i).then(|| cpm_micros.value(i)),
                    synced_at: DateTime::from_timestamp_millis(synced_at.value(i)).unwrap(),
                });
            }
        }

        Ok(metrics)
    }

    /// Convert creatives to Parquet format
    fn creatives_to_parquet(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<Vec<u8>> {
        use arrow::array::{StringArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let networks: Vec<String> = creatives.iter().map(|c| c.network.clone()).collect();
        let campaign_ids: Vec<Option<String>> =
            creatives.iter().map(|c| c.campaign_id.clone()).collect();
        let campaign_names: Vec<Option<String>> =
            creatives.iter().map(|c| c.campaign_name.clone()).collect();
        let creative_ids: Vec<Option<String>> =
            creatives.iter().map(|c| c.creative_id.clone()).collect();
        let headlines: Vec<Option<String>> = creatives.iter().map(|c| c.headline.clone()).collect();
        let image_urls: Vec<Option<String>> =
            creatives.iter().map(|c| c.image_url.clone()).collect();
        let landing_page_urls: Vec<Option<String>> = creatives
            .iter()
            .map(|c| c.landing_page_url.clone())
            .collect();
        let item_ids: Vec<Option<String>> = creatives.iter().map(|c| c.item_id.clone()).collect();
        let synced_at: Vec<i64> = creatives
            .iter()
            .map(|c| c.synced_at.timestamp_millis())
            .collect();

        let schema = Schema::new(vec![
            Field::new("network", DataType::Utf8, false),
            Field::new("campaign_id", DataType::Utf8, true),
            Field::new("campaign_name", DataType::Utf8, true),
            Field::new("creative_id", DataType::Utf8, true),
            Field::new("headline", DataType::Utf8, true),
            Field::new("image_url", DataType::Utf8, true),
            Field::new("landing_page_url", DataType::Utf8, true),
            Field::new("item_id", DataType::Utf8, true),
            Field::new(
                "synced_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(networks)),
                Arc::new(StringArray::from(campaign_ids)),
                Arc::new(StringArray::from(campaign_names)),
                Arc::new(StringArray::from(creative_ids)),
                Arc::new(StringArray::from(headlines)),
                Arc::new(StringArray::from(image_urls)),
                Arc::new(StringArray::from(landing_page_urls)),
                Arc::new(StringArray::from(item_ids)),
                Arc::new(TimestampMillisecondArray::from(synced_at)),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(buffer)
    }

    /// Convert Parquet data to creatives
    fn parquet_to_creatives(&self, data: &[u8]) -> anyhow::Result<Vec<CreativeMetadata>> {
        use arrow::array::{Array, StringArray, TimestampMillisecondArray};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = Bytes::from(data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let mut creatives = Vec::new();

        for batch in reader {
            let batch = batch?;

            let networks = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast network column"))?;

            let campaign_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_id column"))?;

            let campaign_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast campaign_name column"))?;

            let creative_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast creative_id column"))?;

            let headlines = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast headline column"))?;

            let image_urls = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast image_url column"))?;

            let landing_page_urls = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast landing_page_url column"))?;

            let item_ids = batch
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast item_id column"))?;

            let synced_at = batch
                .column(8)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast synced_at column"))?;

            for i in 0..batch.num_rows() {
                creatives.push(CreativeMetadata {
                    network: networks.value(i).to_string(),
                    campaign_id: campaign_ids
                        .is_valid(i)
                        .then(|| campaign_ids.value(i).to_string()),
                    campaign_name: campaign_names
                        .is_valid(i)
                        .then(|| campaign_names.value(i).to_string()),
                    creative_id: creative_ids
                        .is_valid(i)
                        .then(|| creative_ids.value(i).to_string()),
                    headline: headlines
                        .is_valid(i)
                        .then(|| headlines.value(i).to_string()),
                    image_url: image_urls
                        .is_valid(i)
                        .then(|| image_urls.value(i).to_string()),
                    landing_page_url: landing_page_urls
                        .is_valid(i)
                        .then(|| landing_page_urls.value(i).to_string()),
                    item_id: item_ids.is_valid(i).then(|| item_ids.value(i).to_string()),
                    synced_at: DateTime::from_timestamp_millis(synced_at.value(i)).unwrap(),
                });
            }
        }

        Ok(creatives)
    }
}

#[async_trait]
impl CreativeStore for S3CreativeStore {
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()> {
        let parquet_data = self.creatives_to_parquet(creatives)?;

        let key = self.registry_key();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(parquet_data))
            .send()
            .await?;

        tracing::info!("Stored creative registry to s3://{}/{}", self.bucket, key);
        Ok(())
    }

    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>> {
        let key = self.registry_key();

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;

        let data = response.body.collect().await?.into_bytes().to_vec();
        let creatives = self.parquet_to_creatives(&data)?;

        tracing::info!("Loaded {} creatives from s3://{}", creatives.len(), key);
        Ok(creatives)
    }
}

#[async_trait]
impl MetricsStore for S3CreativeStore {
    async fn store_metrics(&self, metrics: Vec<PerformanceMetrics>) -> anyhow::Result<()> {
        // Group metrics by date
        let mut metrics_by_date: std::collections::HashMap<
            chrono::NaiveDate,
            Vec<PerformanceMetrics>,
        > = std::collections::HashMap::new();

        for metric in metrics {
            metrics_by_date.entry(metric.date).or_default().push(metric);
        }

        // Store each date's metrics separately
        for (date, date_metrics) in metrics_by_date {
            let parquet_data = self.metrics_to_parquet(date_metrics)?;
            let key = self.metrics_key(date);

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(parquet_data))
                .send()
                .await?;

            tracing::info!("Stored metrics for {} to s3://{}", date, key);
        }

        Ok(())
    }

    async fn load_metrics(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> anyhow::Result<Vec<PerformanceMetrics>> {
        let mut all_metrics = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let key = self.metrics_key(current_date);

            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(response) => {
                    let data = response.body.collect().await?.into_bytes().to_vec();
                    let metrics = self.parquet_to_metrics(&data)?;
                    all_metrics.extend(metrics);
                }
                Err(e) => {
                    // Log warning but continue - metrics might not exist for all dates
                    tracing::warn!("Failed to load metrics for {}: {}", key, e);
                }
            }

            current_date = current_date.succ_opt().unwrap_or(current_date);
        }

        tracing::info!(
            "Loaded {} metrics from {} to {}",
            all_metrics.len(),
            start_date,
            end_date
        );
        Ok(all_metrics)
    }
}

#[async_trait]
impl AssetStore for S3CreativeStore {
    async fn store_assets(&self, assets: Vec<AssetRecord>) -> anyhow::Result<()> {
        // One Parquet file per (network, type) partition
        for (key, parquet_data) in assets_partition_files(&self.key_prefix, assets)? {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(parquet_data))
                .send()
                .await?;

            tracing::info!("Stored assets partition to s3://{}/{}", self.bucket, key);
        }

        Ok(())
    }

    async fn load_assets(&self) -> anyhow::Result<Vec<AssetRecord>> {
        let prefix = self.assets_prefix();

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await?;

        let mut assets = Vec::new();

        if let Some(objects) = response.contents {
            for obj in objects {
                let Some(key) = obj.key() else {
                    continue;
                };
                if !key.ends_with(".parquet") {
                    continue;
                }

                match self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await
                {
                    Ok(response) => {
                        let data = response.body.collect().await?.into_bytes().to_vec();
                        assets.extend(parquet_to_assets(&data)?);
                    }
                    Err(e) => {
                        // A partition file appearing or disappearing mid-list is
                        // not fatal - load what is readable
                        tracing::warn!("Failed to load assets partition {}: {}", key, e);
                    }
                }
            }
        }

        tracing::info!(
            "Loaded {} assets from s3://{}/{}",
            assets.len(),
            self.bucket,
            prefix
        );
        Ok(assets)
    }
}

#[async_trait]
impl CampaignStore for S3CreativeStore {
    async fn store_campaigns(&self, campaigns: Vec<CampaignRecord>) -> anyhow::Result<()> {
        for (key, parquet_data) in campaigns_partition_files(&self.key_prefix, campaigns)? {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(parquet_data))
                .send()
                .await?;
            tracing::info!("Stored campaigns partition to s3://{}/{}", self.bucket, key);
        }
        Ok(())
    }

    async fn load_campaigns(&self) -> anyhow::Result<Vec<CampaignRecord>> {
        let prefix = self.campaigns_prefix();
        let mut continuation_token: Option<String> = None;
        let mut campaigns = Vec::new();
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(token) = continuation_token.as_deref() {
                request = request.continuation_token(token);
            }
            let response = request.send().await?;
            for object in response.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if !key.ends_with("/campaigns.parquet") {
                    continue;
                }
                let object = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await?;
                let data = object.body.collect().await?.into_bytes();
                campaigns.extend(parquet_to_campaigns(&data)?);
            }
            continuation_token = response.next_continuation_token().map(str::to_owned);
            if continuation_token.is_none() {
                break;
            }
        }
        tracing::info!(
            "Loaded {} campaigns from s3://{}/{}",
            campaigns.len(),
            self.bucket,
            prefix
        );
        Ok(campaigns)
    }

    async fn publish_campaign_revisions(
        &self,
        revisions: Vec<CampaignRecord>,
        config: &trace_iceberg::publisher::PublisherConfig,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            config.bucket == self.bucket
                && config.prefix.trim_matches('/') == self.key_prefix.trim_matches('/'),
            "Iceberg publisher bucket/prefix must match campaign S3 store"
        );
        let files = campaign_revision_files(&self.key_prefix, revisions)?;
        if files.is_empty() {
            return Ok(());
        }
        let mut objects = Vec::with_capacity(files.len());
        for (key, bytes, network) in &files {
            let object = campaign_revision_object(config, &self.key_prefix, key, bytes, network)?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(bytes.clone()))
                .send()
                .await?;
            objects.push(object);
        }
        let published = trace_iceberg::publisher::append_parquet_files(
            config,
            "campaigns",
            &files[0].1,
            objects,
        )
        .await?;
        tracing::info!(
            "Published {} immutable campaign Parquet objects to trace.campaigns",
            published
        );
        Ok(())
    }
}

#[async_trait]
impl HierarchyStore for S3CreativeStore {
    async fn store_hierarchy(&self, hierarchy: &AccountHierarchy) -> anyhow::Result<()> {
        let json_data = serde_json::to_vec_pretty(hierarchy)?;
        let key = self.hierarchy_key(&hierarchy.network, &hierarchy.account_id);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(json_data))
            .send()
            .await?;

        tracing::info!(
            "Stored hierarchy for {}:{} to s3://{}",
            hierarchy.network,
            hierarchy.account_id,
            key
        );
        Ok(())
    }

    async fn load_hierarchy(
        &self,
        network: &str,
        account_id: &str,
    ) -> anyhow::Result<Option<AccountHierarchy>> {
        let key = self.hierarchy_key(network, account_id);

        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(response) => {
                let data = response.body.collect().await?.into_bytes().to_vec();
                let hierarchy = serde_json::from_slice(&data)?;
                tracing::info!(
                    "Loaded hierarchy for {}:{} from s3://{}",
                    network,
                    account_id,
                    key
                );
                Ok(Some(hierarchy))
            }
            Err(e) => {
                tracing::debug!("No hierarchy found for {}:{}: {}", network, account_id, e);
                Ok(None)
            }
        }
    }

    async fn list_hierarchies(&self) -> anyhow::Result<Vec<(String, String)>> {
        let prefix = format!("{}/hierarchy/", self.key_prefix);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .send()
            .await?;

        let mut hierarchies = Vec::new();

        if let Some(objects) = response.contents {
            for obj in objects {
                if let Some(key) = obj.key() {
                    // Extract network and account_id from key pattern: "prefix/hierarchy/network-account_id.json"
                    if let Some(filename) = key.strip_prefix(&prefix) {
                        if let Some(stem) = filename.strip_suffix(".json") {
                            if let Some((network, account_id)) = stem.split_once('-') {
                                hierarchies.push((network.to_string(), account_id.to_string()));
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(
            "Found {} hierarchies in s3://{}",
            hierarchies.len(),
            self.bucket
        );
        Ok(hierarchies)
    }
}

/// Mock store for testing
///
/// Clone shares the backing buffer, so a clone observes what the registry
/// persisted — run-loop tests rely on this to assert persisted contents.
#[derive(Clone)]
pub struct MockCreativeStore {
    data: std::sync::Arc<tokio::sync::RwLock<Vec<CreativeMetadata>>>,
}

impl MockCreativeStore {
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl CreativeStore for MockCreativeStore {
    async fn store(&self, creatives: Vec<CreativeMetadata>) -> anyhow::Result<()> {
        let mut data = self.data.write().await;
        *data = creatives;
        Ok(())
    }

    async fn load(&self) -> anyhow::Result<Vec<CreativeMetadata>> {
        let data = self.data.read().await;
        Ok(data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{SubsecRound, Utc};

    /// Helper function to create a test store without requiring AWS credentials
    /// The S3 client is created with a dummy config for testing parquet conversion only
    fn create_test_store() -> S3CreativeStore {
        // Create a minimal config that won't actually connect to S3
        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        S3CreativeStore {
            client: Client::from_conf(config),
            bucket: "test-bucket".to_string(),
            key_prefix: "test-prefix".to_string(),
        }
    }

    #[test]
    fn test_creatives_to_parquet() {
        let store = create_test_store();

        let creatives = vec![CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: Some("Test Campaign".to_string()),
            creative_id: Some("cr456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: Some("https://example.com/page".to_string()),
            item_id: Some("item789".to_string()),
            synced_at: Utc::now(),
        }];

        let result = store.creatives_to_parquet(creatives);
        assert!(result.is_ok());
        let parquet_data = result.unwrap();
        assert!(!parquet_data.is_empty());
    }

    #[test]
    fn test_parquet_roundtrip() {
        let store = create_test_store();

        let original = vec![CreativeMetadata {
            network: "taboola".to_string(),
            campaign_id: Some("camp123".to_string()),
            campaign_name: Some("Test Campaign".to_string()),
            creative_id: Some("cr456".to_string()),
            headline: Some("Test Headline".to_string()),
            image_url: Some("https://example.com/img.jpg".to_string()),
            landing_page_url: Some("https://example.com/page".to_string()),
            item_id: Some("item789".to_string()),
            synced_at: Utc::now(),
        }];

        let parquet_data = store.creatives_to_parquet(original.clone()).unwrap();
        let restored = store.parquet_to_creatives(&parquet_data).unwrap();

        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].network, original[0].network);
        assert_eq!(restored[0].campaign_id, original[0].campaign_id);
        assert_eq!(restored[0].headline, original[0].headline);
    }

    #[test]
    fn test_assets_partition_key_layout() {
        // Hive-style layout matching the documented trace.assets
        // partitioning: network=<network>/type=<type>/
        assert_eq!(
            assets_partition_key("test-prefix", "taboola", "headline"),
            "test-prefix/assets/network=taboola/type=headline/assets.parquet"
        );
        assert_eq!(create_test_store().assets_prefix(), "test-prefix/assets/");
    }

    #[test]
    fn campaign_parquet_is_partitioned_by_network_and_roundtrips_fields() {
        let now = Utc::now().trunc_subsecs(3);
        let row = |network: &str, name: Option<&str>| CampaignRecord {
            network: network.into(),
            campaign_id: "same-id".into(),
            campaign_name: name.map(str::to_owned),
            status: Some("active".into()),
            account_id: "account-1".into(),
            account_name: None,
            synced_at: now,
        };
        let files = campaigns_partition_files(
            "trace-events",
            vec![row("taboola", Some("Named")), row("mgid", None)],
        )
        .unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|(key, _)| key
            == "trace-events/iceberg/campaigns/data/network=taboola/campaigns.parquet"));
        assert!(files
            .iter()
            .any(|(key, _)| key
                == "trace-events/iceberg/campaigns/data/network=mgid/campaigns.parquet"));

        let taboola = files
            .iter()
            .find(|(key, _)| key.contains("network=taboola"))
            .map(|(_, data)| parquet_to_campaigns(data).unwrap())
            .unwrap();
        assert_eq!(taboola, vec![row("taboola", Some("Named"))]);

        let mgid = files
            .iter()
            .find(|(key, _)| key.contains("network=mgid"))
            .map(|(_, data)| parquet_to_campaigns(data).unwrap())
            .unwrap();
        assert_eq!(mgid, vec![row("mgid", None)]);
    }

    #[test]
    fn campaign_revision_keys_are_unique_and_never_use_mutable_key() {
        let row = CampaignRecord {
            network: "taboola".into(),
            campaign_id: "campaign-1".into(),
            campaign_name: Some("Campaign".into()),
            status: Some("active".into()),
            account_id: "account-1".into(),
            account_name: Some("Account".into()),
            synced_at: Utc::now().trunc_subsecs(3),
        };
        let first = campaign_revision_files("trace-events", vec![row.clone()]).unwrap();
        let second = campaign_revision_files("trace-events", vec![row]).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_ne!(first[0].0, second[0].0);
        assert!(first[0]
            .0
            .starts_with("trace-events/iceberg/campaigns/data/network=taboola/revision-"));
        assert!(first[0].0.ends_with(".parquet"));
        assert_ne!(
            first[0].0,
            campaigns_partition_key("trace-events", "taboola")
        );
        assert_eq!(first[0].2, "taboola");
        assert_eq!(
            trace_iceberg::types::schema_from_parquet(&first[0].1)
                .unwrap()
                .1,
            1
        );
        let config = trace_iceberg::publisher::PublisherConfig {
            catalog_uri: "http://catalog.invalid".into(),
            warehouse: "s3://test-bucket/trace-events/iceberg".into(),
            bucket: "test-bucket".into(),
            prefix: "trace-events".into(),
            region: "us-east-1".into(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };
        let object = campaign_revision_object(
            &config,
            "trace-events",
            &first[0].0,
            &first[0].1,
            &first[0].2,
        )
        .unwrap();
        assert_eq!(object.location, format!("s3://test-bucket/{}", first[0].0));
        assert_eq!(object.record_count, 1);
        assert_eq!(object.partition_network.as_deref(), Some("taboola"));
        assert!(object.partition_day.is_none());
        assert!(campaign_revision_object(
            &config,
            "trace-events",
            &campaigns_partition_key("trace-events", "taboola"),
            &first[0].1,
            "taboola"
        )
        .is_err());
    }

    #[test]
    fn test_assets_partition_files_one_parquet_per_network_and_type() {
        let now = Utc::now().trunc_subsecs(3);
        let asset = |network: &str, asset_type: &str, content: &str| AssetRecord {
            asset_id: format!("{}:{}:{}", network, asset_type, content),
            network: network.to_string(),
            asset_type: asset_type.to_string(),
            content: content.to_string(),
            creative_id: Some("cr456".to_string()),
            campaign_id: None,
            campaign_name: None,
            item_id: None,
            first_seen: now,
            last_seen: now,
            synced_at: now,
        };

        let files = assets_partition_files(
            "trace-events",
            vec![
                asset("taboola", "headline", "Headline A"),
                asset("taboola", "headline", "Headline B"),
                asset("taboola", "image", "https://img/a.jpg"),
                asset("mgid", "headline", "Headline A"),
            ],
        )
        .unwrap();

        let mut keys: Vec<&str> = files.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "trace-events/assets/network=mgid/type=headline/assets.parquet",
                "trace-events/assets/network=taboola/type=headline/assets.parquet",
                "trace-events/assets/network=taboola/type=image/assets.parquet",
            ]
        );

        // The taboola headline partition holds both of its rows
        let taboola_headlines = files
            .iter()
            .find(|(k, _)| k.contains("network=taboola/type=headline"))
            .map(|(_, data)| parquet_to_assets(data).unwrap())
            .unwrap();
        assert_eq!(taboola_headlines.len(), 2);
        assert!(taboola_headlines
            .iter()
            .all(|a| a.network == "taboola" && a.asset_type == "headline"));
    }

    #[test]
    fn test_assets_parquet_schema_matches_iceberg_ddl() {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // The trace.assets DDL (analytics/schemas/assets_iceberg.sql) declares
        // asset_id/network/type/content as STRING NOT NULL and first_seen/
        // last_seen as TIMESTAMP NOT NULL, PARTITIONED BY (network, type).
        // Those identity and observation columns are the syncer's half of the
        // table; the DDL's performance aggregates are computed downstream from
        // ad_events. The write/read roundtrip cannot catch a column drifting
        // from the DDL (both sides share one hand-written schema), so pin the
        // written Parquet schema to the DDL here.
        let now = Utc::now().trunc_subsecs(3);
        let parquet_data = assets_to_parquet(vec![AssetRecord {
            asset_id: "taboola:headline:Test Headline".to_string(),
            network: "taboola".to_string(),
            asset_type: "headline".to_string(),
            content: "Test Headline".to_string(),
            creative_id: None,
            campaign_id: None,
            campaign_name: None,
            item_id: None,
            first_seen: now,
            last_seen: now,
            synced_at: now,
        }])
        .unwrap();

        let schema = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_data))
            .unwrap()
            .schema()
            .clone();

        let column = |name: &str| {
            schema
                .field_with_name(name)
                .unwrap_or_else(|e| panic!("column {} missing from assets parquet: {}", name, e))
        };

        // Identity and partition columns, in DDL declaration order
        let strings = ["asset_id", "network", "type", "content"];
        for name in strings {
            let field = column(name);
            assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
            assert!(
                !field.is_nullable(),
                "{} must be NOT NULL per the DDL",
                name
            );
        }

        // Observation timestamps
        for name in ["first_seen", "last_seen"] {
            let field = column(name);
            assert_eq!(
                field.data_type(),
                &arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None
                )
            );
            assert!(
                !field.is_nullable(),
                "{} must be NOT NULL per the DDL",
                name
            );
        }
    }

    #[test]
    fn test_assets_parquet_roundtrip() {
        let store = create_test_store();

        // The Parquet schema stores timestamps at millisecond precision,
        // so the fixture must not carry sub-millisecond nanoseconds the
        // roundtrip is expected to preserve
        let synced_at = Utc::now().trunc_subsecs(3);
        let original = vec![
            AssetRecord {
                asset_id: "taboola:headline:Test Headline".to_string(),
                network: "taboola".to_string(),
                asset_type: "headline".to_string(),
                content: "Test Headline".to_string(),
                creative_id: Some("cr456".to_string()),
                campaign_id: Some("camp123".to_string()),
                campaign_name: Some("Test Campaign".to_string()),
                item_id: Some("item789".to_string()),
                first_seen: synced_at,
                last_seen: synced_at,
                synced_at,
            },
            AssetRecord {
                asset_id: "mgid:image:https://example.com/img.jpg".to_string(),
                network: "mgid".to_string(),
                asset_type: "image".to_string(),
                content: "https://example.com/img.jpg".to_string(),
                creative_id: None,
                campaign_id: None,
                campaign_name: None,
                item_id: None,
                first_seen: synced_at,
                last_seen: synced_at,
                synced_at,
            },
        ];

        let parquet_data = assets_to_parquet(original.clone()).unwrap();
        assert!(!parquet_data.is_empty());
        let restored = parquet_to_assets(&parquet_data).unwrap();

        assert_eq!(restored, original);
    }
}
