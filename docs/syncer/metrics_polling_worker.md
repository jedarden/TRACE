# Ad API Sync: Polling Worker for Performance Metrics

## Overview

The TRACE syncer includes a production-ready polling worker that continuously fetches performance metrics from ad network APIs (Taboola, Outbrain, MGID, RevContent) and stores them in S3 for analytics.

## Architecture

```
┌──────────────────────┐
│  Polling Worker      │
│  (tokio::interval)   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  API Clients         │
│  - Taboola           │
│  - Outbrain          │
│  - MGID              │
│  - RevContent        │
│  - Demo (testing)    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  MetricsRegistry     │
│  (in-memory buffer)  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  S3 Storage          │
│  (Parquet by date)   │
└──────────────────────┘
```

## Components

### 1. Main Polling Loop (`main.rs`)

```rust
// Continuous sync mode with configurable interval
let mut timer = interval(Duration::from_secs(args.interval));
timer.tick().await; // Skip first immediate tick

loop {
    if sync_metrics {
        run_metrics_sync(&mut metrics_registry, &mut clients, start_date, end_date).await?;
    }
    timer.tick().await;
}
```

### 2. Metrics Sync Function (`run_metrics_sync`)

- Iterates through all configured API clients
- Fetches metrics for the specified date range
- Accumulates metrics in the `MetricsRegistry`
- Persists to S3 after all clients are polled
- Tracks total metrics fetched and errors

### 3. API Client Trait (`api_client.rs`)

Each ad network client implements:

```rust
#[async_trait]
pub trait ApiClient: Send + Sync {
    async fn fetch_metrics(&mut self, start_date: NaiveDate, end_date: NaiveDate)
        -> Result<MetricsSyncResult>;
    fn network_name(&self) -> &str;
}
```

### 4. Performance Metrics Data Model (`creative.rs`)

```rust
pub struct PerformanceMetrics {
    pub network: String,
    pub campaign_id: String,
    pub campaign_name: Option<String>,
    pub creative_id: Option<String>,
    pub date: chrono::NaiveDate,
    pub impressions: i64,
    pub clicks: i64,
    pub spend_micros: i64,
    pub conversions: Option<i64>,
    pub ctr_bps: Option<i32>,      // Calculated CTR in basis points
    pub cpc_micros: Option<i64>,   // Calculated CPC
    pub cpm_micros: Option<i64>,   // Calculated CPM
    pub synced_at: DateTime<Utc>,
}
```

## Usage

### Command Line Options

```bash
# Run in continuous mode (default: 1 hour interval)
trace-syncer --mode metrics

# Specify custom interval (e.g., 30 minutes)
trace-syncer --mode metrics --interval 1800

# Fetch last 7 days of metrics
trace-syncer --mode metrics --days-back 7

# Fetch only yesterday's metrics
trace-syncer --mode metrics --yesterday

# Run once and exit (useful for cron jobs)
trace-syncer --mode metrics --once

# Sync specific networks only
trace-syncer --mode metrics --networks taboola,outbrain
```

### Environment Variables

```bash
# Required
export TRACE_S3_BUCKET=your-bucket-name
export TRACE_S3_REGION=us-east-1
export TRACE_S3_PREFIX=trace-events

# Optional (API credentials)
export TABOOLA_API_KEY=your_key
export OUTBRAIN_API_KEY=your_key
export MGID_API_KEY=your_key
export REVCONTENT_API_KEY=your_key
```

## Storage Format

Metrics are stored in S3 as Parquet files partitioned by date:

```
s3://bucket/prefix/metrics/metrics-2026-05-08.parquet
s3://bucket/prefix/metrics/metrics-2026-05-07.parquet
...
```

Each Parquet file contains:
- Network identifier
- Campaign ID and name
- Creative ID (if applicable)
- Date
- Raw metrics (impressions, clicks, spend, conversions)
- Calculated metrics (CTR, CPC, CPM)
- Sync timestamp

## Error Handling

- API errors are logged but don't stop the polling loop
- Missing dates in S3 are handled gracefully (warning logged)
- Failed API calls increment an error counter but continue to next client
- S3 upload failures propagate and halt the sync cycle

## Derived Metrics

The system automatically calculates:

1. **CTR (Click-Through Rate)**: `clicks * 10000 / impressions` (in basis points)
2. **CPC (Cost Per Click)**: `spend_micros / clicks` (in microcurrency)
3. **CPM (Cost Per Mille)**: `spend_micros * 1000 / impressions` (in microcurrency)

## Demo Mode

When no API keys are configured, the syncer runs in demo mode with sample data:

```bash
# No API keys set - automatically uses DemoClient
trace-syncer --mode metrics
```

## Integration with Analytics

The stored metrics can be queried via:

- **DuckDB**: Direct Parquet queries
- **Trino/Iceberg**: SQL queries over the data lake
- **Analytics queries**: Pre-built queries in `analytics/queries/`

Example DuckDB query:

```sql
SELECT
    network,
    campaign_id,
    date,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,
    SUM(spend_micros) / 1000000.0 as spend_usd
FROM read_parquet('s3://bucket/prefix/metrics/*.parquet')
WHERE date >= '2026-05-01'
GROUP BY network, campaign_id, date
ORDER BY date DESC;
```

## Monitoring

The syncer logs key metrics:

- Number of metrics fetched per network
- Total metrics fetched per sync cycle
- Number of errors encountered
- S3 upload confirmations

Example log output:

```
INFO Starting metrics sync from 2026-05-01 to 2026-05-08...
INFO Fetching metrics from taboola...
INFO Fetched 150 metrics from taboola
INFO Fetching metrics from outbrain...
INFO Fetched 120 metrics from outbrain
INFO Persisting metrics to S3...
INFO Stored metrics for 2026-05-01 to s3://bucket/prefix/metrics/metrics-2026-05-01.parquet
INFO Metrics sync complete: 270 metrics fetched, 0 errors
```

## Deployment

### Kubernetes

The syncer can be deployed as a Deployment or CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: trace-metrics-syncer
spec:
  schedule: "0 */1 * * *"  # Every hour
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: syncer
            image: trace-syncer:latest
            args: ["--mode", "metrics", "--once"]
            env:
            - name: TRACE_S3_BUCKET
              value: "your-bucket"
```

### Docker

```bash
docker run -d \
  -e TRACE_S3_BUCKET=your-bucket \
  -e TRACE_S3_REGION=us-east-1 \
  -e TABOOLA_API_KEY=your_key \
  trace-syncer:latest \
  --mode metrics --interval 3600
```

## Performance Considerations

- **Memory**: Metrics are buffered in memory before S3 upload
- **Network**: One API call per network per sync cycle
- **S3**: One PUT request per date with metrics
- **Storage**: Parquet compression reduces storage by ~5-10x vs JSON

## Future Enhancements

Potential improvements:

1. **Incremental sync**: Only fetch dates that don't exist in S3
2. **Backfill mode**: Separate worker for historical data
3. **Metrics aggregation**: Pre-aggregate by week/month
4. **Alerting**: Notify on sync failures or unusual metrics
5. **Rate limiting**: Respect API rate limits with exponential backoff
