# TRACE Implementation Plan

**Traffic Recording, Attribution, and Campaign Events**

---

## Overview

TRACE is a lightweight, self-hosted event tracking system for affiliate marketing and direct response advertising. It captures raw traffic signals and attributes performance back to campaigns, creatives, and assets.

## Design Philosophy

1. **Log first, parse later** — Collector is an access log. Raw requests stored as-is. All enrichment happens downstream.
2. **Dynamic schema** — Query parameters stored as MAP. No schema migrations when adding new UTM/custom params.
3. **Observation only** — Nothing needs pre-registration. Assets, parameters, campaigns discovered from data.
4. **Reprocessable** — Raw event logs are source of truth. Replay from beginning if ETL improves.
5. **Minimal infrastructure** — Single container with DuckDB at typical affiliate volumes (~15-20k events/day).

---

## Architecture

```
┌──────────────────────┐
│  Browser             │
│  + JS Tag / Pixel    │
└──────────┬───────────┘
           │ POST/GET /collect
           │ JSON body or query params
           ▼
┌──────────────────────┐
│  Collector (Rust)    │
│  /collect endpoint   │
│  Write JSONL by hour │
└──────────┬───────────┘
           │ events-YYYYMMDD-HH.jsonl
           │ (compressed to .gz on rotation)
           ▼
┌──────────────────────┐
│  Flusher (Rust)      │
│  File watcher        │
│  Parse → Parquet     │
│  Upload to S3        │
└──────────┬───────────┘
           │ s3://bucket/events/dt=YYYY-MM-DD/hour=HH/
           │ Hive partitioned Parquet files
           ▼
┌──────────────────────┐
│  Iceberg Tables      │
│  + Compaction jobs   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  DuckDB / Trino      │
│  Queries & Reports   │
└──────────────────────┘
```

---

## Data Model

### Collector Event (JSONL)

```json
{
  "ts": "2026-05-08T14:30:00Z",
  "ip": "1.2.3.4",
  "ua": "Mozilla/5.0...",
  "url": "https://example.com/page?utm_source=taboola&utm_campaign=c123&item=i456",
  "params": {
    "utm_source": "taboola",
    "utm_campaign": "c123",
    "item": "i456",
    "tb_image": "img-abc-123",
    "tb_headline": "Lose Weight Fast"
  },
  "type": "pageview"
}
```

### Parquet Schema (Hive Partitioned)

```
ts: TIMESTAMP(MILLIS, UTC) NOT NULL
ip: UTF8 (OPTIONAL)
ua: UTF8 (OPTIONAL)
url: UTF8 NOT NULL
params: UTF8 NOT NULL  -- JSON string of query params
type: UTF8 NOT NULL

Partitioned by:
  dt: STRING (YYYY-MM-DD)
  hour: STRING (HH)
```

---

## Implementation Status

### Phase 1: Core Collection Infrastructure ✅ COMPLETE

**Status**: Implemented and working

#### Collector Service (`collector/`)

- **HTTP Endpoint**: `/collect` accepts both GET (query params) and POST (JSON body)
- **Event Types**: pageview, click, scroll, dwell (extensible via `type` field)
- **Headers Captured**: X-Forwarded-For (IP), User-Agent
- **Log Rotation**: Hourly files with automatic compression to gzip
- **Graceful Shutdown**: Flushes buffers on SIGTERM/SIGINT
- **Health Check**: `/health` endpoint
- **Dockerfile**: Multi-stage Alpine build, healthcheck included

**Tech Stack**: Rust, Axum, Tokio, Chrono, flate2

#### Flusher Service (`flusher/`)

- **File Watching**: Uses `notify` crate for new .jsonl.gz files
- **Parquet Conversion**: Arrow-based in-memory conversion
- **S3 Upload**: Partitioned by `dt=YYYY-MM-DD/hour=HH`
- **DLQ Pattern**: Failed files moved to `/data/dlq` with error metadata
- **Retry Logic**: 3 retries with 5s backoff
- **Dockerfile**: Multi-stage Alpine build

**Tech Stack**: Rust, AWS SDK, Parquet/Arrow, notify

#### Docker Compose (`docker-compose.yml`)

- Collector service exposed on port 8080
- Shared volume `trace-logs` for log files

---

### Phase 2: CI/CD & Deployment ✅ COMPLETE

**Status**: CI workflows complete, ready for deployment

#### Argo Workflows

**Collector CI** (`.argo/trace-collector-ci-workflowtemplate.yaml`) ✅
- Checkout from GitHub
- `cargo fmt --check` and `cargo clippy`
- `cargo build --release`
- Kaniko Docker build to GHCR
- Tag handling: vX.Y.Z → pushes X.Y, X, latest

**Flusher CI** (`.argo/trace-flusher-ci-workflowtemplate.yaml`) ✅
- Checkout from GitHub
- `cargo fmt --check` and `cargo clippy`
- Kaniko Docker build to GHCR
- Tag handling: vX.Y.Z → pushes X.Y, X, latest

**Deployment**
- Target: ardenone-manager cluster
- Services: Collector + Flusher deployments
- Persistent volume for shared logs
- S3 bucket configuration

---

### Phase 3: Client Integration 📋 PLANNED

#### JavaScript Tag

**Features**:
- Autocapture pageviews on load
- Link decoration for session stitching
- Heartbeat pings for dwell time
- Click tracking on outbound links
- Scroll depth tracking
- Privacy: Local storage only, no third-party cookies

**API**:
```javascript
// Basic pageview (autocaptured)
TRACE.collect();

// Custom event
TRACE.collect('click', { element: 'buy-button' });

// Set user ID (first-party cookie)
TRACE.identify('user-123');
```

**Delivery Options**:
1. **Pixel tag** (backward compatible):
```html
<img src="https://trace.example.com/collect?url=PAGE_URL&type=pageview" width="1" height="1">
```

2. **JS tag** (recommended):
```html
<script src="https://trace.example.com/trace.js" data-collector="https://trace.example.com/collect"></script>
```

---

### Phase 4: Attribution & Cross-Network Normalization ✅ COMPLETE

**Status**: Cross-network normalization implemented, attribution sync planned

#### Cross-Network Normalization

Different ad networks use different parameter names for the same concepts. The normalization layer unifies these into a common schema.

**Supported Networks**:
- Taboola (`tb_item`, `tb_image`, `tb_headline`)
- Outbrain (`ob_item`, `ob_creative`)
- MGID (`mg_id`, `mg_title`, `mg_image`)
- RevContent (`rc_id`, `rc_title`, `rc_thumb`)

**Implementation**:
1. **Rust Normalizer Module** (`collector/src/normalizer.rs`):
   - Network detection from `utm_source` or parameter prefixes
   - Parameter mapping to common schema
   - Generic fallback for unknown networks

2. **SQL Views** (`docs/analytics/normalization.sql`):
   - `normalized_campaigns` - Unified view across all networks
   - `network_performance` - Compare metrics by network
   - `top_creatives` - Best performing creatives cross-network
   - `creative_fatigue` - Detect declining performance
   - `cross_network_creatives` - Find same creative across networks

**Normalized Schema**:
```rust
pub struct NormalizedCampaign {
    pub network: String,        // Detected ad network
    pub campaign_id: Option<String>,
    pub creative_id: Option<String>,
    pub headline: Option<String>,
    pub image_id: Option<String>,
    pub item_id: Option<String>,
}
```

**Usage**:
```sql
-- Top creatives across all networks
SELECT network, headline, clicks, ctr_pct
FROM top_creatives
ORDER BY clicks DESC LIMIT 20;

-- Same creative across networks (arbitrage detection)
SELECT * FROM cross_network_creatives
WHERE num_networks >= 2;
```

#### Ad Network API Sync ✅ COMPLETE

**Status**: Syncer service implemented with support for all major ad networks

**Implementation**:
1. **API Sync Jobs**: Periodic fetch from ad network APIs ✅
2. **Creative Registry**: Store creative metadata (image URLs, headlines, landing pages) ✅
3. **Asset Tagging**: Tag images/headlines with semantic categories (future enhancement)
4. **Performance Attribution**: Join events with creative registry (via S3 Parquet files) ✅

#### Syncer Service (`syncer/`)

The syncer service fetches creative metadata from ad network APIs and stores it in S3 as Parquet files for enrichment.

**Supported Networks**:
- Taboola (Backstage API)
- Outbrain (Amplify API)
- MGID API
- RevContent API
- Demo client (for testing without API keys)

**Features**:
- Configurable sync interval (default: 1 hour)
- One-shot mode (`--once`) for CI/CD
- Network filtering (`--networks taboola,outbrain`)
- In-memory registry with S3 persistence
- Parquet storage for efficient querying

**Tech Stack**: Rust, AWS SDK, Parquet/Arrow, reqwest

**API Client Structure**:
```rust
pub trait ApiClient {
    async fn fetch_creatives(&mut self) -> Result<ApiSyncResult>;
    fn network_name(&self) -> &str;
}
```

**Creative Metadata Schema**:
```rust
pub struct CreativeMetadata {
    pub network: String,
    pub campaign_id: Option<String>,
    pub campaign_name: Option<String>,
    pub creative_id: Option<String>,
    pub headline: Option<String>,
    pub image_url: Option<String>,
    pub landing_page_url: Option<String>,
    pub item_id: Option<String>,
    pub synced_at: DateTime<Utc>,
}
```

**Usage**:
```bash
# Run once and exit
trace-syncer --once

# Continuous sync mode (default interval: 1 hour)
trace-syncer --interval 3600

# Sync specific networks only
trace-syncer --networks taboola,outbrain
```

**Environment Variables**:
```
TRACE_S3_BUCKET=my-trace-bucket
TRACE_S3_REGION=us-east-1
TRACE_S3_PREFIX=trace-events
TABOOLA_API_KEY=xxx          # Optional
OUTBRAIN_API_KEY=xxx         # Optional
MGID_API_KEY=xxx             # Optional
REVCONTENT_API_KEY=xxx       # Optional
```

**S3 Storage**:
- Registry stored as `s3://bucket/creative-registry.parquet`
- Can be loaded directly in DuckDB for joining with event data

**DuckDB Integration**:
```sql
-- Load creative registry
CREATE TABLE creatives AS
SELECT * FROM read_parquet('s3://bucket/trace-events/creative-registry.parquet');

-- Join with events for enriched attribution
SELECT
    e.network,
    c.headline,
    c.image_url,
    c.landing_page_url,
    COUNT(*) FILTER (WHERE e.type = 'click') AS clicks
FROM events e
LEFT JOIN creatives c
    ON e.network = c.network
    AND e.params:utm_campaign = c.campaign_id
    AND e.params:tb_image = c.creative_id
GROUP BY ALL;
```

---

### Phase 5: Analytics & Reporting ✅ COMPLETE

**Status**: Compaction service and Iceberg documentation complete

#### Compactor Service (`compactor/`)

- **Parquet Merging**: Combines hourly files into daily partitions
- **S3 Operations**: Reads from `events/dt=*/hour=*` writes to `events-compacted/dt=*`
- **Cleanup**: Deletes original hourly files after successful compaction
- **Configurable Lookback**: Processes last N days (default: 7)
- **CronJob Scheduling**: Runs daily at 2 AM UTC

**Tech Stack**: Rust, AWS SDK, Parquet/Arrow

#### DuckDB Queries

Documentation includes 30+ sample queries:

**Common Metrics**:
```sql
-- CTR by campaign
SELECT
  params:utm_campaign AS campaign,
  COUNT(*) FILTER (WHERE type = 'pageview') AS views,
  COUNT(*) FILTER (WHERE type = 'click') AS clicks,
  clicks::FLOAT / NULLIF(views, 0) AS ctr
FROM read_parquet('s3://bucket/events/**/*.parquet')
GROUP BY 1;

-- Asset performance (by headline)
SELECT
  params:tb_headline AS headline,
  COUNT(*) FILTER (WHERE type = 'click') AS clicks,
  COUNT(DISTINCT params:utm_campaign) AS campaigns
FROM read_parquet('s3://bucket/events/**/*.parquet')
WHERE params:tb_headline IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
```

#### Compaction Strategy ✅ IMPLEMENTED

**Problem**: Many small Parquet files from hourly uploads

**Solution**:
1. **Daily compaction job**: Merge hourly files into daily partitions ✅
2. **Retention**: Raw JSONL deleted after successful Parquet upload ✅
3. **Iceberg integration**: For large-scale queries (optional) ✅

**Implementation**:
- Compactor service runs as CronJob (daily at 2 AM UTC)
- Reads from `s3://bucket/events/dt=YYYY-MM-DD/hour=HH/*.parquet`
- Writes to `s3://bucket/events-compacted/dt=YYYY-MM-DD/part-*.parquet`
- Deletes source files after successful merge
- Configurable lookback period (default 7 days)

---

### Phase 6: Advanced Features 📋 PLANNED

#### Session Stitching

- First-party cookie for user identification
- Link decoration (utm_session) for cross-site tracking
- Session timeout (30 min inactivity)

#### Fraud Detection

- Bot filtering via User-Agent analysis
- IP-based rate limiting
- Suspicious pattern detection (rapid clicks, same IP multiple campaigns)

#### Real-time Dashboard

- Live event stream (WebSocket)
- Campaign performance in last hour
- Alerting on anomalies

---

## Deployment Plan

### Infrastructure Requirements

**Minimal** (~15k events/day):
- 1 container (collector + flusher combined)
- 2 CPU, 4GB RAM
- 10GB volume for logs
- S3 bucket (any region)

**Recommended** (~100k+ events/day):
- Separate collector and flusher containers
- Collector: 1 CPU, 2GB RAM
- Flusher: 2 CPU, 4GB RAM
- 50GB volume for logs
- Iceberg tables + Trino for analytics

### Environment Variables

**Collector**:
```
TRACE_LOG_DIR=/data/logs
RUST_LOG=info
PORT=8080
```

**Flusher**:
```
TRACE_LOG_DIR=/data/logs
TRACE_DLQ_DIR=/data/dlq
TRACE_S3_BUCKET=my-trace-bucket
TRACE_S3_REGION=us-east-1
TRACE_S3_PREFIX=trace-events
AWS_ACCESS_KEY_ID=***
AWS_SECRET_ACCESS_KEY=***
AWS_SESSION_TOKEN=***  # if using temporary creds
```

**Compactor**:
```
TRACE_S3_BUCKET=my-trace-bucket
TRACE_S3_REGION=us-east-1
TRACE_S3_PREFIX=trace-events
COMPACTOR_LOOKBACK_DAYS=7
AWS_ACCESS_KEY_ID=***
AWS_SECRET_ACCESS_KEY=***
AWS_SESSION_TOKEN=***  # if using temporary creds
```

---

## Roadmap

| Phase | Status | Target |
|-------|--------|--------|
| 1. Core Collection | ✅ Complete | DONE |
| 2. CI/CD & Deploy | ✅ Complete | 2026-05-15 |
| 3. Client JS Tag | ✅ Complete | 2026-05-30 |
| 4. Cross-Network Normalization | ✅ Complete | 2026-05-08 |
| 5. Analytics Layer | ✅ Complete | 2026-05-08 |
| 6. Ad Network API Sync | ✅ Complete | 2026-05-08 |

---

## Open Questions

1. **Session storage**: Cookie vs localStorage for session IDs? (Advanced feature)
2. **PII handling**: Should we hash IPs for GDPR compliance? (Advanced feature)
3. **Real-time streaming**: WebSocket support for live dashboards? (Advanced feature)

---

## Next Steps

1. ✅ Create this plan document
2. ✅ Add flusher CI workflow template
3. ✅ Add compactor CI workflow template
4. ✅ Add syncer CI workflow template
5. ✅ Implement JS client tag
6. ✅ Implement ad network API sync
7. ⏳ Create ArgoCD manifests for deployment
8. ⏳ Set up S3 bucket and IAM policies
9. ⏳ Add semantic tagging for headlines/images (future enhancement)
10. ⏳ Implement real-time dashboard (future enhancement)
