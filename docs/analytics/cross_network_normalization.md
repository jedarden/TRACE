# Cross-Network Normalization

## Overview

Different ad networks use different parameter names for the same campaign concepts. TRACE's cross-network normalization layer unifies these into a common schema for unified analytics.

## Supported Networks

| Network | Source Parameter | Image ID | Headline/Title | Creative ID |
|---------|------------------|----------|----------------|-------------|
| **Taboola** | `utm_source=taboola` | `tb_image` | `tb_headline` | `tb_image` |
| **Outbrain** | `utm_source=outbrain` | `ob_creative` | *(not passed)* | `ob_creative` |
| **MGID** | `utm_source=mgid` | `mg_image` | `mg_title` | `mg_id` |
| **RevContent** | `utm_source=revcontent` | `rc_thumb` | `rc_title` | `rc_id` |

## Normalized Schema

The normalized view provides these common fields:

- **`network`** - Detected ad network (taboola, outbrain, mgid, revcontent, unknown)
- **`campaign_id`** - Campaign identifier from `utm_campaign`
- **`creative_id`** - Unique creative identifier (normalized from network-specific field)
- **`headline`** - Creative headline/title text (normalized from network-specific field)
- **`image_id`** - Image or thumbnail identifier (normalized from network-specific field)
- **`item_id`** - Item identifier where available

## Usage

### Using the Normalized Views

Load the normalization views in DuckDB:

```sql
-- Load the normalization views
COPY /path/to/TRACE/docs/analytics/normalization.sql
-- Or paste the contents directly
```

Query across all networks:

```sql
-- Top creatives across all networks
SELECT
    network,
    headline,
    clicks,
    views,
    ctr_pct
FROM top_creatives
WHERE views >= 100
ORDER BY ctr_pct DESC
LIMIT 20;

-- Compare the same creative across networks
SELECT
    normalized_headline,
    networks,
    total_clicks,
    overall_ctr
FROM cross_network_creatives
WHERE num_networks >= 2
ORDER BY overall_ctr DESC;
```

### Using the Rust Normalizer

The `collector` crate includes a `normalizer` module that can be used programmatically:

```rust
use trace_collector::normalizer::NetworkNormalizer;

// Detect network from parameters
let network = NetworkNormalizer::detect_network(&params);

// Normalize to common schema
let normalized = NetworkNormalizer::normalize(&params);

println!("Network: {}", normalized.network);
println!("Creative ID: {:?}", normalized.creative_id);
println!("Headline: {:?}", normalized.headline);

// Generate a unique fingerprint for deduplication
let fingerprint = NetworkNormalizer::creative_fingerprint(&normalized);
```

## Event Format

Events now include normalized campaign data:

```json
{
  "ts": "2026-05-08T14:30:00Z",
  "ip": "1.2.3.4",
  "ua": "Mozilla/5.0...",
  "url": "https://example.com/?utm_source=taboola&tb_image=img123&tb_headline=Click+Here",
  "params": {
    "utm_source": "taboola",
    "tb_image": "img123",
    "tb_headline": "Click Here"
  },
  "type": "click",
  "normalized": {
    "network": "taboola",
    "campaign_id": null,
    "creative_id": "img123",
    "headline": "Click Here",
    "image_id": "img123",
    "item_id": null
  }
}
```

## Analytics Use Cases

### 1. Cross-Network Performance Comparison

Compare CTR across all networks:

```sql
SELECT
    network,
    SUM(clicks) AS total_clicks,
    SUM(views) AS total_views,
    ROUND(100.0 * SUM(clicks) / NULLIF(SUM(views), 0), 2) AS ctr
FROM normalized_campaigns
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY network
ORDER BY ctr DESC;
```

### 2. Creative Arbitrage Detection

Find the same creative running on multiple networks:

```sql
SELECT * FROM cross_network_creatives
WHERE num_networks >= 2
ORDER BY total_clicks DESC;
```

### 3. Creative Fatigue Monitoring

Detect declining performance across networks:

```sql
SELECT * FROM creative_fatigue
WHERE fatigue_change_pct < -20
ORDER BY fatigue_change_pct ASC;
```

### 4. Network-Specific Deep Dives

```sql
-- Taboola top headlines
SELECT
    headline,
    clicks,
    views,
    ctr_pct
FROM top_creatives
WHERE network = 'taboola'
ORDER BY clicks DESC
LIMIT 20;

-- MGID top titles
SELECT
    headline,
    clicks,
    views,
    ctr_pct
FROM top_creatives
WHERE network = 'mgid'
ORDER BY clicks DESC
LIMIT 20;
```

## Implementation Details

### Network Detection Logic

1. **Primary**: Check `utm_source` parameter for known network names
2. **Fallback**: Check for network-specific parameter prefixes (`tb_`, `ob_`, `mg_`, `rc_`)
3. **Default**: Return "unknown" if no network detected

### Parameter Mapping

Each network's parameters are mapped to the normalized schema:

**Taboola:**
- `tb_image` → `creative_id`, `image_id`
- `tb_headline` → `headline`
- `tb_item` → `item_id`

**Outbrain:**
- `ob_creative` → `creative_id`, `image_id`
- `ob_item` → `item_id`
- *(no headline passed in URL)*

**MGID:**
- `mg_id` → `creative_id`, `item_id`
- `mg_title` → `headline`
- `mg_image` → `image_id`

**RevContent:**
- `rc_id` → `creative_id`, `item_id`
- `rc_title` → `headline`
- `rc_thumb` → `image_id`

### Generic Fallback

For unknown networks, the normalizer attempts to extract data from common parameter names:
- `utm_campaign` → `campaign_id`
- `item`, `asset` → `item_id`
- `headline`, `title`, `head` → `headline`
- `image`, `img`, `thumb`, `thumbnail` → `image_id`

## Testing

The normalizer module includes comprehensive unit tests:

```bash
cd collector
cargo test normalizer
```

Tests cover:
- Network detection from `utm_source`
- Network detection from parameter prefixes
- Parameter mapping for each network
- Generic fallback handling
- Creative fingerprint generation

## Future Enhancements

Potential additions to the normalization layer:

1. **Additional Networks**: Add support for more ad networks (AdVenture, ContentAd, etc.)
2. **Creative Metadata**: Fetch creative metadata (image URLs, landing pages) via network APIs
3. **Semantic Analysis**: Classify headlines into categories (weight loss, finance, etc.)
4. **Image Hashing**: Detect duplicate images across networks using perceptual hashing
5. **Bid Normalization**: Normalize bid/cost data if cost parameters are added
