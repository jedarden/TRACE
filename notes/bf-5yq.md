# Bead bf-5yq: Normalization - Unified Event Schema Across Ad Networks

## Summary

This bead verifies and documents the existing cross-network normalization implementation in TRACE.

## Implementation Status

### 1. Rust Normalizer Module (`collector/src/normalizer.rs`)

**Status:** ✅ Complete (implemented in commit ed5f2ba)

**Supported Networks:**
- Taboola (`tb_` prefix, `utm_source=taboola`)
- Outbrain (`ob_` prefix, `utm_source=outbrain`)
- MGID (`mg_` prefix, `utm_source=mgid`)
- RevContent (`rc_` prefix, `utm_source=revcontent`)
- Unknown (generic fallback)

**Normalized Fields:**
- `network` - Detected ad network
- `campaign_id` - From `utm_campaign`
- `creative_id` - Network-specific creative identifier
- `headline` - Creative headline/title text
- `image_id` - Image or thumbnail identifier
- `item_id` - Content item identifier

### 2. Iceberg Schema (`analytics/schemas/ad_events_iceberg.sql`)

**Status:** ✅ Complete (implemented in commit b1e68ee)

**Schema Fields:**
- Basic event fields: `ts`, `ip`, `ua`, `url`, `type`
- Identity fields: `session_id`, `user_id`, `cookie_id`
- Normalized network detection: `network`
- Campaign identifiers: `campaign_id`, `campaign_name`
- Creative identifiers: `creative_id`, `headline`, `image_id`, `item_id`
- Raw parameters: `params` (MAP for flexibility)

### 3. SQL Analytics Views (`docs/analytics/normalization.sql`)

**Status:** ✅ Complete

**Available Views:**
- `normalized_campaigns` - Base normalized view
- `network_performance` - Daily metrics by network
- `top_creatives` - Best-performing headlines
- `creative_fatigue` - Declining performance detection
- `cross_network_creatives` - Same creative across networks

### 4. Documentation (`docs/analytics/cross_network_normalization.md`)

**Status:** ✅ Complete

**Contents:**
- Supported networks matrix
- Normalized schema definition
- Usage examples (Rust and SQL)
- Analytics use cases
- Implementation details
- Testing guide

## Data Flow

1. **Collection:** Client sends event with network-specific URL parameters
2. **Detection:** `NetworkNormalizer::detect_network()` identifies the ad network
3. **Normalization:** `NetworkNormalizer::normalize()` maps parameters to common schema
4. **Storage:** Event written to JSONL with `normalized` field
5. **Analytics:** SQL views query normalized fields for cross-network analysis

## Example Event

```json
{
  "ts": "2026-05-08T14:30:00Z",
  "url": "https://example.com/?utm_source=taboola&tb_image=img123&tb_headline=Click+Here",
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

## Verification

The normalization implementation was verified complete with:
- ✅ Network detection for 4 ad networks
- ✅ Parameter mapping to unified schema
- ✅ Generic fallback for unknown networks
- ✅ Iceberg schema for long-term storage
- ✅ SQL views for cross-network analytics
- ✅ Comprehensive documentation

## Notes

- `campaign_name` field in Iceberg schema requires network API enrichment (future work)
- `cookie_id` is handled separately as `session_id` in the validator
- Raw `params` map is preserved for flexibility and debugging
