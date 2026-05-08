# Collector: Event Schema Validation and Normalization (bf-3s7)

## Summary

Event schema validation and normalization is fully implemented in the TRACE collector.

## Implementation Status

### 1. Event Schema Validation (`collector/src/validator.rs` - 402 lines)

**Features:**
- Event type validation against allowed values (pageview, click, scroll, dwell)
- URL validation and normalization (adds https:// if missing)
- Query parameter sanitization (blocks dangerous keys like password, token, api_key)
- Session ID and user ID validation (alphanumeric, hyphens, underscores only)
- Payload size validation (max 1MB)
- Comprehensive error reporting with field-level details

**Validation Rules:**
- Maximum string length: 4096 characters
- Maximum payload size: 1MB
- Maximum ID length: 256 characters
- Blocked parameter keys: password, passwd, secret, token, api_key, access_token, auth, cookie, session, csrf
- Valid ID characters: alphanumeric, hyphen, underscore

**Tests:** 12 test cases covering all validation scenarios

### 2. Cross-Network Campaign Normalization (`collector/src/normalizer.rs` - 374 lines)

**Supported Networks:**
- Taboola (tb_*, utm_source=taboola)
- Outbrain (ob_*, utm_source=outbrain)
- MGID (mg_*, utm_source=mgid)
- RevContent (rc_*, utm_source=revcontent)
- Generic/unknown (fallback)

**Normalized Schema:**
- `network`: Detected ad network name
- `campaign_id`: Network's campaign identifier
- `creative_id`: Creative/asset identifier
- `headline`: Headline or title text
- `image_id`: Image identifier or thumbnail URL
- `item_id`: Item identifier

**Features:**
- Network detection from utm_source or parameter prefixes
- Parameter mapping to unified schema
- Creative fingerprinting for deduplication
- Campaign data detection helper

**Tests:** 8 test cases covering all networks and edge cases

### 3. Integration (`collector/src/main.rs`)

Both modules are integrated into the collector's HTTP endpoints:
- POST /collect: JSON payload validation and normalization
- GET /collect: Query parameter validation and normalization

## Usage Example

```rust
// Validation
let validated = EventValidator::validate(
    "pageview",
    Some(&"https://example.com?utm_source=taboola&tb_image=img123".to_string()),
    &params,
    &extra,
)?;

// Normalization
let normalized = NetworkNormalizer::normalize(&validated.params);
// normalized.network == "taboola"
// normalized.creative_id == Some("img123".to_string())
```

## Added in Previous Commits

- `ed5f2ba` - Phase 4: Cross-Network Normalization (normalizer.rs)
- `19ddd27` - feat: complete HTTP event ingestion endpoint with validation and queue (validator.rs)

## Bead Status

This bead (bf-3s7) verifies and documents the existing implementation. The event schema validation and normalization functionality is complete and production-ready.
