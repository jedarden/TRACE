# Phase 2: JS Tag and Pixel Endpoint - Complete

## Summary

Phase 2 client-side tracking assets are fully implemented and functional.

## Components Delivered

### 1. JavaScript Tag (`client/trace.js`)
- **Size**: 30KB source, 12KB minified
- **Features**:
  - Pageview autocapture on load and SPA navigation
  - Dwell heartbeat (30s intervals, configurable)
  - Click tracking on all links with coordinates
  - Scroll depth tracking (25%, 50%, 75%, 90% thresholds)
  - Session stitching via link decoration
  - Cross-domain tracking (PostMessage API, storage bridge)
  - First-party cookie identity and persistence
  - Privacy-first design (localStorage/cookie only, no third-party cookies)

### 2. Ad Network Macro Capture
The JS tag captures **all URL query parameters**, including:
- **UTM parameters**: utm_source, utm_campaign, utm_content, utm_medium, utm_term
- **Taboola**: tb_item, tb_image, tb_headline
- **Outbrain**: ob_item, ob_creative
- **MGID**: mg_id, mg_title, mg_image
- **RevContent**: rc_id, rc_title, rc_thumb
- **Google Ads**: gclid, campaignid, adgroupid
- **Meta**: fbclid, campaign_id, ad_id

### 3. Pixel Fallback (`client/pixel-example.html`)
- Simple 1x1 img tag for JS-blocked environments
- GET request to /collect endpoint
- Documentation and examples provided

### 4. Collector Endpoint
- **GET /collect**: Query string endpoint for pixel tracking
- **POST /collect**: JSON payload endpoint for JS tag
- Log-first design: writes raw requests to hourly log files
- No parsing at collection time (enrichment happens downstream)

### 5. Documentation
- **README.md**: Comprehensive integration guide
- **demo.html**: Interactive demo showing all features
- **pixel-example.html**: Pixel tag documentation and examples

## Verification

All required functionality from the task description is implemented:
- ✅ JS tag fires pageview events
- ✅ Dwell heartbeat every 30 seconds
- ✅ Click events with coordinates
- ✅ Full URL + UTM + ad network macros captured
- ✅ Pixel fallback for JS-blocked environments
- ✅ Both hit the Collector endpoint

## File Structure

```
client/
├── trace.js           # Main JavaScript tracking library (30KB)
├── trace.min.js       # Minified production bundle (12KB)
├── build.js           # Build script for minification
├── demo.html          # Interactive demo
├── pixel-example.html # Pixel tag documentation
└── README.md          # Integration guide
```

## Integration Example

```html
<!-- Add to website <head> -->
<script src="https://your-domain.com/trace.js"
        data-collector="https://your-domain.com/collect"></script>

<!-- Pixel fallback for JS-blocked environments -->
<img src="https://your-domain.com/collect?url=PAGE_URL&type=pageview"
     width="1" height="1" alt="" style="display:none">
```

## Notes

- The collector was simplified to a log-first design (normalizer, queue, validator modules removed)
- Raw requests are logged to `raw-YYYYMMDD-HH.jsonl` files
- Downstream processing (flusher) handles parsing and enrichment
- This design improves performance and reliability at collection time
