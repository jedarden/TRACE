# Bead bf-29u: Normalization - Meta Ads impression/click adapter

## Status: Already Complete

This bead's work was already completed in commit 17e90ab by bead bf-5rh.

## Implementation Summary

The Meta Ads (Facebook/Instagram) normalization adapter is fully implemented in `collector/src/normalizer.rs`:

### Network Detection
- Via utm_source: facebook, instagram, meta
- Via click identifiers: fbclid (Facebook Click ID), igshid (Instagram ID)

### Parameter Normalization
- Campaign ID: utm_campaign, campaignid, campaign_id
- Creative ID: ad_id, adid, adset_id, adsetid, utm_content
- Headline: headline, ad_name, adname, utm_term
- Image ID: imageid, image_id, creative
- Item ID: ad_id, adid, adset_id, adsetid

### Tests
All 10 Meta Ads tests pass:
- test_detect_network_meta_facebook_utm_source
- test_detect_network_meta_fbclid
- test_detect_network_meta_igshid
- test_normalize_meta_with_utm_params
- test_normalize_meta_with_ad_id
- test_normalize_meta_with_adset_id
- test_normalize_meta_with_ad_name
- test_normalize_meta_with_image
- test_normalize_meta_fbclid_only
- test_normalize_meta_complete
- test_creative_fingerprint_meta
- test_has_campaign_data_meta

This bead is a duplicate of bf-5rh.
