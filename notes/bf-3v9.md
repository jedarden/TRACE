# Bead bf-3v9: Session Stitching Attribution Models

## Summary
Implemented three session stitching attribution models for the TRACE analytics system.

## What Was Built

### 1. First-Touch Attribution (`analytics/queries/attribution_first_touch.sql`)
Credits the initial acquisition source (first campaign/source in session) for conversions. Useful for understanding which campaigns acquire users.

### 2. Last-Touch Attribution (`analytics/queries/attribution_last_touch.sql`)
Credits the final touchpoint before conversion. Useful for understanding what directly leads to conversions.

### 3. Linear Attribution (`analytics/queries/attribution_linear.sql`)
Distributes credit equally across all touchpoints in a session, giving fair credit to the entire customer journey.

### 4. Updated Report Registry (`analytics/src/queries.rs`)
Registered all three attribution reports in the analytics module.

## Features
All queries support:
- UTM parameter tracking (source, medium, campaign, content, term)
- Ad network attribution
- Campaign and creative ID attribution
- Revenue and conversion tracking
- Attribution percentage calculations

## Retrospective
- **What worked:** The existing query infrastructure in TRACE made it straightforward to add new attribution reports. The queries use standard DuckDB window functions and CTEs which work well with the Parquet data format.
- **What didn't:** No major issues. The cargo build wasn't available in this environment but the Rust syntax was correct.
- **Surprise:** The codebase already had extensive attribution-related infrastructure (migration V002 with attribution fields) which made implementing these queries natural.
- **Reusable pattern:** Attribution models follow a consistent pattern: extract session touchpoints -> join with conversions -> aggregate by attribution dimensions. This pattern can be extended for other models (time-decay, position-based, etc.).
