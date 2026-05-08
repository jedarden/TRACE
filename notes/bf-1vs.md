# Phase 7: Session Stitching (bf-1vs)

## Summary

Phase 7: Session Stitching was completed in commit `9dc900f`. This bead verified the implementation is complete.

## Implementation

### 1. Collector (`collector/src/main.rs`)
- Added `session_id` and `user_id` fields to Event struct
- Fields extracted from GET query params and POST JSON body
- Support for `trace_session` URL parameter (link decoration)
- Serialized to JSONL logs for downstream processing

### 2. Flusher (`flusher/src/main.rs`)
- Added `session_id` and `user_id` to Parquet schema (nullable)
- Fields properly converted from CollectorEvent
- Available for session-based queries in DuckDB

### 3. Client (`client/trace.js`)
- Session ID generation (UUID4) stored in localStorage
- Session timeout handling (30 minutes of inactivity)
- Link decoration: appends `trace_session` to outbound links
- Cross-site session stitching when `trace_session` present in URL

## Usage Examples

```sql
-- Session-based funnel analysis
SELECT
    session_id,
    COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    MIN(ts) AS session_start,
    MAX(ts) AS session_end
FROM read_parquet('s3://bucket/events/**/*.parquet')
WHERE session_id IS NOT NULL
GROUP BY session_id
ORDER BY session_start DESC
LIMIT 100;
```

## Files Changed (Original Commit)
- `collector/src/main.rs`: Added session_id/user_id extraction
- `flusher/src/main.rs`: Added Parquet schema fields
- `docs/plan/plan.md`: Updated implementation status

## Status
COMPLETE - No additional changes required.
