-- ============================================================================
-- Event Schema Compatibility Views (operator / ad-hoc form)
-- ============================================================================
-- Spans the three raw-Parquet file generations (EV1/EV2/EV3 — see
-- docs/analytics/event_schema_versions.md) in one view with the canonical
-- column set, so the report templates in analytics/queries/ run unchanged
-- against buckets holding old and new files.
--
-- The analytics service builds this programmatically via
-- analytics/src/events_compat.rs (events_compat::setup_compat_events_views,
-- a drop-in for the event views in DuckDBClient::setup_parquet_views).
-- This file is the same SQL by hand, for ad-hoc investigation in the
-- duckdb CLI. Substitute <GLOB> (e.g. 's3://my-trace-bucket/trace-events/events/**/*.parquet')
-- everywhere it appears.
--
-- Why two steps: DuckDB cannot unify a VARCHAR `params` column (EV1/EV2
-- files, JSON strings) with a MAP one (EV3 files) in a single scan —
-- `read_parquet(..., union_by_name=true)` fails at query time with
-- "failed to cast column params from VARCHAR to MAP", *after* the view
-- was created successfully. So the files are classified by footer first
-- (cheap — `parquet_schema` reads metadata only), then each class is
-- scanned separately and the results unioned.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: classify the files under the glob.
-- ----------------------------------------------------------------------------
-- In a Parquet footer a MAP column is a group node (type IS NULL, with a
-- repeated key_value child); a VARCHAR column is a BYTE_ARRAY leaf.
-- map_params = 0  ->  legacy JSON-string params (EV1/EV2)
-- map_params = 1  ->  canonical MAP params (EV3)

-- SELECT file_name,
--        MAX(CASE WHEN name = 'params' AND type IS NULL THEN 1 ELSE 0 END) AS map_params
-- FROM parquet_schema('<GLOB>')
-- GROUP BY file_name;

-- Paste the legacy files into the list literal of the first SELECT below
-- and the MAP files into the second. If one class is empty, delete that
-- SELECT and the UNION ALL between them.

-- ----------------------------------------------------------------------------
-- Step 2: the view.
-- ----------------------------------------------------------------------------
-- Each side references only columns that exist somewhere in that class
-- (union_by_name = true NULL-fills the rest within the side); columns the
-- class never wrote are synthesized as typed NULLs. Trim the NULL::<type>
-- entries for columns your files do have — the full canonical list, with
-- types, is CANONICAL_COLUMNS in analytics/src/events_compat.rs, pinned to
-- the trace.ad_events DDL by a regression test.

CREATE OR REPLACE VIEW parquet_events AS
-- Legacy side: params arrives as a JSON string and is converted to the
-- canonical MAP. json_valid gates it: NULL and malformed strings (the old
-- flusher wrote '' when serialization failed) become NULL instead of
-- erroring the query.
SELECT
    ts, ip, ua, url, type,
    session_id, user_id,
    NULL::VARCHAR AS cookie_id,
    NULL::VARCHAR AS network,
    NULL::VARCHAR AS campaign_id,
    NULL::VARCHAR AS campaign_name,
    NULL::VARCHAR AS creative_id,
    NULL::VARCHAR AS headline,
    NULL::VARCHAR AS image_id,
    NULL::VARCHAR AS item_id,
    CASE WHEN json_valid(params)
         THEN map(json_keys(params),
                  list_transform(json_keys(params),
                                 k -> json_extract_string(params, '$."' || k || '"')))
         ELSE NULL END AS params,
    NULL::VARCHAR AS referrer,
    NULL::VARCHAR AS referrer_network,
    NULL::VARCHAR AS attribution_campaign_id,
    NULL::VARCHAR AS attribution_creative_id,
    NULL::BIGINT AS attribution_touches,
    NULL::BIGINT AS attribution_days_to_convert,
    NULL::VARCHAR AS device_type,
    NULL::VARCHAR AS device_os,
    NULL::VARCHAR AS device_browser,
    NULL::BIGINT AS scroll_depth_pct,
    NULL::BIGINT AS scroll_time_ms,
    NULL::BIGINT AS dwell_time_ms,
    NULL::BIGINT AS dwell_visible_pct,
    NULL::BIGINT AS viewport_width,
    NULL::BIGINT AS viewport_height,
    NULL::DOUBLE AS quality_score,
    NULL::DOUBLE AS bot_probability,
    NULL::DOUBLE AS fraud_score,
    NULL::BOOLEAN AS is_valid,
    NULL::BOOLEAN AS is_verified,
    NULL::VARCHAR AS validation_reason,
    NULL::TIMESTAMP AS enriched_at,
    NULL::VARCHAR AS enrichment_version,
    CAST(ts AS DATE) AS dt
FROM read_parquet(
    ['<LEGACY FILES>'],
    union_by_name = true
)

UNION ALL

-- Current side: params is already the canonical MAP. With no legacy files
-- at all, keep only this side but scan the glob instead of a file list, so
-- newly written files are picked up without rebuilding the view:
--   FROM read_parquet('<GLOB>', union_by_name = true)
SELECT
    ts, ip, ua, url, type,
    session_id, user_id, cookie_id,
    network, campaign_id, campaign_name,
    creative_id, headline, image_id, item_id,
    params,
    referrer, referrer_network,
    attribution_campaign_id, attribution_creative_id,
    attribution_touches, attribution_days_to_convert,
    device_type, device_os, device_browser,
    scroll_depth_pct, scroll_time_ms,
    dwell_time_ms, dwell_visible_pct,
    viewport_width, viewport_height,
    quality_score, bot_probability, fraud_score,
    is_valid, is_verified, validation_reason,
    enriched_at, enrichment_version,
    CAST(ts AS DATE) AS dt
FROM read_parquet(['<MAP FILES>'], union_by_name = true);

-- ----------------------------------------------------------------------------
-- Notes
-- ----------------------------------------------------------------------------
-- * `dt` is derived from `ts` (not a hive partition column) so day-range
--   predicates work across every directory layout the flusher has used
--   (flat, dt=/hour=, <type>/date=/hour=). The trade-off is no file
--   skipping; for partition-pruned scans over homogeneous buckets use the
--   pruning views instead (docs/analytics/iceberg_partition_pruning.md).
-- * Keep session-scoped queries filtered on `session_id IS NOT NULL`:
--   EV1 rows carry no identity and must count as events, not sessions.
-- * Both `params->>'key'` and `params['key']` read the unified params
--   column; `CAST(params AS MAP)` does not exist and will error.
-- ============================================================================
