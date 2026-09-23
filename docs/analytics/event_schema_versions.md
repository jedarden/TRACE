# Event Schema Versions

This document defines the schema versions an events query can encounter,
how they map onto each other, and the rules for writing queries that work
across all of them.

There are two version axes, and confusing them is the common source of
"works on new data, empty on old data" bugs:

- **File generations (EV1-EV3)** — what the flusher has written into
  `<prefix>/events/**/*.parquet` over time. Old files are immutable and
  still in the bucket; every version below coexists in the same glob.
- **Table versions (V001-V004)** — the `trace.ad_events` Iceberg schema
  evolution, one `ALTER` migration each, canonically numbered by
  `analytics/schemas/migrations/`. A migrated table presents the *current*
  schema for all rows; rows written before a migration read NULL in the
  added columns.

## File generations

| Generation | Introduced | Columns | `params` physical type |
|---|---|---|---|
| EV1 | `886983d` (initial flusher, 2026-05-08) | `ts, ip, ua, url, type, params` | VARCHAR (JSON string) |
| EV2 | `9dc900f` (Phase 7 session stitching) | EV1 + `session_id, user_id` | VARCHAR (JSON string) |
| EV3 | `cb7ed1e` (params MAP for Iceberg) and later | full current schema — see below | `MAP<STRING, STRING>` |

EV3 is the current flusher schema: the `trace.ad_events` column set
(`analytics/schemas/ad_events_iceberg.sql`), with `params` as a real MAP.
It grew by point releases after `cb7ed1e` (`5d7caa6` referrer, `226551b`
conversion fields, `93af5e5` scroll metrics, and the quality/enrichment
columns), so EV3 files are not byte-identical either — an early EV3 file
can lack the newest columns. All of them share the MAP `params`, which is
the property the compatibility layer keys on.

Layout note: historical flusher generations wrote below `events/`; the
current live flusher writes `<prefix>/<type>/date=<date>/hour=<hour>/`.
Compatibility views currently target the historical `events/` prefix, so
query by `ts` and verify the configured source root before combining layouts.

### Why a plain `read_parquet` glob cannot span them

- Without `union_by_name`, the schema comes from the first file scanned;
  files missing later columns error ("column session_id was not found").
- With `union_by_name`, missing columns are NULL-filled — but `params`
  still has to unify VARCHAR (EV1/EV2) with MAP (EV3). DuckDB has no
  VARCHAR→MAP cast, so the scan fails at query time with "failed to cast
  column params from VARCHAR to MAP". The failure is deferred: creating a
  view over the glob *succeeds*, and the first report run against it fails.

## Table versions

| Version | Migration | Columns added |
|---|---|---|
| V001 | initial schema | base 16 columns (incl. `session_id`, `user_id`, `cookie_id`, `params`) |
| V002 | `V002__add_referrer_attribution.sql` | `referrer`, `referrer_network`, `attribution_campaign_id`, `attribution_creative_id`, `attribution_touches`, `attribution_days_to_convert`, `device_type`, `device_os`, `device_browser` |
| V003 | `V003__add_engagement_metrics.sql` | `scroll_depth_pct`, `scroll_time_ms`, `dwell_time_ms`, `dwell_visible_pct`, `viewport_width`, `viewport_height` |
| V004 | `V004__add_quality_scores.sql` | `quality_score`, `bot_probability`, `fraud_score`, `is_valid`, `is_verified`, `validation_reason`, `enriched_at`, `enrichment_version` |

`analytics/schemas/schema_migrations.sql` re-states these migrations for
Trino-side application with tracking and rollback; its numbering matches
the `migrations/` directory.

### Mapping file generations to table versions

The table's V001 already declares the identity columns the EV1 files lack —
on the Iceberg side those rows are simply never populated. Conversely the
V002-V004 columns exist in the table long before the flusher wrote them:
EV2-era files have none of them, EV3 files have them as they were added.

| Column group | EV1 file | EV2 file | EV3 file | Table (post-V004) |
|---|---|---|---|---|
| base (`ts` … `item_id`) | ✓ | ✓ | ✓ | ✓ |
| `session_id`, `user_id` | — | ✓ | ✓ | ✓ (NULL for EV1-era rows) |
| `cookie_id` … V001 columns | — | — | ✓ | ✓ (NULL for older rows) |
| V002 referrer/attribution/device | — | — | ✓ (post-`5d7caa6`) | ✓ (NULL for older rows) |
| V003 engagement | — | — | ✓ (post-`93af5e5`) | ✓ (NULL for older rows) |
| V004 quality/enrichment | — | — | ✓ (recent) | ✓ (NULL until scored) |
| `params` | JSON VARCHAR | JSON VARCHAR | MAP | MAP |

## Query compatibility rules

1. **Group by identity through the view, never assume it.** `session_id`
   is absent in EV1 data. Session-scoped queries must keep their
   `session_id IS NOT NULL` filter (the shipped templates in
   `analytics/queries/` do); those rows then count as events in
   type/source summaries but not as sessions.
2. **Read `params` with the MAP accessors.** Through the compatibility
   view, `params->>'key'` and `params['key']` work uniformly on JSON-string
   and MAP files. Do not `CAST(params AS MAP)` — that cast does not exist —
   and do not `json_extract` the view's `params` (it is already a MAP).
3. **Treat post-V002 columns as nullable everywhere.** `device_type`,
   `scroll_depth_pct`, `quality_score`, … are NULL on old files by design;
   a report must COALESCE or filter, not assume.
4. **Filter time by `ts`.** The view also exposes a derived `dt` day column
   (`CAST(ts AS DATE)`) so day-range predicates work across the different
   directory layouts. Partition-pruned scans should use the dedicated
   pruning views over homogeneous buckets instead (see
   `docs/analytics/iceberg_partition_pruning.md`).

## The compatibility view

`analytics/src/events_compat.rs` builds `parquet_events` (and
`parquet_events_compacted`) over a mixed glob:

1. **Classify** every file under the glob by its `params` physical type via
   `parquet_schema` (a footer read — no data scan): BYTE_ARRAY = legacy
   JSON string, group node = MAP.
2. **Project** the canonical column set from each side separately —
   columns a side never wrote become typed NULLs — and convert legacy
   `params` to the canonical MAP with
   `map(json_keys(params), list_transform(json_keys(params), k -> json_extract_string(params, '$."' || k || '"')))`,
   gated by `json_valid` so malformed strings become NULL instead of
   erroring.
3. **Union** the sides (a single scan when no legacy files exist, keeping
   the glob live for newly written files).

`analytics/schemas/events_compat_views.sql` holds the same SQL in
operator-usable form for ad-hoc investigation. In the analytics service,
`events_compat::setup_compat_events_views` creates both views and is a
drop-in for the event views in `DuckDBClient::setup_parquet_views`.

Regression safety: `canonical_columns_match_ad_events_ddl`
(`analytics/src/events_compat.rs`) pins the canonical column list to the
`trace.ad_events` DDL, the same discipline the syncer applies to
`trace.assets`.
