# Impression Capture

How TRACE records first-party impression events — the top of the funnel the
core funnel query (`docs/analytics/queries/core_funnel.sql`: impression →
click → engagement → conversion) and the impression performance report
(`trace-analytics run impression_performance`) consume
(`type = 'impression'` with `params.imp_id`).

Impressions are counted **first-party**: your own ad slots, native widgets,
or email renders call the tag or pixel when a creative is displayed. Ad
networks' own impression numbers live in the syncer's metrics instead
(`docs/syncer/metrics_polling_worker.md`) — the two are joined by campaign
and creative at query time.

---

## Event contract

| Field | Where it lands in `ad_events` | Required | Notes |
|---|---|---|---|
| `type = 'impression'` | `type` column | yes | What every impression/funnel query filters on |
| `imp_id` | `params.imp_id` | recommended | Impression identity — the dedup key. `COUNT(DISTINCT params->>'imp_id')` counts unique impressions |
| `creative_id` | `params.creative_id` | recommended | Which creative was displayed |
| `ad_slot` | `params.ad_slot` | no | Where it was displayed (two slots can run one creative) |
| `in_view_ms` | `params.in_view_ms` | no | Viewability: continuous on-screen time in ms. Must be a non-negative integer (validated — below) |
| `sid` / `uid` / `cid` | `session_id` / `user_id` / `cookie_id` | for session attribution | Session stitching keys |
| `utm_*`, network params | `params` | recommended | Attribution dimensions read from params |
| anything else | `params` | no | Free-form pass-through (`widget_id`, `variant`, ...) |

`imp` is accepted as an alias for `impression` in the `type` field (the
macro name several ad networks use in impression URLs).

## Capture surfaces

### 1. JS tag API

```html
<script src="tag.min.js" data-collector="/e"></script>
<script>
  // From your ad-render callback:
  TRACE.impression({ creative_id: 'creative-7', ad_slot: 'hero', in_view_ms: 2400 });
  TRACE.impression('creative-7');                        // creative_id shorthand
  TRACE.impression({ imp_id: 'adserver-imp-42' });       // network-supplied ID used verbatim
</script>
```

The tag always sends `type: 'impression'`. When no `imp_id` is supplied it
generates one scoped to the page view (`<page-view-id>:<creative[:slot]>`),
so the same impression re-sent (beacon replay, prefetch double-fire) carries
the same ID and is collapsed downstream — while distinct page views never
collide. Extra keys pass through as params. Sent via `navigator.sendBeacon`
like every other tag event.

### 2. Impression pixel — `GET /i`

For places where a `<script>` cannot run (email, third-party widgets):

```html
<img src="https://trace.example.com/i?imp_id=imp-9&creative_id=creative-2&sid=SESSION&utm_source=taboola&utm_campaign=c-77"
     width="1" height="1" alt="" style="display:none">
```

### 3. Impression postback — `POST /i`

Server-to-server render confirmation (your ad server, email platform).
Body may be JSON or URL-encoded form data:

```bash
curl -X POST https://trace.example.com/i \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'imp_id=imp-11&sid=sess-9&uid=user-9&creative_id=creative-4&utm_source=mgid&utm_campaign=c-12'
```

### 4. Anywhere else

`POST /e` and `GET /p` / `/collect` accept `type=impression` explicitly, as
with any event type.

## Endpoint defaults

The flusher derives the event type from the recorded request path when the
request carries no explicit type:

| Path | Default type (no explicit `type`) |
|---|---|
| `/c` | `conversion` |
| `/i` | `impression` |
| `/p`, `/collect` (GET) | `pageview` |
| `/e`, `/collect` (POST) | `unknown` |

This is what makes bare render pings work: an ad server that can only hit
`GET /i?imp_id=...&sid=...` still produces a `type = 'impression'` row. An
explicit `type` parameter always wins.

## Deduplication

Impressions are the one event type with a dedup story, because render
callbacks and pixels fire repeatedly by accident far more often than
conversions do:

1. **Tag** — one impression per key per page view. `TRACE.impression()`
   drops repeat calls with the same `imp_id` (or creative/slot
   combination) before anything is sent.
2. **Flusher** — within one hourly raw log file, impression events sharing
   an `imp_id` collapse to the first arrival (`dedupe_impressions` in
   `flusher/src/main.rs`). Duplicate sends land in the same hour file, so
   this catches beacon replays and postback retries the tag could not.
   Impressions without an `imp_id` are never dropped — there is nothing to
   key on. Duplicates that straddle an hour boundary survive; the query
   layer handles those.
3. **Query** — reports count unique impressions as
   `COUNT(DISTINCT params->>'imp_id')` alongside raw `COUNT(*)`, so any
   residual duplication is visible and exclude-able.

The collector itself stays at-least-once with no dedup by design (see
[`collector-api.md`](collector-api.md) — "Idempotency"); dedup belongs to
the layer that can see `imp_id`.

## Validation

- **`in_view_ms`** must be a non-negative integer. Anything else is dropped
  at parse time (`sanitize_in_view_ms`), because the impression reports
  cast `(params->>'in_view_ms')::BIGINT` — one malformed value would fail
  every one of them, the same protection `revenue` gets.
- Everything else is stored raw. The collector performs no event
  validation at all (log-first); the flusher parses, types, and sanitizes.

## Persistence

Impression events partition under `type=impression/date=YYYY-MM-DD/hour=HH/`
like every other event type, in the same `ad_events` Parquet schema —
`session_id`/`user_id`/`cookie_id` typed columns from `sid`/`uid`/`cid`,
everything else in the `params` map.

## Pipeline notes

- **Collector** stores the request raw (`collector/src/main.rs`); `/i` is
  routed GET (pixel GIF) and POST (postback), path recorded bare.
- **Flusher** (`flusher/src/raw_log_parser.rs`) parses the raw line, applies
  the endpoint default above, extracts `sid`/`uid`/`cid` from JSON *and*
  form-encoded bodies, sanitizes `in_view_ms`, and (in
  `flusher/src/main.rs`) dedupes by `imp_id` per raw file.
- **Round-trip tests**: `flusher/src/main.rs` parses a raw log line and
  reads the resulting Parquet back (`type` column + `params` map) for the
  tag event, the bare pixel, and a malformed `in_view_ms`;
  `client/test/impression.test.mjs` covers the tag API including the dedup
  guarantee; collector `contract_*` tests pin the `/i` pixel and postback
  responses.

## Verifying impressions are flowing

```sql
SELECT COUNT(*) AS impressions,
       COUNT(DISTINCT params->>'imp_id') AS unique_impressions,
       AVG(TRY_CAST(params->>'in_view_ms' AS BIGINT)) AS avg_in_view_ms
FROM ad_events
WHERE type = 'impression';
```

or `trace-analytics run impression_performance --start-date ... --end-date ...`.
