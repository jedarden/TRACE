# Conversion Capture

How TRACE records first-party conversion events with revenue — the events the
Campaign ROI query (`docs/analytics/queries.md`), all four attribution models
(`docs/analytics/attribution_and_funnel_queries.md`), and the README's
funnel-conversion metric consume (`type = 'conversion'` with
`params.revenue`).

---

## Event contract

| Field | Where it lands in `ad_events` | Required | Notes |
|---|---|---|---|
| `type = 'conversion'` | `type` column | yes | What every attribution/ROI query filters on |
| `conversion_type` | `params.conversion_type` | recommended | What kind: `purchase`, `lead`, `signup`, ... |
| `revenue` | `params.revenue` | for revenue queries | Numeric string; `SUM((params->>'revenue')::DECIMAL)` |
| `sid` / `uid` / `cid` | `session_id` / `user_id` / `cookie_id` | for session attribution | Session stitching keys |
| `utm_*`, network params | `params` | recommended | Attribution dimensions read from params |
| anything else | `params` | no | Free-form pass-through (`currency`, `order_id`, ...) |

`conversion_ts` in the attribution docs is derived (`MIN(ts)` over the
conversion events), not stored; `conversion_type` in `attribution_analysis`
is the event's `type` column.

`purchase` and `signup` are also first-class event types (they partition as
`purchase/` and `signup/`), because `attribution_analysis` filters on
`type IN ('conversion', 'purchase', 'signup')`.

## Capture surfaces

### 1. JS tag API

```html
<script src="tag.min.js" data-collector="/e"></script>
<script>
  // On the order-confirmation page:
  TRACE.conversion({ conversion_type: 'purchase', revenue: 49.99, currency: 'USD' });
  TRACE.conversion('signup');            // string shorthand, no revenue
  TRACE.conversion({ type: 'lead' });    // options.type becomes conversion_type
</script>
```

The tag always sends `type: 'conversion'`; the conversion kind travels in
`conversion_type` so the event type stays queryable. Extra keys pass through
as params. Sent via `navigator.sendBeacon` like every other tag event.

### 2. Conversion pixel — `GET /c`

For places where a `<script>` cannot run:

```html
<img src="https://trace.example.com/c?conversion_type=purchase&revenue=49.99&sid=SESSION"
     width="1" height="1" alt="" style="display:none">
```

### 3. Conversion postback — `POST /c`

Server-to-server confirmation (ad networks, order webhooks). Body may be
JSON or URL-encoded form data — the format ad-network postbacks actually
use:

```bash
curl -X POST https://trace.example.com/c \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'sid=sess-9&uid=user-9&conversion_type=purchase&revenue=33.75&utm_source=taboola&utm_campaign=c-77'
```

### 4. Anywhere else

`POST /e` and `GET /p` accept `type=conversion` (or `purchase`/`signup`)
explicitly, as with any event type.

## Endpoint defaults

The flusher derives the event type from the recorded request path when the
request carries no explicit type:

| Path | Default type (no explicit `type`) |
|---|---|
| `/c` | `conversion` |
| `/p`, `/collect` (GET) | `pageview` |
| `/e`, `/collect` (POST) | `unknown` |

This is what makes bare postback pings work: a network that can only hit
`GET /c?sid=...&uid=...` still produces a `type = 'conversion'` row, keyed to
the session via `sid`. An explicit `type` parameter always wins.

## Pipeline notes

- **Collector** stores the request raw (`collector/src/main.rs`); the path is
  recorded as the bare URL path (`/c`), the query string separately.
- **Flusher** (`flusher/src/raw_log_parser.rs`) parses the raw line, applies
  the endpoint default above, and extracts `sid`/`uid`/`cid` from JSON *and*
  form-encoded bodies. Events partition under `type=conversion/date=.../hour=...`
  like every other type.
- **Revenue safety**: a `revenue` param that is not a finite number is
  dropped at parse time. One malformed value would otherwise fail the
  `(params->>'revenue')::DECIMAL` cast and break every documented attribution
  query for the affected window.
- **Round-trip tests**: `flusher/src/main.rs` parses a raw log line and reads
  the resulting Parquet back (`type` column + `params` map) to assert the
  contract end to end; `client/test/conversion.test.mjs` covers the tag API;
  collector tests cover pixel and postback logging.

## Verifying conversions are flowing

```sql
SELECT COUNT(*) AS conversions,
       SUM((params->>'revenue')::DECIMAL) AS revenue
FROM ad_events
WHERE type = 'conversion';
```

or `trace-analytics run attribution_first_touch --start-date ... --end-date ...`.
