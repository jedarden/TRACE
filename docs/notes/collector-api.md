# Collector HTTP API Contract

The complete request/response contract for the TRACE collector's ingestion
surface — `POST /collect`, the pixel endpoints, their aliases, status codes,
body size limit, malformed-input behavior, and retry/idempotency semantics.
The conformance tests live in `collector/src/main.rs` (`contract_*` tests in
`mod tests`); every claim below that affects a client is pinned by one of
them. Conversion-specific semantics (`/c` defaults, revenue handling) are in
[`conversion-capture.md`](conversion-capture.md); impression semantics
(`/i` defaults, `imp_id` dedup) in
[`impression-capture.md`](impression-capture.md).

---

## Design stance

The collector is **log-first**: it validates transport concerns only —
routing, method, body size, UTF-8 — and never inspects event content. There
is no schema, no required field, and no event-type validation at ingestion.
Every accepted request becomes exactly one JSON line in the current hour's
raw log (`raw-YYYYMMDD-HH.jsonl`, UTC); all parsing and typing happens in
the flusher. A direct consequence: **the collector never rejects a payload
for being malformed** — only for being untransportable (too large, or not
UTF-8 text).

## Endpoints

| Path | Methods | Purpose | Success response |
|---|---|---|---|
| `/collect` | GET, POST | Combined endpoint — the JS tag's default `data-collector` target | GET: 200 GIF; POST: 204 |
| `/e` | POST | JS-tag events (pageview, scroll, heartbeat, click, …) | 204 |
| `/p` | GET | Pageview pixel | 200 GIF |
| `/c` | GET, POST | Conversion pixel / server-to-server postback | GET: 200 GIF; POST: 204 |
| `/i` | GET, POST | Impression pixel / server-to-server postback | GET: 200 GIF; POST: 204 |
| `/health` | GET | Liveness probe | 200, body `OK`, nothing logged |

`/collect` exists so deployments need one URL for both ingestion paths; `/e`
and `/p` are aliases whose only difference is the flusher's default event
type when the request carries no explicit `type` (`/p` and `GET /collect`
default to `pageview`, `/e` and `POST /collect` to `unknown`, `/c` to
`conversion`, `/i` to `impression`).

## Request contract

### JS-tag path — `POST /collect` (or `/e`, `/c`, `/i`)

- **Body**: any UTF-8 text up to the size limit. The tag sends a JSON
  object, labeled `application/json` by both its `sendBeacon` blob and its
  `fetch` fallback; other embedders commonly deliver beacons as
  `text/plain`, and ad-network postbacks use
  `application/x-www-form-urlencoded`. **Content-Type is not inspected** —
  format detection happens in the flusher (JSON first, then form encoding).
- **Query string**: may accompany the body; it is recorded but the flusher
  derives event params from the POST body only.
- **Headers captured** (everything else is dropped — notably, no cookies
  are ever read or stored):

  | Header | Recorded as | Notes |
  |---|---|---|
  | `User-Agent` | `headers.user_agent` | |
  | `Referer` | `headers.referer` | |
  | `X-Forwarded-For` | `headers.x_forwarded_for` + `client_ip` | `client_ip` is the **first hop**, trimmed |
  | `X-Real-IP` | `headers.x_real_ip` (+ `client_ip` fallback) | used for `client_ip` only when XFF is absent |
  | `Accept-Language` | `headers.accept_language` | |
  | `Accept-Encoding` | `headers.accept_encoding` | |

- **Timestamp**: `ts` is the collector's own receive clock (RFC 3339 UTC).
  The client's `ts` inside the body is event data, not the record's
  timestamp — the collector does not trust client clocks.
- **Client IP**: first `X-Forwarded-For` hop, else `X-Real-IP`, else
  absent. The socket address is never used — the collector assumes it sits
  behind a proxy and would otherwise record the proxy's IP.

### Pixel path — `GET /collect` (or `/p`, `/c`, `/i`)

- **Query string**: stored verbatim (still percent-encoded, undecoded).
  **No parameters are required** — a bare `GET /p` succeeds and is logged;
  the flusher's endpoint defaults (above) type it.
- **Body**: GET bodies are ignored entirely (not logged).
- Same header capture and client-IP rules as the POST path.

## Response contract

| Status | When | Body | Logged? |
|---|---|---|---|
| `200` | GET endpoints | 35-byte 1×1 transparent GIF89a, `Content-Type: image/gif` | yes |
| `204` | POST endpoints | empty | yes |
| `400` | body is not valid UTF-8 | error text | **no** |
| `404` | unknown path | error text | **no** |
| `405` | method not routed for that path (e.g. `POST /p`, `GET /e`, `PUT /collect`, `OPTIONS /collect`) | error text | **no** |
| `413` | body exceeds 2 MiB | error text | **no** |

Rejected requests never touch the log — a 4xx has no side effects.

Two caveats clients should know about:

- **A 2xx means "accepted into the in-memory buffer", not "durable".** If
  the append to the log file fails (disk full, I/O error), the collector
  logs the error server-side and still returns success; the event is lost.
  Durability arrives at hourly rotation / graceful shutdown, when the file
  is flushed, fsynced, and renamed to `.ready` for the flusher.
- **The pixel response carries no cache headers.** The GIF may be cached by
  browsers/proxies for a repeated identical URL. Pixel URLs that repeat
  verbatim should include a varying parameter (`ts`, cache-buster) if every
  hit must register.

## Size limit

Bodies are capped at **2 MiB (2,097,152 bytes), inclusive** — enforced via
axum's `DefaultBodyLimit` (set explicitly in `collector/src/main.rs` as
`MAX_EVENT_BODY_BYTES`). A body of exactly 2 MiB is accepted; one byte over
is rejected with `413` and discarded. GET requests carry no body, so the
limit binds only the POST endpoints. There is no explicit query-string
length limit; practical limits come from the HTTP stack and any fronting
proxy.

## Malformed input

| Input | Collector behavior | Downstream (flusher) |
|---|---|---|
| Invalid JSON body | accepted `204`, stored verbatim | JSON parse fails → body tried as form encoding → event lands as its endpoint default type (usually `unknown`) with whatever params survive |
| Unparseable-as-anything body | accepted `204`, stored verbatim | same as above; raw body remains in the log for replay |
| Non-numeric `revenue` | accepted, stored verbatim | param dropped at parse time (protects the attribution SQL casts) |
| Non-integer or negative `in_view_ms` | accepted, stored verbatim | param dropped at parse time (protects the impression report's `::BIGINT` casts) |
| Empty body | accepted `204`, logged as `""` | endpoint default type, no params |
| Empty query string | accepted `200` GIF | endpoint default type, no params |
| Non-UTF-8 body | **rejected `400`**, discarded | — (never logged) |
| Oversized body | **rejected `413`**, discarded | — (never logged) |

Because raw logs are the source of truth, nothing is ever "fixed up" at
ingestion — a malformed event can be reprocessed later by replaying the log
with improved flusher logic.

## Retry

- **2xx is terminal.** The tag is fire-and-forget (`sendBeacon` with a
  `fetch` keepalive fallback); no retry on success.
- **4xx is permanent.** A 400/413/405 will fail identically on retry — fix
  the payload, don't resend it.
- **Transport failures** (connection refused, timeout, 5xx): retrying is
  *safe* but *duplicating* — see idempotency below. For the browser tag the
  practical answer is not to retry; for server-to-server postbacks, retry
  with an idempotency key (below).
- **CORS**: the collector serves no CORS headers and no `OPTIONS` route
  (a preflight gets 405). `sendBeacon` never preflights, so tag events
  still arrive cross-origin fire-and-forget; but the `fetch` fallback with
  `Content-Type: application/json` does preflight and will fail
  cross-origin. Deploy first-party (tag and collector on the same site —
  the documented `trace.example.com` pattern), or terminate CORS at the
  fronting proxy.

## Idempotency

There is none, by design, at any layer that matters to callers:

- The collector is **append-only and at-least-once**: one log line per
  *accepted request*, so a client that retries a delivered event produces
  duplicate rows in `ad_events`. There is no event ID, dedupe key, or
  upsert at ingestion. The one exception downstream: impression events
  carrying an `imp_id` are deduplicated in the flusher, per raw log file —
  see [`impression-capture.md`](impression-capture.md).
- Consequences: ad-network postback retries and beacon replays inflate
  counts. Callers that cannot avoid retrying (S2S webhooks) should include
  a natural idempotency key as an ordinary parameter (`order_id`,
  `transaction_id` — or `imp_id` for impressions) so later processing can
  collapse duplicates.
- The flip side of no-ingestion-state: the collector holds no per-client
  session and cannot create retry loops or ordering artifacts; a request's
  position in the log is its arrival order, nothing more.

## The recorded line

One JSON object per accepted request (schema shared with the flusher's
`RawRequest`):

```json
{
  "ts": "2026-09-17T12:34:56.789012+00:00",
  "method": "POST",
  "path": "/collect",
  "headers": { "user_agent": "…", "referer": "…", "x_forwarded_for": "…" },
  "query_params": "src=tag&v=2",
  "body": "{\"type\":\"pageview\",…}",
  "client_ip": "203.0.113.7"
}
```

`path` is the bare URL path (never includes the query string);
`query_params` and `body` are stored raw and verbatim; absent fields
(`query_params` on a queryless request, `body` on GETs) are `null`.

## Conformance

The `contract_*` tests in `collector/src/main.rs` drive the **real router**
(`build_app` — the same stack `main` serves, body limit and
framework-generated rejections included), not the handlers directly:

```bash
cd collector && cargo test contract
```

They pin: 204/empty for POSTs, the exact GIF bytes and `image/gif` type for
pixels, verbatim body/query recording, the filtered-header set, XFF
first-hop IP resolution, content-type indifference, malformed-JSON
acceptance, empty-body acceptance, 400 for non-UTF-8, the inclusive 2 MiB
limit, 405/404 with no log side effects, `/health`, the `/i` impression
pixel and postback, and one-line-per-delivery duplicate semantics.
