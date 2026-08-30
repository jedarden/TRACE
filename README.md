# 📡 TRACE

**Traffic Recording, Attribution, and Campaign Events**

---

TRACE is a lightweight, self-hosted event tracking system built for affiliate marketing and direct response advertising campaigns. It captures raw traffic signals as users move through your properties and attributes performance back to the campaigns, creatives, and assets that drove them.

## 🎯 What It Does

- **Captures everything** — A single JS tag or pixel on your pages records every pageview, click, scroll, and dwell event with the full URL and query parameters intact
- **Zero configuration** — No tracking links to register, no schemas to define, no campaigns to pre-configure. New parameters, campaigns, and creatives appear automatically when traffic arrives
- **Attributes to assets** — Maps ad-level performance back to the individual headlines, images, and landing pages that composed each ad by syncing creative data from ad network APIs
- **Cross-network normalization** — Unifies macro naming across Taboola, Outbrain, MGID, RevContent, and other native ad platforms into a single queryable format
- **Funnel stitching** — Tracks users across multiple pages and events using first-party cookies and link decoration, connecting the full journey from ad click to conversion

## 🏗️ Architecture

```
┌──────────────────────┐
│  Your Pages + JS Tag │
│  (pixel / img tag)   │
└──────────┬───────────┘
           │ raw HTTP requests
           ▼
┌──────────────────────┐
│  Collector           │
│  append line to file │
└──────────┬───────────┘
           │ buffer rotation
           ▼
┌──────────────────────┐
│  Flusher             │
│  parse → Parquet     │
│  upload to S3        │
└──────────┬───────────┘
           │ partitioned files
           ▼
┌──────────────────────┐
│  Iceberg Tables      │
│  + compaction        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  DuckDB / SQL        │
│  queries & reports   │
└──────────────────────┘
```

## 💡 Design Principles

- **📝 Log first, parse later** — The collector is a glorified access log. Raw requests are stored as-is. All parsing, normalization, and enrichment happens downstream in ETL
- **🗺️ Dynamic schema** — Query parameters are stored as `MAP<STRING, STRING>` in Parquet. No schema migrations when you add a new UTM or custom parameter
- **👀 Observation only** — Nothing needs to be registered or configured before use. Assets, parameters, and campaigns are discovered from the data itself
- **🔄 Reprocessable** — Raw event logs are the source of truth. If ETL logic improves, replay from the beginning
- **🪶 Minimal infrastructure** — At typical affiliate volumes (~15-20k events/day), the entire system runs on a single container with DuckDB for analytics. No Kafka, no Spark, no Flink

## 📊 What You Can Measure

| Metric | How |
|---|---|
| 🖱️ **Click-through rate** | Clicks / pageviews per campaign, ad, or asset |
| ⏱️ **Dwell time** | Heartbeat pings from the JS tag measure time on page |
| 🔀 **Funnel conversion** | Sessionized events from landing page through checkout |
| 🏆 **Asset performance** | Headline and image effectiveness across all ad combinations |
| 📉 **Creative fatigue** | Performance decay of individual assets over time |
| 🌐 **Cross-network comparison** | Same creative tested across multiple traffic sources |

## 📁 Project Structure

```
TRACE/
├── collector/        # HTTP event collector
├── flusher/          # raw-event to Parquet pipeline
├── syncer/           # object-store synchronization
├── compactor/        # Parquet compaction
├── analytics/        # DuckDB-backed query service
├── client/           # browser tracking client
├── docs/
│   ├── research/    # Market research, competitive analysis, technical spikes
│   └── plan/        # Architecture decisions, implementation phases, roadmap
├── docker-compose.yml
└── README.md
```

## 🚀 Status

**Implemented prototype, not a turnkey release.** The collector, flusher,
syncer, compactor, analytics service, and browser client exist with tests.
There are no published binaries or container images, and the current Compose
file expects an external S3-compatible store and Iceberg REST catalog. Expect
to supply and operate that infrastructure before evaluating the full pipeline.

## 📄 License

Apache-2.0 — see [LICENSE](LICENSE).
