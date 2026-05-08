# Genesis Bead: TRACE Implementation

**Bead ID:** bf-1au
**Tied to plan:** `/home/coding/TRACE/docs/plan/plan.md`
**Repository:** jedarden/TRACE

## Overview

TRACE (Traffic Recording, Attribution, and Campaign Events) is a lightweight, self-hosted event tracking system for affiliate marketing and direct response advertising.

## Project Status

### Phase 1: Core Collection Infrastructure ✅ COMPLETE
- Collector service (Rust, Axum) — HTTP endpoint, JSONL logging, hourly rotation
- Flusher service (Rust) — file watching, Parquet conversion, S3 upload
- Dockerfiles and docker-compose.yml
- K8s manifests (deployments, services, PVCs)

### Phase 2: CI/CD & Deployment ✅ COMPLETE
- Collector CI workflow template
- Flusher CI workflow template
- K8s deployment manifests ready

### Phase 3: Client Integration 📋 PLANNED
- JavaScript tracking tag
- Autocapture pageviews
- Link decoration for session stitching
- Dwell time heartbeats

### Phase 4: Attribution & Campaign Sync 📋 PLANNED
- Ad network API integration (Taboola, Outbrain, MGID, RevContent)
- Creative registry
- Asset tagging and performance attribution

### Phase 5: Analytics & Reporting 📋 PLANNED
- DuckDB queries for common metrics
- Parquet compaction strategy
- Optional Iceberg + Trino for scale

### Phase 6: Advanced Features 📋 PLANNED
- Session stitching
- Fraud detection
- Real-time dashboard

## Design Principles

1. **Log first, parse later** — Raw requests stored as-is, enrichment downstream
2. **Dynamic schema** — Query params as MAP, no migrations for new params
3. **Observation only** — No pre-registration, assets discovered from data
4. **Reprocessable** — Raw logs are source of truth, replayable
5. **Minimal infrastructure** — Single container + DuckDB at typical volumes

## Next Steps

1. Deploy collector and flusher to ardenone-manager cluster
2. Set up S3 bucket and IAM credentials
3. Implement JavaScript client tag
4. Begin Phase 4 (ad network integration)
