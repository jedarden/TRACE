# DuckDB: Analytics query layer over Iceberg tables

## Summary

Bead bf-3j4 delivered a comprehensive DuckDB analytics query layer with full support for both Apache Iceberg tables and legacy Parquet files. The implementation includes automatic backend selection, a web UI for ad-hoc queries, 24 pre-built analytics reports, and a scheduled report runner.

## Components Delivered

### 1. DuckDB Client with Iceberg Extension (`analytics/src/duckdb.rs`)
- In-memory DuckDB with configurable extensions (httpfs, iceberg)
- S3/MinIO credential configuration
- Automatic backend selection based on configuration
- Helper methods for table references (events, campaigns, creatives)
- JSON and CSV output formatting

### 2. Web UI for Ad-Hoc Queries (`analytics/src/web.rs` + `static/index.html`)
- SQL query endpoint (`/api/query`)
- Report listing API (`/api/reports`)
- Report execution with parameters (`/api/reports/run`)
- Dual-format response (CSV + JSON)
- Health check endpoint

### 3. 24 Pre-Built Analytics Reports (`analytics/src/queries.rs`)
- **Daily Reports**: daily_summary, ctr_by_campaign, top_headlines, top_images, creative_fatigue
- **Campaign Metrics**: campaign_funnel, network_comparison, cross_network_creatives
- **Attribution**: first_touch, last_touch, linear, attribution_analysis
- **User Journey**: session_flow, user_journey, common_paths, cohort_journey, funnel_with_paths
- **Alerting**: traffic_spike_detection, zero_traffic_alert
- **Time Analysis**: trending_campaigns, hourly_traffic_pattern

### 4. Scheduled Report Runner (`analytics/src/reporter.rs`)
- Daily report execution (CTR summary, top performers, fatigued creatives)
- Daemon mode with configurable interval
- Both JSON and CSV output
- K8s CronJob manifest support

### 5. CLI Interface (`analytics/src/main.rs`)
- Run specific reports by name
- List all available reports
- Execute raw SQL queries
- Start web UI server
- Daily reports one-shot execution

## Retrospective

### What worked
- **Dual-backend design**: The template-based query rendering with `{{events_table}}` placeholder enables seamless migration from Parquet to Iceberg without modifying query files
- **Modular architecture**: Clear separation between DuckDB client, query definitions, report runner, and web server
- **Comprehensive testing**: Unit tests for JSON/CSV formatting, template rendering, and report categories

### What didn't
- **Initial iceberg_scan() syntax**: The first attempt at using DuckDB's Iceberg extension had incorrect syntax; fixed by using the proper `catalog_uri` parameter format
- **View management**: Initial implementation mixed view creation with query execution; refactored to separate concerns

### Surprise
- **DuckDB's Iceberg extension maturity**: The extension supports partition pruning and performs well with S3-backed Iceberg tables
- **Template system flexibility**: The simple `{{events_table}}` placeholder approach works well for both backends

### Reusable Pattern
For analytics layers supporting multiple backends:
1. Use template placeholders for table references
2. Create view-based rendering that abstracts backend differences
3. Implement helper methods that return appropriate SQL fragments based on configuration
4. Support both legacy (Parquet) and modern (Iceberg) modes for migration flexibility

## Files Modified/Created

- `analytics/src/duckdb.rs` - DuckDB client with Iceberg support
- `analytics/src/queries.rs` - Report definitions and template rendering
- `analytics/src/reporter.rs` - Scheduled and one-shot report execution
- `analytics/src/web.rs` - Web UI server
- `analytics/src/main.rs` - CLI entry point
- `analytics/src/config.rs` - Configuration management
- `analytics/static/index.html` - Web UI frontend
- `analytics/queries/*.sql` - 24 pre-built analytics queries

## Related Commits

- 08a5774 - Initial close (bead status not updated)
- d930ba5, f894bb5 - rust-s3 upgrade
- f8b03a6 - CSV/JSON download buttons
- a7829f5 - Dual-format response and daily reports
- b104b7b - DuckDB query layer with Iceberg support
