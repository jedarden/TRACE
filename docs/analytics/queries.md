# TRACE Analytics Queries

## DuckDB Setup

### Reading Parquet from S3

```sql
-- Install and load S3 extension
INSTALL httpfs;
LOAD httpfs;

-- Set S3 credentials
SET s3_region='us-east-1';
SET s3_access_key_id='YOUR_ACCESS_KEY';
SET s3_secret_access_key='YOUR_SECRET_KEY';

-- Or use IAM role (no credentials needed)
SET s3_use_ssl=true;
```

### Basic Event Query

```sql
-- Read all events
SELECT *
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
LIMIT 100;
```

## Core Metrics

### Click-Through Rate (CTR)

```sql
-- CTR by campaign
SELECT
    params->>'utm_campaign' AS campaign,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * clicks / NULLIF(views, 0),
        2
    ) AS ctr_pct
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 50;
```

### Daily Event Summary

```sql
-- Events by day and type
SELECT
    DATE(ts) AS date,
    type,
    COUNT(*) AS events,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Hourly Traffic Pattern

```sql
-- Traffic by hour of day
SELECT
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    type,
    COUNT(*) AS events
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1, 2;
```

## Campaign Performance

### Campaign Funnel

```sql
-- Conversion funnel by campaign
WITH funnel AS (
    SELECT
        params->>'utm_campaign' AS campaign,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        COUNT(*) FILTER (WHERE type = 'dwell') AS dwells
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
)
SELECT
    campaign,
    pageviews,
    clicks,
    ROUND(100.0 * clicks / NULLIF(pageviews, 0), 2) AS click_through_pct,
    scrolls,
    ROUND(100.0 * scrolls / NULLIF(clicks, 0), 2) AS scroll_after_click_pct,
    dwells,
    ROUND(100.0 * dwells / NULLIF(scrolls, 0), 2) AS dwell_after_scroll_pct
FROM funnel
ORDER BY pageviews DESC
LIMIT 20;
```

### Campaign ROI (if you have conversion data)

```sql
-- Campaign performance with conversions
SELECT
    params->>'utm_campaign' AS campaign,
    params->>'utm_source' AS source,
    params->>'utm_medium' AS medium,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE type = 'conversion') AS conversions,
    COALESCE(SUM(
        (params->>'revenue')::DECIMAL
    ) FILTER (WHERE type = 'conversion'), 0) AS revenue,
    ROUND(
        COALESCE(SUM((params->>'revenue')::DECIMAL) FILTER (WHERE type = 'conversion'), 0) /
        NULLIF(COUNT(*) FILTER (WHERE type = 'click'), 0),
        2
    ) AS revenue_per_click
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1, 2, 3
ORDER BY revenue DESC
LIMIT 20;
```

## Asset Performance

### Top Headlines

```sql
-- Performance by Taboola headline
SELECT
    params->>'tb_headline' AS headline,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params->>'tb_headline' IS NOT NULL
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 50;
```

### Top Images

```sql
-- Performance by creative image
SELECT
    params->>'tb_image' AS image_id,
    params->>'utm_source' AS source,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    COUNT(DISTINCT params->>'tb_headline') AS num_headlines
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params->>'tb_image' IS NOT NULL
    AND ts >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY 1, 2
ORDER BY clicks DESC
LIMIT 50;
```

### Creative Combinations

```sql
-- Best headline + image combinations
SELECT
    params->>'tb_headline' AS headline,
    params->>'tb_image' AS image,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE params->>'tb_headline' IS NOT NULL
    AND params->>'tb_image' IS NOT NULL
    AND ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1, 2
HAVING COUNT(*) FILTER (WHERE type = 'click') >= 10
ORDER BY clicks DESC
LIMIT 20;
```

## Cross-Network Analysis

### Network Comparison

```sql
-- Compare performance across ad networks
SELECT
    params->>'utm_source' AS network,
    COUNT(*) FILTER (WHERE type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE type = 'pageview'), 0),
        2
    ) AS ctr,
    COUNT(DISTINCT params->>'utm_campaign') AS num_campaigns,
    COUNT(DISTINCT params->>'tb_headline') AS num_headlines
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1
ORDER BY clicks DESC;
```

### Same Creative Across Networks

```sql
-- Find creatives running on multiple networks
WITH creative_ids AS (
    -- Taboola
    SELECT
        params->>'tb_image' AS creative_id,
        'taboola' AS network
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE params->>'tb_image' IS NOT NULL
    UNION ALL
    -- Outbrain
    SELECT
        params->>'ob_creative' AS creative_id,
        'outbrain' AS network
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE params->>'ob_creative' IS NOT NULL
)
SELECT
    creative_id,
    COUNT(DISTINCT network) AS num_networks,
    array_agg(DISTINCT network) AS networks
FROM creative_ids
GROUP BY 1
HAVING COUNT(DISTINCT network) > 1
ORDER BY 2 DESC;
```

## Time-Based Analysis

### Trending Campaigns

```sql
-- Campaigns with increasing momentum
WITH daily_metrics AS (
    SELECT
        DATE(ts) AS date,
        params->>'utm_campaign' AS campaign,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1, 2
),
trends AS (
    SELECT
        campaign,
        AVG(clicks) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS recent_clicks,
        AVG(clicks) FILTER (WHERE date < CURRENT_DATE - INTERVAL '7 days') AS prior_clicks
    FROM daily_metrics
    GROUP BY 1
)
SELECT
    campaign,
    recent_clicks,
    prior_clicks,
    ROUND(
        100.0 * (recent_clicks - prior_clicks) / NULLIF(prior_clicks, 0),
        2
    ) AS momentum_pct
FROM trends
WHERE prior_clicks > 10
ORDER BY momentum_pct DESC
LIMIT 20;
```

### Creative Fatigue Detection

```sql
-- Detect declining creative performance
WITH creative_daily AS (
    SELECT
        DATE(ts) AS date,
        params->>'tb_headline' AS headline,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
        AND params->>'tb_headline' IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 100
),
daily_ctr AS (
    SELECT
        date,
        headline,
        clicks,
        views,
        ROUND(100.0 * clicks / NULLIF(views, 0), 2) AS ctr
    FROM creative_daily
),
fatigue AS (
    SELECT
        headline,
        AVG(ctr) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS recent_ctr,
        AVG(ctr) FILTER (WHERE date < CURRENT_DATE - INTERVAL '7 days'
                          AND date >= CURRENT_DATE - INTERVAL '21 days') AS prior_ctr
    FROM daily_ctr
    GROUP BY 1
)
SELECT
    headline,
    recent_ctr,
    prior_ctr,
    ROUND(
        100.0 * (recent_ctr - prior_ctr) / NULLIF(prior_ctr, 0),
        2
    ) AS fatigue_change_pct
FROM fatigue
WHERE prior_ctr > 0
ORDER BY fatigue_change_pct ASC
LIMIT 20;
```

## Session Analytics

### Session-Based Funnel Analysis

```sql
-- Analyze user behavior within sessions
WITH session_metrics AS (
    SELECT
        session_id,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        COUNT(*) FILTER (WHERE type = 'dwell') AS dwells,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 60 AS session_duration_minutes
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
    GROUP BY session_id
)
SELECT
    pageviews,
    clicks,
    scrolls,
    dwells,
    ROUND(AVG(session_duration_minutes), 2) AS avg_duration_minutes,
    COUNT(*) AS num_sessions
FROM session_metrics
GROUP BY pageviews, clicks, scrolls, dwells
ORDER BY num_sessions DESC
LIMIT 20;
```

### Session Stitching Across Pages

```sql
-- Track user journeys across multiple pages
WITH user_journeys AS (
    SELECT
        session_id,
        user_id,
        type,
        url,
        ts,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts) AS step_number
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
)
SELECT
    session_id,
    user_id,
    STRING_AGG(url, ' -> ' ORDER BY ts) AS journey,
    COUNT(*) AS events,
    MIN(ts) AS session_start,
    MAX(ts) AS session_end
FROM user_journeys
GROUP BY session_id, user_id
HAVING COUNT(*) >= 2
ORDER BY events DESC
LIMIT 50;
```

### User-Level Analytics (Cross-Session)

```sql
-- Analyze user behavior across multiple sessions
SELECT
    user_id,
    COUNT(DISTINCT session_id) AS num_sessions,
    COUNT(*) FILTER (WHERE type = 'pageview') AS total_pageviews,
    COUNT(*) FILTER (WHERE type = 'click') AS total_clicks,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen,
    EXTRACT(DAY FROM (MAX(ts) - MIN(ts))) + 1 AS days_active
FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
WHERE ts >= CURRENT_DATE - INTERVAL '30 days'
    AND user_id IS NOT NULL
GROUP BY user_id
ORDER BY num_sessions DESC
LIMIT 50;
```

### Session Source Attribution

```sql
-- Track which campaigns and sources drive sessions
WITH session_sources AS (
    SELECT
        session_id,
        params->>'utm_source' AS source,
        params->>'utm_campaign' AS campaign,
        params->>'utm_medium' AS medium,
        MIN(ts) AS session_start
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
    GROUP BY session_id, source, campaign, medium
)
SELECT
    source,
    campaign,
    medium,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT session_id) FILTER (
        WHERE session_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    ) AS sessions_last_24h
FROM session_sources
GROUP BY source, campaign, medium
ORDER BY sessions DESC
LIMIT 20;
```

### Engagement by Session Depth

```sql
-- Categorize sessions by engagement level
WITH session_engagement AS (
    SELECT
        session_id,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 60 AS duration_minutes
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
    GROUP BY session_id
),
engagement_levels AS (
    SELECT
        session_id,
        pageviews,
        clicks,
        scrolls,
        duration_minutes,
        CASE
            WHEN pageviews >= 5 AND duration_minutes >= 5 THEN 'high'
            WHEN pageviews >= 3 OR duration_minutes >= 2 THEN 'medium'
            WHEN pageviews >= 2 THEN 'low'
            ELSE 'bounce'
        END AS engagement_level
    FROM session_engagement
)
SELECT
    engagement_level,
    COUNT(*) AS num_sessions,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_sessions,
    ROUND(AVG(pageviews), 2) AS avg_pageviews,
    ROUND(AVG(clicks), 2) AS avg_clicks,
    ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes
FROM engagement_levels
GROUP BY engagement_level
ORDER BY
    CASE engagement_level
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
        ELSE 4
    END;
```

### Link Decoration Tracking

```sql
-- Verify session stitching is working via link decoration
WITH decorated_sessions AS (
    SELECT
        session_id,
        COUNT(DISTINCT url) AS distinct_urls,
        COUNT(*) AS events,
        MIN(ts) AS first_event,
        MAX(ts) AS last_event
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
    GROUP BY session_id
)
SELECT
    CASE
        WHEN distinct_urls = 1 THEN 'single_page'
        WHEN distinct_urls BETWEEN 2 AND 3 THEN 'multi_page_low'
        ELSE 'multi_page_high'
    END AS session_type,
    COUNT(*) AS num_sessions,
    ROUND(AVG(events), 2) AS avg_events_per_session
FROM decorated_sessions
GROUP BY session_type
ORDER BY num_sessions DESC;
```

## User Journey Analysis

### Session Flow

```sql
-- Common page sequences within sessions
WITH session_events AS (
    SELECT
        session_id,
        type,
        url,
        ts,
        LAG(type) OVER (PARTITION BY session_id ORDER BY ts) AS prev_type,
        LAG(url) OVER (PARTITION BY session_id ORDER BY ts) AS prev_url
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
)
SELECT
    prev_type,
    type,
    COUNT(*) AS flow_count,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM session_events
WHERE prev_type IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;
```

### Landing Page Performance

```sql
-- Top landing pages and bounce rate
WITH sessions AS (
    SELECT
        session_id,
        MIN(url) AS landing_url,
        COUNT(*) AS events,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
        AND session_id IS NOT NULL
    GROUP BY 1
)
SELECT
    landing_url,
    COUNT(*) AS sessions,
    SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) AS bounced,
    ROUND(
        100.0 * SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0),
        2
    ) AS bounce_rate_pct
FROM sessions
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
```

## Performance Optimization

### Using Compacted Data

```sql
-- Query compacted daily partitions (faster)
SELECT *
FROM read_parquet('s3://my-trace-bucket/trace-events/events-compacted/**/*.parquet')
WHERE dt >= '2026-05-01'
  AND dt < '2026-05-08';
```

### Partition Pruning

```sql
-- Explicit partition filter for better performance
SELECT *
FROM read_parquet('s3://my-trace-bucket/trace-events/events/dt=2026-05-08/*.parquet')
WHERE type = 'click';
```

### Query Hints

```sql
-- Set memory limit for large queries
SET memory_limit='2GB';

-- Enable parallel processing
SET threads=4;

-- Use hive partitioning
SET enable_hive_partitioning=true;
```

## Scheduled Reports

### Daily Summary Report

```sql
-- Create a summary view
CREATE OR REPLACE VIEW daily_summary AS
WITH daily AS (
    SELECT
        DATE(ts) AS date,
        params->>'utm_source' AS source,
        params->>'utm_campaign' AS campaign,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(DISTINCT session_id) AS sessions
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    GROUP BY 1, 2, 3
)
SELECT
    date,
    source,
    COUNT(DISTINCT campaign) AS active_campaigns,
    SUM(views) AS total_views,
    SUM(clicks) AS total_clicks,
    ROUND(100.0 * SUM(clicks) / NULLIF(SUM(views), 0), 2) AS overall_ctr,
    SUM(sessions) AS total_sessions
FROM daily
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- Query the view
SELECT * FROM daily_summary
WHERE date >= CURRENT_DATE - INTERVAL '7 days';
```

## Alerts and Anomalies

### Traffic Spike Detection

```sql
-- Detect unusual traffic spikes
WITH hourly_baseline AS (
    SELECT
        DATE_TRUNC('hour', ts) AS hour,
        COUNT(*) AS events,
        AVG(COUNT(*)) OVER (
            PARTITION BY EXTRACT(HOUR FROM ts)
            ORDER BY DATE_TRUNC('day', ts)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_avg,
        STDDEV(COUNT(*)) OVER (
            PARTITION BY EXTRACT(HOUR FROM ts)
            ORDER BY DATE_TRUNC('day', ts)
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_stddev
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1
)
SELECT
    hour,
    events,
    baseline_avg,
    baseline_stddev,
    ROUND(
        100.0 * (events - baseline_avg) / NULLIF(baseline_avg, 0),
        2
    ) AS deviation_pct
FROM hourly_baseline
WHERE baseline_avg IS NOT NULL
    AND events > baseline_avg + (2 * baseline_stddev)
ORDER BY hour DESC
LIMIT 10;
```

### Zero Traffic Alert

```sql
-- Find campaigns with no recent traffic
WITH campaign_activity AS (
    SELECT
        params->>'utm_campaign' AS campaign,
        MAX(ts) AS last_event,
        COUNT(*) AS total_events
    FROM read_parquet('s3://my-trace-bucket/trace-events/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
)
SELECT
    campaign,
    last_event,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - last_event) AS days_since_last_event,
    total_events
FROM campaign_activity
WHERE last_event < CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY 2 ASC;
```
