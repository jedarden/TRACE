# Attribution and Funnel Queries for TRACE

This document describes the pre-built attribution and funnel analysis queries available in TRACE analytics.

## Attribution Queries

Attribution models assign credit for conversions to different touchpoints in a user's journey. TRACE provides four attribution models:

### 1. First-Touch Attribution (`attribution_first_touch`)

Credits the initial acquisition source (first campaign/source in a session) for conversions. Useful for understanding which campaigns acquire users.

**Usage:**
```bash
trace-analytics run attribution_first_touch \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/first_touch.json
```

**Output Columns:**
- `source` - UTM source or '(direct)'
- `medium` - UTM medium or '(none)'
- `campaign` - UTM campaign or '(not set)'
- `content` - UTM content
- `term` - UTM term
- `ad_network` - Ad network (taboola, outbrain, etc.)
- `attributed_sessions` - Number of sessions attributed
- `conversions` - Number of conversions attributed
- `attributed_revenue` - Total revenue attributed
- `attribution_pct` - Percentage of total conversions

**When to use:**
- Evaluating top-of-funnel acquisition campaigns
- Understanding initial customer acquisition channels
- Comparing brand awareness campaigns

### 2. Last-Touch Attribution (`attribution_last_touch`)

Credits the final touchpoint before conversion. Useful for understanding what directly leads to conversions.

**Usage:**
```bash
trace-analytics run attribution_last_touch \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/last_touch.json
```

**Output Columns:** Same as first-touch

**When to use:**
- Optimizing conversion-focused campaigns
- Understanding immediate conversion drivers
- Attribution for bottom-of-funnel activities

### 3. Linear Attribution (`attribution_linear`)

Distributes credit equally across all touchpoints in a session, giving fair credit to the entire customer journey.

**Usage:**
```bash
trace-analytics run attribution_linear \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/linear.json
```

**Output Columns:**
- `source` - UTM source or '(direct)'
- `medium` - UTM medium or '(none)'
- `campaign` - UTM campaign or '(not set)'
- `content` - UTM content
- `term` - UTM term
- `ad_network` - Ad network
- `touched_sessions` - Number of sessions with this touchpoint
- `attributed_conversions` - Fractional conversions attributed
- `attributed_revenue` - Fractional revenue attributed
- `attribution_pct` - Percentage of total conversions

**When to use:**
- Understanding full-journey impact
- Fair credit distribution across all touchpoints
- Multi-channel attribution analysis

### 4. Multi-Touch Attribution Analysis (`attribution_analysis`)

Tracks all touchpoints in a user's journey leading to conversion with position-based tagging.

**Usage:**
```bash
trace-analytics run attribution_analysis \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/attribution_analysis.json
```

**Output Columns:**
- `user_id` - User identifier
- `session_id` - Session identifier
- `network` - Ad network
- `campaign_id` - Campaign ID
- `creative_id` - Creative ID
- `url` - Landing page URL
- `event_type` - Event type
- `session_ts` - Session timestamp
- `conversion_ts` - Conversion timestamp
- `conversion_type` - Type of conversion
- `touch_position` - Position in journey (1=first)
- `total_touches` - Total touchpoints in journey
- `days_to_conversion` - Days from touch to conversion
- `attribution_model_first` - Tagged if first touch
- `attribution_model_last` - Tagged if last touch

**When to use:**
- Analyzing complete user journeys
- Understanding path to conversion
- Building custom attribution models

## Funnel Queries

Funnel analysis tracks user progression through defined steps, identifying drop-off points and conversion rates.

### 1. Campaign Funnel (`campaign_funnel`)

Analyzes engagement funnel by campaign, showing progression from pageviews through clicks, scrolls, and dwells.

**Usage:**
```bash
trace-analytics run campaign_funnel \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/campaign_funnel.json
```

**Output Columns:**
- `campaign` - UTM campaign name
- `pageviews` - Number of pageviews
- `clicks` - Number of clicks
- `click_through_pct` - CTR percentage
- `scrolls` - Number of scroll events
- `scroll_after_click_pct` - Scroll rate after clicks
- `dwells` - Number of dwell events
- `dwell_after_scroll_pct` - Dwell rate after scrolls

**When to use:**
- Comparing campaign engagement quality
- Identifying high-intent campaigns
- Understanding user behavior by campaign

### 2. Funnel with Paths (`funnel_with_paths`)

Analyzes a defined conversion funnel with user journey paths. Customize the funnel steps in the query.

**Usage:**
```bash
trace-analytics run funnel_with_paths \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/funnel_paths.json
```

**Output Columns:**
- `step_number` - Step in the funnel
- `step_name` - Name of the step
- `users_reached_step` - Unique users at this step
- `prev_step_users` - Users from previous step
- `conversion_rate_pct` - Conversion rate to this step

**Customizing Funnel Steps:**
Edit the CASE statement in the query to match your funnel:
```sql
CASE
    WHEN url LIKE '%/pricing%' THEN 1
    WHEN url LIKE '%/signup%' OR type = 'signup' THEN 2
    WHEN url LIKE '%/checkout%' OR type = 'purchase' THEN 3
    WHEN url LIKE '%/thank-you%' OR type = 'conversion' THEN 4
    ELSE 0
END AS funnel_step
```

**When to use:**
- Analyzing conversion funnels
- Identifying drop-off points
- Optimizing user flows

### 3. Drop-Off Analysis (`drop_off_analysis`)

Identifies the last action users take before leaving, helping to understand where users disengage.

**Usage:**
```bash
trace-analytics run drop_off_analysis \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/drop_off.json
```

**Output Columns:**
- `last_event_type` - Type of last event
- `last_url` - URL where user dropped off
- `second_to_last_url` - Previous URL
- `sessions` - Number of sessions ending here
- `unique_users` - Number of unique users
- `avg_events_before_dropoff` - Average events before leaving

**When to use:**
- Identifying problematic pages
- Understanding user disengagement points
- Optimizing content and UX

### 4. Session Flow Matrix (`session_flow_matrix`)

Shows how users navigate between pages with transition counts for visualization.

**Usage:**
```bash
trace-analytics run session_flow_matrix \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/flow_matrix.json
```

**Output Columns:**
- `from_page` - Source page URL
- `to_page` - Destination page URL
- `transition_count` - Number of transitions
- `unique_sessions` - Number of unique sessions

**When to use:**
- Building flow visualizations
- Understanding navigation patterns
- Identifying common paths

### 5. Common Paths (`common_paths`)

Identifies the most frequent navigation paths users take through the site.

**Usage:**
```bash
trace-analytics run common_paths \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/common_paths.json
```

**Output Columns:**
- `path` - Array of URLs in the path
- `steps` - Number of steps in the path
- `frequency` - How often this path occurs
- `percentage` - Percentage of all paths

**When to use:**
- Identifying popular user flows
- Optimizing common paths
- Understanding user behavior

## Journey Analysis Queries

### 1. Session Reconstruction (`session_reconstruction`)

Reconstructs sessions from events using gap-based sessionization (30-minute gap).

**Usage:**
```bash
trace-analytics run session_reconstruction \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/sessions.json
```

**Output Columns:**
- `session_id` - Reconstructed session ID
- `user_id` - User identifier
- `session_start` - Session start time
- `session_end` - Session end time
- `event_count` - Number of events
- `unique_pages` - Number of unique pages
- `duration_seconds` - Session duration
- `landing_page` - First page in session
- `is_bounce` - Whether session is a bounce (1 event)
- `source_network` - Acquisition network
- `campaign` - Campaign ID
- `device_type` - Device type

### 2. User Journey (`user_journey`)

Reconstructs complete user journey across all sessions.

**Usage:**
```bash
trace-analytics run user_journey \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/user_journeys.json
```

**Output Columns:**
- `user_id` - User identifier
- `session_seq` - Session sequence number
- `session_start` - Session start time
- `session_end` - Session end time
- `event_count` - Events in session
- `unique_pages` - Unique pages visited
- `duration_seconds` - Session duration
- `landing_page` - First page
- `source_network` - Acquisition source
- `campaign` - Campaign ID
- `event_types` - Array of event types

### 3. Cohort Journey (`cohort_journey`)

Analyzes user behavior by acquisition cohort (first touch network) over time.

**Usage:**
```bash
trace-analytics run cohort_journey \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/cohort_journey.json
```

**Output Columns:**
- `cohort` - Acquisition network
- `day_number` - Days since first touch
- `active_users` - Active users in cohort
- `sessions` - Number of sessions
- `avg_events_per_session` - Average events
- `avg_pages_per_session` - Average pages

**When to use:**
- Comparing cohort retention
- Understanding lifetime value by source
- Analyzing cohort behavior over time

### 4. Returning User Analysis (`returning_user_analysis`)

Segments users by session frequency and engagement level.

**Usage:**
```bash
trace-analytics run returning_user_analysis \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/returning_users.json
```

**Output Columns:**
- `user_segment` - Segment (new, returning_2, returning_3_5, etc.)
- `user_count` - Number of users
- `total_sessions` - Total sessions
- `total_events` - Total events
- `avg_sessions_per_user` - Average sessions
- `avg_active_days` - Average active days
- `avg_unique_pages` - Average unique pages

## Session Flow (`session_flow`)

Analyzes common event sequences within sessions.

**Usage:**
```bash
trace-analytics run session_flow \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/session_flow.json
```

**Output Columns:**
- `prev_type` - Previous event type
- `type` - Current event type
- `flow_count` - Number of transitions
- `unique_sessions` - Number of unique sessions

## Landing Page Performance (`landing_page_performance`)

Analyzes top landing pages and bounce rates.

**Usage:**
```bash
trace-analytics run landing_page_performance \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/landing_pages.json
```

**Output Columns:**
- `landing_url` - Landing page URL
- `sessions` - Number of sessions
- `bounced` - Number of bounced sessions
- `bounce_rate_pct` - Bounce rate percentage

## Report Categories

All attribution and funnel queries are categorized under:

- **Journey** - Attribution models, user journey analysis, session reconstruction
- **Campaign** - Campaign funnels

## Using the Analytics CLI

### List Available Reports

```bash
trace-analytics list
```

### Run a Report

```bash
trace-analytics run <report_name> \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --format json|csv \
  --output <path>
```

### Example Workflow

```bash
# 1. Run first-touch attribution
trace-analytics run attribution_first_touch \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/attribution/first_touch_2026-05-01_to_2026-05-08.json

# 2. Run campaign funnel
trace-analytics run campaign_funnel \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/funnels/campaign_2026-05-01_to_2026-05-08.json

# 3. Run drop-off analysis
trace-analytics run drop_off_analysis \
  --start-date 2026-05-01 \
  --end-date 2026-05-08 \
  --format json \
  --output reports/funnels/drop_off_2026-05-01_to_2026-05-08.json
```

## Environment Setup

Ensure these environment variables are set:

```bash
# S3 Configuration
TRACE_S3_BUCKET=my-trace-bucket
TRACE_S3_REGION=us-east-1
TRACE_S3_PREFIX=trace-events

# AWS Credentials (if not using IAM role)
AWS_ACCESS_KEY_ID=***
AWS_SECRET_ACCESS_KEY=***

# Optional: Iceberg Catalog
TRACE_ICEBERG_CATALOG_URI=http://iceberg-catalog:8181
TRACE_ICEBERG_WAREHOUSE=s3://my-trace-bucket/iceberg
```

## Query Templates

All queries support template variables:
- `{{s3_path}}` - S3 path to events
- `{{start_date}}` - Query start date
- `{{end_date}}` - Query end date

These are automatically replaced when running reports via the CLI.
