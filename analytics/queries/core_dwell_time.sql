-- ============================================================================
-- Core Dwell Time Query: Aggregate heartbeat pings into time-on-page
-- ============================================================================
-- Calculates dwell time (time spent on page) from multiple sources:
-- - Explicit dwell_time_ms field from dwell events
-- - Inferred from time between events per session
-- - Aggregated by session, page, campaign, and creative
--
-- Usage:
--   Replace {{events_table}} with your events table name
--   Replace {{start_date}} and {{end_date}} with your date range
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Dwell time by session (using explicit dwell events)
-- ----------------------------------------------------------------------------

WITH session_dwell_events AS (
    SELECT
        session_id,
        url,
        network,
        campaign_id,
        creative_id,
        -- Sum dwell_time_ms from all dwell events for this session/page
        SUM(dwell_time_ms) AS total_dwell_ms,
        COUNT(*) FILTER (WHERE type = 'dwell') AS dwell_ping_count,
        -- Get the dwell_visible_pct for quality assessment
        AVG(dwell_visible_pct) AS avg_visible_pct,
        MIN(ts) AS first_dwell_ts,
        MAX(ts) AS last_dwell_ts
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND type = 'dwell'
        AND dwell_time_ms IS NOT NULL
        AND session_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    session_id,
    url,
    network,
    campaign_id,
    creative_id,
    ROUND(total_dwell_ms / 1000.0, 1) AS dwell_time_seconds,
    dwell_ping_count,
    ROUND(avg_visible_pct, 1) AS avg_visible_pct,
    first_dwell_ts,
    last_dwell_ts,
    -- Classify dwell time
    CASE
        WHEN total_dwell_ms < 5000 THEN '< 5s (bounce)'
        WHEN total_dwell_ms < 15000 THEN '5-15s (skim)'
        WHEN total_dwell_ms < 30000 THEN '15-30s (reading)'
        WHEN total_dwell_ms < 60000 THEN '30-60s (engaged)'
        ELSE '60s+ (deep)'
    END AS dwell_category
FROM session_dwell_events
ORDER BY total_dwell_ms DESC
LIMIT 1000;

-- ----------------------------------------------------------------------------
-- Dwell time by landing page (aggregated)
-- ----------------------------------------------------------------------------

SELECT
    url,
    network,
    campaign_id,
    COUNT(DISTINCT session_id) AS sessions,
    ROUND(AVG(dwell_time_ms) / 1000.0, 1) AS avg_dwell_seconds,
    ROUND(MIN(dwell_time_ms) / 1000.0, 1) AS min_dwell_seconds,
    ROUND(MAX(dwell_time_ms) / 1000.0, 1) AS max_dwell_seconds,
    -- Percentiles for distribution analysis
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dwell_time_ms) / 1000.0,
        1
    ) AS median_dwell_seconds,
    ROUND(
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY dwell_time_ms) / 1000.0,
        1
    ) AS p90_dwell_seconds,
    -- Quality metrics
    ROUND(AVG(dwell_visible_pct), 1) AS avg_visible_pct
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND type = 'dwell'
    AND dwell_time_ms IS NOT NULL
    AND session_id IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT session_id) >= 5
ORDER BY avg_dwell_seconds DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Dwell time by campaign/creative (performance correlation)
-- ----------------------------------------------------------------------------

WITH creative_dwell AS (
    SELECT
        campaign_id,
        creative_id,
        headline,
        network,
        session_id,
        SUM(dwell_time_ms) AS session_dwell_ms
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND type = 'dwell'
        AND dwell_time_ms IS NOT NULL
        AND session_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),
creative_stats AS (
    SELECT
        campaign_id,
        creative_id,
        headline,
        network,
        COUNT(*) AS sessions_with_dwell,
        ROUND(AVG(session_dwell_ms) / 1000.0, 1) AS avg_dwell_seconds,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_dwell_ms) / 1000.0,
            1
        ) AS median_dwell_seconds,
        -- Engagement distribution
        SUM(CASE WHEN session_dwell_ms < 5000 THEN 1 ELSE 0 END) AS bounce_count,
        SUM(CASE WHEN session_dwell_ms >= 30000 THEN 1 ELSE 0 END) AS engaged_count,
        ROUND(
            100.0 * SUM(CASE WHEN session_dwell_ms >= 30000 THEN 1 ELSE 0 END) /
            NULLIF(COUNT(*), 0),
            1
        ) AS engagement_rate_pct
    FROM creative_dwell
    GROUP BY 1, 2, 3, 4
)
SELECT * FROM creative_stats
WHERE sessions_with_dwell >= 10
ORDER BY avg_dwell_seconds DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Inferred dwell time (time between events when no explicit dwell events)
-- ----------------------------------------------------------------------------

WITH event_sequence AS (
    SELECT
        session_id,
        url,
        type,
        ts,
        LEAD(ts) OVER (
            PARTITION BY session_id
            ORDER BY ts
        ) AS next_event_ts,
        LEAD(url) OVER (
            PARTITION BY session_id
            ORDER BY ts
        ) AS next_url
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
),
page_dwell_inferred AS (
    SELECT
        session_id,
        url,
        type,
        ts,
        -- Calculate time until next event
        EXTRACT(EPOCH FROM (
            COALESCE(next_event_ts, ts + INTERVAL '30 minutes') - ts
        ))::BIGINT AS dwell_ms_inferred,
        next_url
    FROM event_sequence
)
SELECT
    session_id,
    url,
    MIN(ts) AS first_seen,
    MAX(ts) AS last_seen,
    COUNT(*) AS event_count,
    -- Use the maximum inferred dwell time for this page in the session
    ROUND(MAX(dwell_ms_inferred) / 1000.0, 1) AS max_dwell_seconds,
    ROUND(AVG(dwell_ms_inferred) / 1000.0, 1) AS avg_dwell_seconds,
    -- Check if user left the site (next_url is NULL or different domain)
    MAX(CASE
        WHEN next_url IS NULL THEN 'exited'
        WHEN next_url IS NOT NULL THEN 'continued'
    END) AS exit_type
FROM page_dwell_inferred
WHERE dwell_ms_inferred <= 1800000  -- Cap at 30 minutes
GROUP BY 1, 2
HAVING COUNT(*) >= 1
ORDER BY MAX(dwell_ms_inferred) DESC
LIMIT 100;

-- ----------------------------------------------------------------------------
-- Dwell time distribution histogram
-- ----------------------------------------------------------------------------

SELECT
    CASE
        WHEN dwell_time_ms < 5000 THEN '0-5s'
        WHEN dwell_time_ms < 10000 THEN '5-10s'
        WHEN dwell_time_ms < 20000 THEN '10-20s'
        WHEN dwell_time_ms < 30000 THEN '20-30s'
        WHEN dwell_time_ms < 60000 THEN '30-60s'
        WHEN dwell_time_ms < 120000 THEN '1-2m'
        WHEN dwell_time_ms < 300000 THEN '2-5m'
        ELSE '5m+'
    END AS dwell_bucket,
    COUNT(*) AS dwell_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(
        100.0 * COUNT(DISTINCT session_id) /
        NULLIF(SUM(COUNT(DISTINCT session_id)) OVER (), 0),
        1
    ) AS session_pct
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND type = 'dwell'
    AND dwell_time_ms IS NOT NULL
GROUP BY 1
ORDER BY
    CASE dwell_bucket
        WHEN '0-5s' THEN 1
        WHEN '5-10s' THEN 2
        WHEN '10-20s' THEN 3
        WHEN '20-30s' THEN 4
        WHEN '30-60s' THEN 5
        WHEN '1-2m' THEN 6
        WHEN '2-5m' THEN 7
        WHEN '5m+' THEN 8
    END;
