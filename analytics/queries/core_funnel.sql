-- ============================================================================
-- Core Funnel Conversion Query: Session chain from ad click to conversion
-- ============================================================================
-- Analyzes conversion funnels with multiple dimensions:
-- - Full funnel: impression → click → engagement → conversion
-- - By campaign/creative: Which campaigns drive best conversion
-- - Drop-off analysis: Where users abandon the funnel
-- - Time-to-convert: How long conversions take
--
-- Usage:
--   Replace {{events_table}} with your events table name
--   Replace {{start_date}} and {{end_date}} with your date range
--
-- Conversion Definition:
--   - Adjust the conversion criteria in the CTEs below
--   - Default: Sessions with depth >= 3 OR explicit conversion event
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Full Funnel Analysis (by campaign)
-- ----------------------------------------------------------------------------

WITH session_events AS (
    -- Aggregate events to session level for funnel stages
    SELECT
        session_id,
        -- First-touch attribution
        MIN(network) AS network,
        MIN(campaign_id) AS campaign_id,
        MIN(creative_id) AS creative_id,
        MIN(headline) AS headline,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        -- Funnel stage events
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        COUNT(*) FILTER (WHERE type = 'dwell') AS dwells,
        -- Depth calculation
        COUNT(DISTINCT url) AS unique_pages,
        -- Conversion indicator (customize this logic)
        MAX(CASE
            WHEN type = 'conversion' THEN 1
            WHEN unique_pages >= 3 THEN 1  -- Depth-based conversion
            ELSE 0
        END) AS converted
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id
),
funnel_stages AS (
    SELECT
        campaign_id,
        network,
        -- Stage 1: All sessions (landing)
        COUNT(*) AS sessions,
        -- Stage 2: Clicked (engaged)
        SUM(CASE WHEN clicks > 0 THEN 1 ELSE 0 END) AS clicked,
        -- Stage 3: Scrolled (content interaction)
        SUM(CASE WHEN scrolls > 0 THEN 1 ELSE 0 END) AS scrolled,
        -- Stage 4: Deep engagement (multiple pages or dwell)
        SUM(CASE
            WHEN unique_pages >= 2 OR dwells > 0 THEN 1
            ELSE 0
        END) AS engaged,
        -- Stage 5: Converted
        SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS conversions
    FROM session_events
    WHERE campaign_id IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    campaign_id,
    network,
    sessions AS landing_sessions,
    clicked AS click_sessions,
    scrolled AS scroll_sessions,
    engaged AS engaged_sessions,
    conversions AS converted_sessions,
    -- Conversion rates
    ROUND(100.0 * clicked / NULLIF(sessions, 0), 2) AS landing_to_click_pct,
    ROUND(100.0 * scrolled / NULLIF(clicked, 0), 2) AS click_to_scroll_pct,
    ROUND(100.0 * engaged / NULLIF(scrolled, 0), 2) AS scroll_to_engage_pct,
    ROUND(100.0 * conversions / NULLIF(engaged, 0), 2) AS engage_to_convert_pct,
    -- Overall conversion rate
    ROUND(100.0 * conversions / NULLIF(sessions, 0), 2) AS overall_conversion_pct
FROM funnel_stages
WHERE sessions >= 10
ORDER BY conversions DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Funnel by Creative (headline + image performance)
-- ----------------------------------------------------------------------------

WITH session_creative AS (
    SELECT
        session_id,
        creative_id,
        headline,
        network,
        MIN(campaign_id) AS campaign_id,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(*) FILTER (WHERE type = 'scroll') AS scrolls,
        COUNT(DISTINCT url) AS unique_pages,
        MAX(CASE
            WHEN type = 'conversion' THEN 1
            WHEN unique_pages >= 3 THEN 1
            ELSE 0
        END) AS converted
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
        AND creative_id IS NOT NULL
    GROUP BY session_id, creative_id, headline, network
),
creative_funnel AS (
    SELECT
        creative_id,
        headline,
        network,
        COUNT(*) AS sessions,
        SUM(CASE WHEN clicks > 0 THEN 1 ELSE 0 END) AS clicked,
        SUM(CASE WHEN scrolls > 0 THEN 1 ELSE 0 END) AS scrolled,
        SUM(CASE WHEN unique_pages >= 2 THEN 1 ELSE 0 END) AS multi_page,
        SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS conversions,
        AVG(EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))) AS avg_duration_seconds
    FROM session_creative
    GROUP BY 1, 2, 3
)
SELECT
    creative_id,
    headline,
    network,
    sessions,
    clicked,
    scrolled,
    multi_page,
    conversions,
    ROUND(100.0 * conversions / NULLIF(sessions, 0), 2) AS conversion_rate_pct,
    ROUND(100.0 * clicked / NULLIF(sessions, 0), 2) AS click_rate_pct,
    ROUND(avg_duration_seconds, 1) AS avg_duration_seconds
FROM creative_funnel
WHERE sessions >= 10
ORDER BY conversion_rate_pct DESC, sessions DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Drop-off Analysis (where users leave the funnel)
-- ----------------------------------------------------------------------------

WITH user_journey AS (
    SELECT
        session_id,
        network,
        campaign_id,
        -- Track event sequence to identify drop-off points
        ARRAY_AGG(type ORDER BY ts) AS event_sequence,
        ARRAY_AGG(url ORDER BY ts) AS url_sequence,
        MIN(ts) AS first_event,
        MAX(ts) AS last_event,
        COUNT(*) AS event_count,
        COUNT(DISTINCT url) AS unique_pages,
        MAX(CASE WHEN type = 'conversion' THEN 1 ELSE 0 END) AS converted
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id, network, campaign_id
),
drop_off_points AS (
    SELECT
        campaign_id,
        network,
        converted,
        -- Classify drop-off stage
        CASE
            WHEN event_count = 1 THEN 'no_engagement'
            WHEN event_sequence[1] = 'pageview' AND NOT event_sequence ANYARRAY['click', 'scroll'] THEN 'view_only'
            WHEN event_sequence ANYARRAY['click'] AND NOT event_sequence ANYARRAY['scroll'] THEN 'clicked_no_scroll'
            WHEN event_sequence ANYARRAY['scroll'] AND unique_pages < 2 THEN 'single_page'
            WHEN unique_pages >= 2 AND converted = 0 THEN 'multi_page_no_convert'
            WHEN converted = 1 THEN 'converted'
            ELSE 'other'
        END AS drop_stage,
        COUNT(*) AS sessions,
        ROUND(AVG(event_count), 1) AS avg_events,
        ROUND(AVG(unique_pages), 1) AS avg_pages
    FROM user_journey
    WHERE campaign_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    campaign_id,
    network,
    drop_stage,
    sessions,
    ROUND(100.0 * sessions / SUM(sessions) OVER (PARTITION BY campaign_id, network), 1) AS stage_pct,
    avg_events,
    avg_pages
FROM drop_off_points
WHERE sessions >= 5
ORDER BY campaign_id, network,
    CASE drop_stage
        WHEN 'no_engagement' THEN 1
        WHEN 'view_only' THEN 2
        WHEN 'clicked_no_scroll' THEN 3
        WHEN 'single_page' THEN 4
        WHEN 'multi_page_no_convert' THEN 5
        WHEN 'converted' THEN 6
        ELSE 7
    END;

-- ----------------------------------------------------------------------------
-- Time-to-Convert Analysis
-- ----------------------------------------------------------------------------

WITH conversion_sessions AS (
    SELECT
        session_id,
        network,
        campaign_id,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        -- Find first conversion event timestamp
        MIN(ts) FILTER (WHERE type = 'conversion') AS first_conversion_ts,
        COUNT(*) FILTER (WHERE type = 'pageview') AS pageviews,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks,
        COUNT(DISTINCT url) AS unique_pages,
        CASE
            WHEN MAX(CASE WHEN type = 'conversion' THEN 1 ELSE 0 END) = 1 THEN true
            WHEN COUNT(DISTINCT url) >= 3 THEN true  -- Depth-based conversion
            ELSE false
        END AS converted
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id, network, campaign_id
    HAVING CASE
        WHEN MAX(CASE WHEN type = 'conversion' THEN 1 ELSE 0 END) = 1 THEN true
        WHEN COUNT(DISTINCT url) >= 3 THEN true
        ELSE false
    END
)
SELECT
    campaign_id,
    network,
    COUNT(*) AS conversions,
    -- Time to convert distribution
    ROUND(AVG(EXTRACT(EPOCH FROM (session_end - session_start))), 1) AS avg_session_seconds,
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (session_end - session_start))
        ),
        1
    ) AS median_session_seconds,
    ROUND(AVG(pageviews), 1) AS avg_pageviews,
    ROUND(AVG(unique_pages), 1) AS avg_unique_pages,
    -- Time buckets
    SUM(CASE
        WHEN EXTRACT(EPOCH FROM (session_end - session_start)) < 30 THEN 1
        ELSE 0
    END) AS under_30s,
    SUM(CASE
        WHEN EXTRACT(EPOCH FROM (session_end - session_start)) BETWEEN 30 AND 120 THEN 1
        ELSE 0
    END) AS between_30s_2m,
    SUM(CASE
        WHEN EXTRACT(EPOCH FROM (session_end - session_start)) > 120 THEN 1
        ELSE 0
    END) AS over_2m
FROM conversion_sessions
WHERE campaign_id IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) >= 3
ORDER BY conversions DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Daily Funnel Trend (monitoring funnel performance over time)
-- ----------------------------------------------------------------------------

SELECT
    DATE_TRUNC('day', ts) AS date,
    network,
    COUNT(DISTINCT session_id) AS total_sessions,
    -- Funnel stages
    COUNT(DISTINCT session_id FILTER (
        WHERE EXISTS (
            SELECT 1 FROM {{events_table}} e2
            WHERE e2.session_id = {{events_table}}.session_id
            AND e2.type = 'click'
        )
    )) AS sessions_with_click,
    COUNT(DISTINCT session_id FILTER (
        WHERE EXISTS (
            SELECT 1 FROM {{events_table}} e2
            WHERE e2.session_id = {{events_table}}.session_id
            AND e2.type = 'scroll'
        )
    )) AS sessions_with_scroll,
    -- Conversion count
    COUNT(DISTINCT session_id FILTER (
        WHERE EXISTS (
            SELECT 1 FROM {{events_table}} e2
            WHERE e2.session_id = {{events_table}}.session_id
            AND (e2.type = 'conversion' OR (
                SELECT COUNT(DISTINCT url)
                FROM {{events_table}} e3
                WHERE e3.session_id = {{events_table}}.session_id
            ) >= 3)
        )
    )) AS conversions,
    -- Daily conversion rate
    ROUND(
        100.0 * COUNT(DISTINCT session_id FILTER (
            WHERE EXISTS (
                SELECT 1 FROM {{events_table}} e2
                WHERE e2.session_id = {{events_table}}.session_id
                AND (e2.type = 'conversion' OR (
                    SELECT COUNT(DISTINCT url)
                    FROM {{events_table}} e3
                    WHERE e3.session_id = {{events_table}}.session_id
                ) >= 3)
            )
        )) / NULLIF(COUNT(DISTINCT session_id), 0),
        2
    ) AS daily_conversion_rate
FROM {{events_table}}
WHERE ts >= '{{start_date}}'::TIMESTAMP
    AND ts < '{{end_date}}'::TIMESTAMP
    AND session_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
