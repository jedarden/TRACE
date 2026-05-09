-- First-Touch Attribution: Credits the first campaign/source in a session for conversions
-- This model is useful for understanding which campaigns initially acquire users
WITH session_touches AS (
    SELECT
        session_id,
        MIN(ts) AS first_touch_ts,
        FIRST_VALUE(params->>'utm_source') OVER (PARTITION BY session_id ORDER BY ts) AS first_source,
        FIRST_VALUE(params->>'utm_medium') OVER (PARTITION BY session_id ORDER BY ts) AS first_medium,
        FIRST_VALUE(params->>'utm_campaign') OVER (PARTITION BY session_id ORDER BY ts) AS first_campaign,
        FIRST_VALUE(params->>'utm_content') OVER (PARTITION BY session_id ORDER BY ts) AS first_content,
        FIRST_VALUE(params->>'utm_term') OVER (PARTITION BY session_id ORDER BY ts) AS first_term,
        FIRST_VALUE(params->>'network') OVER (PARTITION BY session_id ORDER BY ts) AS first_network,
        FIRST_VALUE(params->>'campaign_id') OVER (PARTITION BY session_id ORDER BY ts) AS first_campaign_id,
        FIRST_VALUE(params->>'creative_id') OVER (PARTITION BY session_id ORDER BY ts) AS first_creative_id,
        COUNT(*) FILTER (WHERE type = 'conversion') AS conversions,
        COALESCE(SUM((params->>'revenue')::DECIMAL) FILTER (WHERE type = 'conversion'), 0) AS revenue
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id, ts, type, params
),
first_touch_attributed AS (
    SELECT DISTINCT
        first_source AS utm_source,
        first_medium AS utm_medium,
        first_campaign AS utm_campaign,
        first_content AS utm_content,
        first_term AS utm_term,
        first_network AS network,
        first_campaign_id AS campaign_id,
        first_creative_id AS creative_id,
        COALESCE(SUM(conversions), 0) AS conversions,
        COALESCE(SUM(revenue), 0) AS revenue,
        COUNT(DISTINCT session_id) AS attributed_sessions
    FROM session_touches
    GROUP BY
        first_source, first_medium, first_campaign, first_content,
        first_term, first_network, first_campaign_id, first_creative_id
)
SELECT
    COALESCE(utm_source, '(direct)') AS source,
    COALESCE(utm_medium, '(none)') AS medium,
    COALESCE(utm_campaign, '(not set)') AS campaign,
    COALESCE(first_content, '(not set)') AS content,
    COALESCE(utm_term, '(not set)') AS term,
    COALESCE(network, '(unknown)') AS ad_network,
    attributed_sessions,
    conversions,
    ROUND(revenue, 2) AS attributed_revenue,
    ROUND(
        100.0 * conversions / NULLIF(SUM(conversions) OVER (), 0),
        2
    ) AS attribution_pct
FROM first_touch_attributed
WHERE conversions > 0 OR revenue > 0
ORDER BY attributed_revenue DESC
LIMIT 50;
