-- Linear Attribution: Distributes conversion credit equally across all touchpoints in a session
-- This model gives fair credit to the entire customer journey
WITH session_conversions AS (
    SELECT
        session_id,
        COUNT(*) FILTER (WHERE type = 'conversion') AS conversions,
        COALESCE(SUM((params->>'revenue')::DECIMAL) FILTER (WHERE type = 'conversion'), 0) AS revenue
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id
    HAVING COUNT(*) FILTER (WHERE type = 'conversion') > 0
),
session_touchpoints AS (
    SELECT DISTINCT
        e.session_id,
        e.params->>'utm_source' AS utm_source,
        e.params->>'utm_medium' AS utm_medium,
        e.params->>'utm_campaign' AS utm_campaign,
        e.params->>'utm_content' AS utm_content,
        e.params->>'utm_term' AS utm_term,
        e.params->>'network' AS network,
        e.params->>'campaign_id' AS campaign_id,
        e.params->>'creative_id' AS creative_id,
        sc.conversions AS total_conversions,
        sc.revenue AS total_revenue,
        COUNT(*) OVER (PARTITION BY e.session_id) AS touchpoint_count
    FROM {{events_table}} e
    INNER JOIN session_conversions sc ON e.session_id = sc.session_id
    WHERE e.ts >= '{{start_date}}'::TIMESTAMP
        AND e.ts < '{{end_date}}'::TIMESTAMP
        AND e.session_id IS NOT NULL
),
linear_attribution AS (
    SELECT
        COALESCE(utm_source, '(direct)') AS source,
        COALESCE(utm_medium, '(none)') AS medium,
        COALESCE(utm_campaign, '(not set)') AS campaign,
        COALESCE(utm_content, '(not set)') AS content,
        COALESCE(utm_term, '(not set)') AS term,
        COALESCE(network, '(unknown)') AS ad_network,
        campaign_id,
        creative_id,
        SUM(total_conversions::DECIMAL / touchpoint_count) AS attributed_conversions,
        SUM(total_revenue / touchpoint_count) AS attributed_revenue,
        COUNT(DISTINCT session_id) AS touched_sessions
    FROM session_touchpoints
    GROUP BY
        utm_source, utm_medium, utm_campaign, utm_content,
        utm_term, network, campaign_id, creative_id
)
SELECT
    source,
    medium,
    campaign,
    content,
    term,
    ad_network,
    touched_sessions,
    ROUND(attributed_conversions, 2) AS attributed_conversions,
    ROUND(attributed_revenue, 2) AS attributed_revenue,
    ROUND(
        100.0 * attributed_conversions / NULLIF(SUM(attributed_conversions) OVER (), 0),
        2
    ) AS attribution_pct
FROM linear_attribution
WHERE attributed_conversions > 0 OR attributed_revenue > 0
ORDER BY attributed_revenue DESC
LIMIT 50;
