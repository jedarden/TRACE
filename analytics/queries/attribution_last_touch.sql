-- Last-Touch Attribution: Credits the last campaign/source before a conversion
-- This model is useful for understanding what directly led to conversions
WITH session_touches AS (
    SELECT
        session_id,
        type,
        params->>'utm_source' AS utm_source,
        params->>'utm_medium' AS utm_medium,
        params->>'utm_campaign' AS utm_campaign,
        params->>'utm_content' AS utm_content,
        params->>'utm_term' AS utm_term,
        params->>'network' AS network,
        params->>'campaign_id' AS campaign_id,
        params->>'creative_id' AS creative_id,
        params->>'revenue' AS revenue,
        ts,
        LEAD(type) OVER (PARTITION BY session_id ORDER BY ts) AS next_type
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
),
conversion_touches AS (
    SELECT
        session_id,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        network,
        campaign_id,
        creative_id,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts DESC) AS touch_rank
    FROM session_touches
    WHERE next_type = 'conversion' OR (type = 'conversion' AND LAG(type) OVER (PARTITION BY session_id ORDER BY ts) IS NOT NULL)
),
last_touch_attributed AS (
    SELECT
        COALESCE(utm_source, '(direct)') AS source,
        COALESCE(utm_medium, '(none)') AS medium,
        COALESCE(utm_campaign, '(not set)') AS campaign,
        COALESCE(utm_content, '(not set)') AS content,
        COALESCE(utm_term, '(not set)') AS term,
        COALESCE(network, '(unknown)') AS ad_network,
        campaign_id,
        creative_id,
        COUNT(*) AS conversions,
        COALESCE(SUM(revenue::DECIMAL), 0) AS revenue,
        COUNT(DISTINCT session_id) AS attributed_sessions
    FROM conversion_touches
    WHERE touch_rank = 1
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
    attributed_sessions,
    conversions,
    ROUND(revenue, 2) AS attributed_revenue,
    ROUND(
        100.0 * conversions / NULLIF(SUM(conversions) OVER (), 0),
        2
    ) AS attribution_pct
FROM last_touch_attributed
WHERE conversions > 0 OR revenue > 0
ORDER BY attributed_revenue DESC
LIMIT 50;
