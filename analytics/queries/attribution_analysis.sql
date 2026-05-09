-- Multi-touch attribution analysis
-- Tracks all touchpoints in a user's journey leading to conversion
WITH user_touchpoints AS (
    SELECT DISTINCT
        user_id,
        session_id,
        FIRST_VALUE(network) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS network,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS campaign_id,
        FIRST_VALUE(creative_id) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS creative_id,
        FIRST_VALUE(url) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS url,
        FIRST_VALUE(type) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS event_type,
        MIN(ts) OVER (PARTITION BY user_id, session_id ORDER BY ts) AS session_ts
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
),
conversions AS (
    SELECT
        user_id,
        session_id,
        MIN(ts) AS conversion_ts,
        type AS conversion_type
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND (type = 'conversion' OR type = 'purchase' OR type = 'signup')
    GROUP BY user_id, session_id, type
),
journeys AS (
    SELECT
        t.user_id,
        t.session_id,
        t.network,
        t.campaign_id,
        t.creative_id,
        t.url,
        t.event_type,
        t.session_ts,
        c.conversion_ts,
        c.conversion_type,
        ROW_NUMBER() OVER (PARTITION BY t.user_id ORDER BY t.session_ts) AS touch_position,
        COUNT(*) OVER (PARTITION BY t.user_id) AS total_touches,
        EXTRACT(DAY FROM (c.conversion_ts - t.session_ts)) AS days_to_conversion
    FROM user_touchpoints t
    INNER JOIN conversions c ON t.user_id = c.user_id
    WHERE t.session_ts <= c.conversion_ts
)
SELECT
    user_id,
    session_id,
    network,
    campaign_id,
    creative_id,
    url,
    event_type,
    session_ts,
    conversion_ts,
    conversion_type,
    touch_position,
    total_touches,
    days_to_conversion,
    CASE WHEN touch_position = 1 THEN 'first_touch' ELSE NULL END AS attribution_model_first,
    CASE WHEN touch_position = total_touches THEN 'last_touch' ELSE NULL END AS attribution_model_last
FROM journeys
ORDER BY user_id, touch_position
LIMIT 1000;
