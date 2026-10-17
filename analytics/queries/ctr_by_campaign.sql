-- Click-through rate by campaign. Keep unsynced event campaigns and synced
-- campaigns with no traffic; ad networks can reuse a campaign ID.
WITH filtered_events AS (
    SELECT network, campaign_id, type
    FROM {{events_table}}
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        -- Prune event day partitions in Parquet mode.
        AND {{ts_partition_filter}}
)
SELECT
    COALESCE(c.campaign_id, e.campaign_id) AS campaign_id,
    COALESCE(c.network, e.network) AS network,
    c.campaign_name,
    c.status,
    c.account_id,
    COUNT(*) FILTER (WHERE e.type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE e.type = 'click') AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE e.type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE e.type = 'pageview'), 0),
        2
    ) AS ctr_pct
FROM {{campaigns_table}} c
FULL OUTER JOIN filtered_events e
    ON e.network = c.network
    AND e.campaign_id = c.campaign_id
GROUP BY COALESCE(c.campaign_id, e.campaign_id), COALESCE(c.network, e.network),
    c.campaign_name, c.status, c.account_id
ORDER BY clicks DESC, views DESC
LIMIT 50;
