-- Per-asset performance: the trace.assets dimension joined to ad events
--
-- The assets table (synced from the ad network creative APIs and exploded
-- to headline / image / landing_page grain by the syncer — schema in
-- analytics/schemas/assets_iceberg.sql) carries the network-specific
-- creative id each asset was synced from. Joining ad_events on that id —
-- and network, since creative ids are only unique within a network — lands
-- each event on the individual assets that composed its creative, which is
-- what the asset-performance and creative-arbitrage reporting needs.
--
-- The join is a LEFT JOIN with the event date filter inside the ON clause,
-- so assets with no traffic in the window still appear (with zeros) rather
-- than silently dropping out of the dimension report.
SELECT
    a.asset_id,
    a.network,
    a.type AS asset_type,
    a.content,
    a.campaign_name,
    COUNT(*) FILTER (WHERE e.type = 'pageview') AS views,
    COUNT(*) FILTER (WHERE e.type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE e.type IN ('conversion', 'purchase', 'signup')) AS conversions,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE e.type = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE e.type = 'pageview'), 0),
        2
    ) AS ctr,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE e.type IN ('conversion', 'purchase', 'signup')) /
        NULLIF(COUNT(*) FILTER (WHERE e.type = 'click'), 0),
        2
    ) AS conversion_rate,
    MIN(e.ts) AS first_event,
    MAX(e.ts) AS last_event
FROM {{assets_table}} a
LEFT JOIN {{events_table}} e
    ON e.creative_id = a.creative_id
    AND e.network = a.network
    AND e.ts >= '{{start_date}}'::TIMESTAMP
    AND e.ts < '{{end_date}}'::TIMESTAMP
GROUP BY 1, 2, 3, 4, 5
ORDER BY clicks DESC, views DESC
LIMIT 50;
