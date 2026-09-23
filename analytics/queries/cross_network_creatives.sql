-- Find creatives running on multiple networks
WITH creative_ids AS (
    -- Taboola
    SELECT
        params->>'tb_image' AS creative_id,
        'taboola' AS network
    FROM {{events_table}}
    WHERE params->>'tb_image' IS NOT NULL
        AND ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
        AND {{ts_partition_filter}}
    UNION ALL
    -- Outbrain
    SELECT
        params->>'ob_creative' AS creative_id,
        'outbrain' AS network
    FROM {{events_table}}
    WHERE params->>'ob_creative' IS NOT NULL
        AND ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP

    -- partition-column conjunct: prunes day directories on the Parquet read path (docs/analytics/iceberg_partition_pruning.md)
        AND {{ts_partition_filter}}
)
SELECT
    creative_id,
    COUNT(DISTINCT network) AS num_networks,
    array_agg(DISTINCT network) AS networks
FROM creative_ids
GROUP BY 1
HAVING COUNT(DISTINCT network) > 1
ORDER BY 2 DESC;
