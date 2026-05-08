-- Find campaigns with no recent traffic
WITH campaign_activity AS (
    SELECT
        params->>'utm_campaign' AS campaign,
        MAX(ts) AS last_event,
        COUNT(*) AS total_events
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
)
SELECT
    campaign,
    last_event,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - last_event) AS days_since_last_event,
    total_events
FROM campaign_activity
WHERE last_event < CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY 2 ASC;
