-- ============================================================================
-- Creative Fatigue Detection
-- ============================================================================
-- Tracks performance decay of individual creative assets over time.
--
-- Metrics calculated:
-- - Rolling 7-day CTR average per creative
-- - Fatigue detection: >20% decline over 3-day window
-- - Fatigue score: 0-1 (higher = more fatigued)
-- - Days since peak CTR
-- - Recommended rotation date
--
-- Usage:
--   Replace {{events_table}} with your events table name
--   Adjust lookback period as needed (default: 60 days)
-- ============================================================================

WITH
-- ----------------------------------------------------------------------------
-- Step 1: Aggregate daily metrics per creative
-- ----------------------------------------------------------------------------
creative_daily AS (
    SELECT
        DATE_TRUNC('day', ts) AS date,
        -- Use creative_id if available, otherwise create composite key
        COALESCE(
            creative_id,
            CONCAT(
                COALESCE(network, 'unknown'), '|',
                COALESCE(params->>'tb_headline', params->>'ob_headline', ''),
                '|',
                COALESCE(params->>'tb_thumbnail', params->>'ob_thumbnail', '')
            )
        ) AS creative_key,
        -- Extract creative metadata for display
        COALESCE(creative_id, CONCAT('composite-', MD5(CONCAT(
            COALESCE(network, 'unknown'),
            COALESCE(params->>'tb_headline', params->>'ob_headline', ''),
            COALESCE(params->>'tb_thumbnail', params->>'ob_thumbnail', '')
        )))) AS creative_id,
        COALESCE(headline, params->>'tb_headline', params->>'ob_headline', '(unnamed)') AS headline,
        COALESCE(image_id, params->>'tb_thumbnail', params->>'ob_thumbnail', '') AS image_id,
        COALESCE(network, 'unknown') AS network,
        COUNT(*) FILTER (WHERE type = 'pageview') AS views,
        COUNT(*) FILTER (WHERE type = 'click') AS clicks
    FROM {{events_table}}
    WHERE ts >= CURRENT_DATE - INTERVAL '60 days'
        AND (
            creative_id IS NOT NULL
            OR params->>'tb_headline' IS NOT NULL
            OR params->>'ob_headline' IS NOT NULL
        )
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING COUNT(*) FILTER (WHERE type = 'pageview') >= 10
),

-- ----------------------------------------------------------------------------
-- Step 2: Calculate daily CTR and fill gaps for missing days
-- ----------------------------------------------------------------------------
daily_ctr_with_gaps AS (
    SELECT
        date,
        creative_key,
        creative_id,
        headline,
        image_id,
        network,
        views,
        clicks,
        CASE
            WHEN views > 0 THEN 100.0 * clicks / views
            ELSE NULL
        END AS ctr_pct
    FROM creative_daily
),

-- ----------------------------------------------------------------------------
-- Step 3: Generate complete date series for each creative (fills gaps)
-- ----------------------------------------------------------------------------
date_series AS (
    SELECT DISTINCT date
    FROM daily_ctr_with_gaps
),
creative_list AS (
    SELECT DISTINCT creative_key, creative_id, headline, image_id, network
    FROM daily_ctr_with_gaps
),
all_dates AS (
    SELECT d.date, c.creative_key, c.creative_id, c.headline, c.image_id, c.network
    FROM date_series d
    CROSS JOIN creative_list c
),
daily_ctr_filled AS (
    SELECT
        a.date,
        a.creative_key,
        a.creative_id,
        a.headline,
        a.image_id,
        a.network,
        COALESCE(d.views, 0) AS views,
        COALESCE(d.clicks, 0) AS clicks,
        d.ctr_pct
    FROM all_dates a
    LEFT JOIN daily_ctr_with_gaps d
        ON a.date = d.date
        AND a.creative_key = d.creative_key
),

-- ----------------------------------------------------------------------------
-- Step 4: Calculate rolling 7-day average CTR
-- ----------------------------------------------------------------------------
rolling_ctr AS (
    SELECT
        date,
        creative_key,
        creative_id,
        headline,
        image_id,
        network,
        SUM(views) AS views_7d,
        SUM(clicks) AS clicks_7d,
        CASE
            WHEN SUM(views) > 0 THEN 100.0 * SUM(clicks) / SUM(views)
            ELSE NULL
        END AS ctr_7d_avg
    FROM daily_ctr_filled
    GROUP BY
        creative_key, creative_id, headline, image_id, network,
        date
    HAVING SUM(views) >= 50  -- Minimum 50 views in 7-day window
),

-- ----------------------------------------------------------------------------
-- Step 5: Find peak CTR and days since peak for each creative
-- ----------------------------------------------------------------------------
peak_stats AS (
    SELECT
        creative_key,
        MAX(ctr_7d_avg) AS peak_ctr,
        MIN(date) FILTER (WHERE ctr_7d_avg = MAX(ctr_7d_avg) OVER (PARTITION BY creative_key)) AS peak_date
    FROM rolling_ctr
    WHERE ctr_7d_avg IS NOT NULL
    GROUP BY creative_key
),

-- ----------------------------------------------------------------------------
-- Step 6: Calculate 3-day window comparison for fatigue detection
-- ----------------------------------------------------------------------------
window_comparison AS (
    SELECT
        r1.date,
        r1.creative_key,
        r1.creative_id,
        r1.headline,
        r1.image_id,
        r1.network,
        r1.ctr_7d_avg AS current_ctr_7d,
        r2.ctr_7d_avg AS prior_ctr_7d,
        r2.date AS prior_date,
        EXTRACT(DAY FROM CURRENT_DATE - p.peak_date)::INTEGER AS days_since_peak,
        p.peak_ctr
    FROM rolling_ctr r1
    LEFT JOIN rolling_ctr r2
        ON r1.creative_key = r2.creative_key
        AND r2.date = r1.date - INTERVAL '3 days'
    INNER JOIN peak_stats p
        ON r1.creative_key = p.creative_key
    WHERE r1.date = CURRENT_DATE - INTERVAL '1 day'  -- Yesterday's complete data
        AND r1.ctr_7d_avg IS NOT NULL
),

-- ----------------------------------------------------------------------------
-- Step 7: Calculate fatigue metrics
-- ----------------------------------------------------------------------------
fatigue_metrics AS (
    SELECT
        creative_key,
        creative_id,
        headline,
        image_id,
        network,
        current_ctr_7d,
        prior_ctr_7d,
        peak_ctr,
        days_since_peak,
        -- Calculate decline percentage
        CASE
            WHEN prior_ctr_7d IS NOT NULL AND prior_ctr_7d > 0
            THEN 100.0 * (prior_ctr_7d - current_ctr_7d) / prior_ctr_7d
            ELSE 0
        END AS decline_pct,
        -- Calculate fatigue score (0-1)
        -- Score based on: decline from 3 days ago + decline from peak + days since peak
        CASE
            WHEN prior_ctr_7d IS NOT NULL AND prior_ctr_7d > 0 AND peak_ctr > 0
            THEN LEAST(1.0,
                -- 50% weight: decline from 3 days ago (capped at 40% decline = 0.5)
                LEAST(0.5, 1.25 * (prior_ctr_7d - current_ctr_7d) / prior_ctr_7d) +
                -- 30% weight: decline from peak (capped at 50% decline = 0.3)
                LEAST(0.3, 0.6 * (peak_ctr - current_ctr_7d) / peak_ctr) +
                -- 20% weight: days since peak (capped at 30 days = 0.2)
                LEAST(0.2, days_since_peak::NUMERIC / 150.0)
            )
            ELSE 0
        END AS fatigue_score
    FROM window_comparison
)

-- ----------------------------------------------------------------------------
-- Final Output: Flagged creatives with fatigue metrics
-- ----------------------------------------------------------------------------
SELECT
    creative_id,
    headline,
    image_id,
    network,
    ROUND(current_ctr_7d, 2) AS current_ctr_7d_pct,
    ROUND(prior_ctr_7d, 2) AS prior_ctr_7d_pct,
    ROUND(peak_ctr, 2) AS peak_ctr_pct,
    ROUND(decline_pct, 1) AS decline_from_3d_ago_pct,
    days_since_peak,
    -- Fatigue score: 0-1 (higher = more fatigued)
    ROUND(fatigue_score, 2) AS fatigue_score,
    -- Flag as fatigued if decline > 20% over 3-day window
    CASE
        WHEN decline_pct > 20 THEN 'YES'
        ELSE 'NO'
    END AS is_fatigued,
    -- Recommended rotation date
    -- If fatigued: rotate immediately
    -- If fatigue_score > 0.5: rotate within 7 days
    -- If fatigue_score > 0.3: rotate within 14 days
    -- Otherwise: no immediate action needed
    CASE
        WHEN decline_pct > 20 THEN CURRENT_DATE
        WHEN fatigue_score > 0.5 THEN CURRENT_DATE + INTERVAL '7 days'
        WHEN fatigue_score > 0.3 THEN CURRENT_DATE + INTERVAL '14 days'
        WHEN fatigue_score > 0.15 THEN CURRENT_DATE + INTERVAL '30 days'
        ELSE NULL
    END AS recommended_rotation_date,
    -- Action recommendation
    CASE
        WHEN decline_pct > 20 THEN 'URGENT: Rotate immediately'
        WHEN fatigue_score > 0.5 THEN 'HIGH: Prepare replacement within 7 days'
        WHEN fatigue_score > 0.3 THEN 'MEDIUM: Plan rotation within 14 days'
        WHEN fatigue_score > 0.15 THEN 'LOW: Monitor closely'
        ELSE 'NORMAL: No action needed'
    END AS action_recommendation
FROM fatigue_metrics
WHERE current_ctr_7d IS NOT NULL
    AND prior_ctr_7d IS NOT NULL
ORDER BY fatigue_score DESC, decline_pct DESC
LIMIT 50;

-- ----------------------------------------------------------------------------
-- Summary Statistics (all creatives)
-- ----------------------------------------------------------------------------
-- Run separately for overall fatigue health check

SELECT
    COUNT(DISTINCT creative_key) AS total_creatives_tracked,
    COUNT(DISTINCT creative_key) FILTER (WHERE decline_pct > 20) AS urgent_fatigue_count,
    COUNT(DISTINCT creative_key) FILTER (WHERE fatigue_score > 0.5) AS high_fatigue_count,
    COUNT(DISTINCT creative_key) FILTER (WHERE fatigue_score > 0.3 AND fatigue_score <= 0.5) AS medium_fatigue_count,
    ROUND(AVG(fatigue_score), 2) AS avg_fatigue_score,
    ROUND(AVG(decline_pct), 1) AS avg_decline_pct,
    ROUND(AVG(days_since_peak), 0) AS avg_days_since_peak
FROM fatigue_metrics;
