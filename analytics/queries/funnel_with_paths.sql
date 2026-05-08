-- Funnel analysis with user journey paths
-- Analyzes drop-off at each step of a defined funnel
-- Modify the funnel steps in the CASE statement to match your goals
WITH funnel_steps AS (
    SELECT
        user_id,
        session_id,
        type AS event_type,
        url,
        ts,
        CASE
            WHEN url LIKE '%/pricing%' THEN 1
            WHEN url LIKE '%/signup%' OR type = 'signup' THEN 2
            WHEN url LIKE '%/checkout%' OR type = 'purchase' THEN 3
            WHEN url LIKE '%/thank-you%' OR type = 'conversion' THEN 4
            ELSE 0
        END AS funnel_step
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
),
user_progression AS (
    SELECT
        user_id,
        ARRAY_AGG(DISTINCT funnel_step) FILTER (WHERE funnel_step > 0) AS completed_steps
    FROM funnel_steps
    WHERE funnel_step > 0
    GROUP BY user_id
),
step_counts AS (
    SELECT
        funnel_step,
        COUNT(DISTINCT user_id) AS users_reached_step,
        LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_step) AS prev_step_users
    FROM funnel_steps
    WHERE funnel_step > 0
    GROUP BY funnel_step
)
SELECT
    funnel_step AS step_number,
    CASE funnel_step
        WHEN 1 THEN 'Pricing View'
        WHEN 2 THEN 'Signup'
        WHEN 3 THEN 'Checkout'
        WHEN 4 THEN 'Conversion'
        ELSE 'Unknown'
    END AS step_name,
    users_reached_step,
    prev_step_users,
    CASE
        WHEN prev_step_users > 0 THEN ROUND(100.0 * users_reached_step / prev_step_users, 2)
        ELSE 100.0
    END AS conversion_rate_pct
FROM step_counts
ORDER BY funnel_step;
