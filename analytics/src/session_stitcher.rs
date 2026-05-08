//! Session Stitching: User journey reconstruction from events
//!
//! This module provides functionality for reconstructing user journeys
//! by stitching together events into sessions and analyzing user behavior patterns.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Session configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// Maximum gap between events in a session (minutes)
    pub session_timeout_minutes: i64,
    /// Maximum session duration (hours)
    pub max_session_hours: i64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            session_timeout_minutes: 30,
            max_session_hours: 4,
        }
    }
}

/// Reconstructed session from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session identifier
    pub session_id: String,
    /// User identifier (optional)
    pub user_id: Option<String>,
    /// Session start timestamp
    pub start_ts: chrono::DateTime<chrono::Utc>,
    /// Session end timestamp
    pub end_ts: chrono::DateTime<chrono::Utc>,
    /// Number of events in session
    pub event_count: i64,
    /// Entry page (first URL)
    pub landing_page: Option<String>,
    /// Exit page (last URL)
    pub exit_page: Option<String>,
    /// Unique pages visited
    pub unique_pages: i64,
    /// Session duration in seconds
    pub duration_seconds: i64,
    /// Whether session bounced (single page view)
    pub is_bounce: bool,
    /// Traffic source (if available)
    pub source: Option<String>,
    /// Campaign (if available)
    pub campaign: Option<String>,
    /// Device type
    pub device_type: Option<String>,
}

/// User journey across multiple sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserJourney {
    /// User identifier
    pub user_id: String,
    /// All sessions for this user
    pub sessions: Vec<Session>,
    /// First session timestamp
    pub first_session_ts: chrono::DateTime<chrono::Utc>,
    /// Last session timestamp
    pub last_session_ts: chrono::DateTime<chrono::Utc>,
    /// Total number of sessions
    pub total_sessions: i64,
    /// Total events across all sessions
    pub total_events: i64,
    /// Total time engaged (seconds)
    pub total_engagement_seconds: i64,
    /// Unique pages visited across all sessions
    pub unique_pages: HashSet<String>,
    /// Conversion events encountered
    pub conversions: Vec<ConversionEvent>,
}

/// Conversion or goal completion event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionEvent {
    /// Timestamp of conversion
    pub ts: chrono::DateTime<chrono::Utc>,
    /// Conversion type
    pub conversion_type: String,
    /// Session ID where conversion occurred
    pub session_id: String,
    /// Attribution touch count
    pub touches: i64,
    /// Days from first session to conversion
    pub days_to_convert: i64,
}

/// Touchpoint in attribution journey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Touchpoint {
    /// Timestamp
    pub ts: chrono::DateTime<chrono::Utc>,
    /// Session identifier
    pub session_id: String,
    /// Network/source
    pub network: Option<String>,
    /// Campaign identifier
    pub campaign_id: Option<String>,
    /// Creative identifier
    pub creative_id: Option<String>,
    /// Page URL
    pub url: String,
    /// Event type
    pub event_type: String,
    /// Position in journey (1-indexed)
    pub position: i64,
    /// Whether this was the converting touch
    pub is_conversion: bool,
}

/// Attribution analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributionAnalysis {
    /// User identifier
    pub user_id: String,
    /// All touchpoints in user's journey
    pub touchpoints: Vec<Touchpoint>,
    /// Total number of touchpoints
    pub total_touchpoints: i64,
    /// First touch network
    pub first_touch_network: Option<String>,
    /// First touch campaign
    pub first_touch_campaign: Option<String>,
    /// Last touch network
    pub last_touch_network: Option<String>,
    /// Last touch campaign
    pub last_touch_campaign: Option<String>,
    /// Conversions
    pub conversions: Vec<ConversionEvent>,
}

/// Session stitcher for reconstructing user journeys
pub struct SessionStitcher;

impl SessionStitcher {
    /// Generate SQL for session reconstruction
    pub fn session_reconstruction_sql(config: &SessionConfig) -> String {
        format!(
            r#"
-- Reconstruct sessions from events using gap-based sessionization
WITH event_gaps AS (
    SELECT
        *,
        LAG(ts) OVER (PARTITION BY session_id ORDER BY ts) AS prev_ts,
        EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (PARTITION BY session_id ORDER BY ts))) / 60 AS gap_minutes
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
),
session_assignments AS (
    SELECT
        *,
        SUM(CASE WHEN gap_minutes > {} OR gap_minutes IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY session_id ORDER BY ts) AS reconstructed_session_seq
    FROM event_gaps
),
sessions AS (
    SELECT
        session_id || '_' || reconstructed_session_seq::TEXT AS reconstructed_session_id,
        session_id,
        user_id,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT url) AS unique_pages,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT AS duration_seconds,
        FIRST_VALUE(url) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS landing_page,
        FIRST_VALUE(network) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS source_network,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS campaign,
        FIRST_VALUE(device_type) OVER (PARTITION BY session_id, reconstructed_session_seq ORDER BY ts) AS device_type
    FROM session_assignments
    WHERE EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT / 3600 <= {}
    GROUP BY session_id, reconstructed_session_seq, user_id
)
SELECT
    reconstructed_session_id AS session_id,
    user_id,
    session_start,
    session_end,
    event_count,
    unique_pages,
    duration_seconds,
    landing_page,
    (CASE WHEN event_count = 1 THEN true ELSE false END) AS is_bounce,
    source_network,
    campaign,
    device_type
FROM sessions
ORDER BY session_start DESC
LIMIT 1000;
"#,
            config.session_timeout_minutes, config.max_session_hours
        )
    }

    /// Generate SQL for user journey analysis
    pub fn user_journey_sql(user_id: &str, config: &SessionConfig) -> String {
        format!(
            r#"
-- Reconstruct complete user journey across all sessions
WITH user_events AS (
    SELECT
        *,
        LAG(ts) OVER (ORDER BY ts) AS prev_ts,
        EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (ORDER BY ts))) / 60 AS gap_minutes
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE user_id = '{}'
        AND ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
),
session_markers AS (
    SELECT
        *,
        SUM(CASE WHEN gap_minutes > {} OR gap_minutes IS NULL THEN 1 ELSE 0 END)
            OVER (ORDER BY ts) AS session_seq
    FROM user_events
),
sessions AS (
    SELECT
        session_seq,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT url) AS unique_pages,
        EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts)))::BIGINT AS duration_seconds,
        FIRST_VALUE(url) OVER (PARTITION BY session_seq ORDER BY ts) AS landing_page,
        FIRST_VALUE(network) OVER (PARTITION BY session_seq ORDER BY ts) AS source_network,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY session_seq ORDER BY ts) AS campaign
    FROM session_markers
    GROUP BY session_seq
)
SELECT
    session_seq,
    session_start,
    session_end,
    event_count,
    unique_pages,
    duration_seconds,
    landing_page,
    source_network,
    campaign
FROM sessions
ORDER BY session_start;
"#,
            user_id, config.session_timeout_minutes
        )
    }

    /// Generate SQL for attribution analysis
    pub fn attribution_sql(conversion_type: Option<&str>) -> String {
        let conversion_filter = if let Some(ct) = conversion_type {
            format!("AND type = '{}'", ct)
        } else {
            "AND (type = 'conversion' OR type = 'purchase' OR type = 'signup')"
        };

        format!(
            r#"
-- Multi-touch attribution analysis
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
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
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
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        {}
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
"#,
            conversion_filter
        )
    }

    /// Generate SQL for common paths analysis
    pub fn common_paths_sql() -> String {
        r#"
-- Most common user paths through the site
WITH session_paths AS (
    SELECT
        session_id,
        ARRAY_AGG(url ORDER BY ts) AS path,
        COUNT(*) AS steps,
        MIN(ts) AS session_start
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
        AND type = 'pageview'
    GROUP BY session_id
    HAVING COUNT(*) <= 10  -- Limit to reasonable path lengths
),
path_frequencies AS (
    SELECT
        path,
        steps,
        COUNT(*) AS frequency
    FROM session_paths
    GROUP BY path, steps
)
SELECT
    path,
    steps,
    frequency,
    ROUND(100.0 * frequency / SUM(frequency) OVER (), 2) AS percentage
FROM path_frequencies
ORDER BY frequency DESC
LIMIT 50;
"#.to_string()
    }

    /// Generate SQL for session flow analysis (transition matrix)
    pub fn session_flow_matrix_sql() -> String {
        r#"
-- Session flow transition matrix for visualization
WITH page_transitions AS (
    SELECT
        session_id,
        url,
        LEAD(url) OVER (PARTITION BY session_id ORDER BY ts) AS next_url,
        type,
        LEAD(type) OVER (PARTITION BY session_id ORDER BY ts) AS next_type
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
        AND type = 'pageview'
)
SELECT
    url AS from_page,
    next_url AS to_page,
    COUNT(*) AS transition_count,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM page_transitions
WHERE next_url IS NOT NULL
GROUP BY from_page, to_page
ORDER BY transition_count DESC
LIMIT 100;
"#.to_string()
    }

    /// Generate SQL for cohort-based journey analysis
    pub fn cohort_journey_sql(cohort_type: &str) -> String {
        match cohort_type {
            "acquisition" => r#"
-- User journey by acquisition cohort (first touch network)
WITH user_cohorts AS (
    SELECT
        user_id,
        FIRST_VALUE(network) OVER (PARTITION BY user_id ORDER BY ts) AS acquisition_network,
        MIN(ts) OVER (PARTITION BY user_id) AS first_touch_ts
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
),
user_sessions AS (
    SELECT
        e.user_id,
        c.acquisition_network,
        c.first_touch_ts,
        e.session_id,
        MIN(e.ts) AS session_start,
        COUNT(*) AS events,
        COUNT(DISTINCT e.url) AS unique_pages,
        EXTRACT(DAY FROM (MIN(e.ts) - c.first_touch_ts)) AS day_number
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet') e
    INNER JOIN user_cohorts c ON e.user_id = c.user_id
    WHERE e.ts >= '{{start_date}}'::TIMESTAMP
        AND e.ts < '{{end_date}}'::TIMESTAMP
    GROUP BY e.user_id, c.acquisition_network, c.first_touch_ts, e.session_id
)
SELECT
    acquisition_network AS cohort,
    day_number,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS sessions,
    AVG(events) AS avg_events_per_session,
    AVG(unique_pages) AS avg_pages_per_session
FROM user_sessions
GROUP BY cohort, day_number
ORDER BY cohort, day_number;
"#.to_string(),

            "campaign" => r#"
-- User journey by campaign cohort
WITH user_cohorts AS (
    SELECT
        user_id,
        FIRST_VALUE(campaign_id) OVER (PARTITION BY user_id ORDER BY ts) AS campaign,
        MIN(ts) OVER (PARTITION BY user_id) AS first_touch_ts
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
        AND campaign_id IS NOT NULL
),
user_sessions AS (
    SELECT
        e.user_id,
        c.campaign,
        c.first_touch_ts,
        e.session_id,
        MIN(e.ts) AS session_start,
        COUNT(*) AS events,
        COUNT(DISTINCT e.url) AS unique_pages,
        EXTRACT(DAY FROM (MIN(e.ts) - c.first_touch_ts)) AS day_number
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet') e
    INNER JOIN user_cohorts c ON e.user_id = c.user_id
    WHERE e.ts >= '{{start_date}}'::TIMESTAMP
        AND e.ts < '{{end_date}}'::TIMESTAMP
    GROUP BY e.user_id, c.campaign, c.first_touch_ts, e.session_id
)
SELECT
    campaign AS cohort,
    day_number,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS sessions,
    AVG(events) AS avg_events_per_session,
    AVG(unique_pages) AS avg_pages_per_session
FROM user_sessions
GROUP BY cohort, day_number
ORDER BY cohort, day_number;
"#.to_string(),

            _ => r#"
-- Default user journey by day cohort
WITH user_cohorts AS (
    SELECT
        user_id,
        DATE(MIN(ts)) AS cohort_date
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
    GROUP BY user_id
),
user_sessions AS (
    SELECT
        e.user_id,
        c.cohort_date,
        e.session_id,
        MIN(e.ts) AS session_start,
        COUNT(*) AS events,
        COUNT(DISTINCT e.url) AS unique_pages,
        EXTRACT(DAY FROM (MIN(e.ts) - c.cohort_date)) AS day_number
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet') e
    INNER JOIN user_cohorts c ON e.user_id = c.user_id
    WHERE e.ts >= '{{start_date}}'::TIMESTAMP
        AND e.ts < '{{end_date}}'::TIMESTAMP
    GROUP BY e.user_id, c.cohort_date, e.session_id
)
SELECT
    cohort_date AS cohort,
    day_number,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS sessions,
    AVG(events) AS avg_events_per_session,
    AVG(unique_pages) AS avg_pages_per_session
FROM user_sessions
GROUP BY cohort, day_number
ORDER BY cohort, day_number;
"#.to_string(),
        }
    }

    /// Generate SQL for funnel analysis with journey paths
    pub fn funnel_with_paths_sql(funnel_steps: &[&str]) -> String {
        let steps_array: Vec<String> = funnel_steps
            .iter()
            .map(|s| format!("'{}'", s))
            .collect();

        format!(
            r#"
-- Funnel analysis with user journey paths
WITH funnel_steps AS (
    SELECT
        user_id,
        session_id,
        type AS event_type,
        url,
        ts,
        CASE
            {}
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
    '{}' AS step_name,
    users_reached_step,
    prev_step_users,
    CASE
        WHEN prev_step_users > 0 THEN ROUND(100.0 * users_reached_step / prev_step_users, 2)
        ELSE 100.0
    END AS conversion_rate_pct
FROM step_counts
ORDER BY funnel_step;
"#,
            (1..=funnel_steps.len())
                .map(|i| format!("WHEN funnel_step = {} THEN {}", i, i))
                .collect::<Vec<_>>()
                .join("\n            "),
            funnel_steps
                .iter()
                .enumerate()
                .map(|(i, s)| format!("CASE WHEN funnel_step = {} THEN '{}' END", i + 1, s))
                .collect::<Vec<_>>()
                .join(",\n            ")
        )
    }

    /// Generate SQL for drop-off analysis
    pub fn drop_off_analysis_sql() -> String {
        r#"
-- Analyze where users drop off in their journey
WITH session_sequences AS (
    SELECT
        session_id,
        user_id,
        ARRAY_AGG(type ORDER BY ts) AS event_sequence,
        ARRAY_AGG(url ORDER BY ts) AS url_sequence,
        COUNT(*) AS total_events
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND session_id IS NOT NULL
    GROUP BY session_id, user_id
),
last_events AS (
    SELECT
        session_id,
        user_id,
        event_sequence,
        url_sequence,
        total_events,
        event_sequence[ARRAY_LENGTH(event_sequence, 1)] AS last_event_type,
        url_sequence[ARRAY_LENGTH(url_sequence, 1)] AS last_url,
        url_sequence[ARRAY_LENGTH(url_sequence, 1) - 1] AS second_to_last_url
    FROM session_sequences
    WHERE ARRAY_LENGTH(event_sequence, 1) > 0
)
SELECT
    last_event_type,
    last_url,
    second_to_last_url,
    COUNT(*) AS sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(total_events) AS avg_events_before_dropoff
FROM last_events
GROUP BY last_event_type, last_url, second_to_last_url
ORDER BY sessions DESC
LIMIT 50;
"#.to_string()
    }

    /// Generate SQL for returning user analysis
    pub fn returning_user_sql() -> String {
        r#"
-- Analyze returning user behavior
WITH user_sessions_summary AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) AS total_sessions,
        MIN(ts) AS first_session,
        MAX(ts) AS last_session,
        COUNT(*) AS total_events,
        COUNT(DISTINCT url) AS total_unique_pages,
        COUNT(DISTINCT DATE_TRUNC('day', ts)) AS active_days
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
    GROUP BY user_id
),
session_gaps AS (
    SELECT
        user_id,
        EXTRACT(DAY FROM (last_session - first_session)) AS days_active_range,
        CASE
            WHEN COUNT(DISTINCT session_id) = 1 THEN 'new'
            WHEN COUNT(DISTINCT session_id) = 2 THEN 'returning_2'
            WHEN COUNT(DISTINCT session_id) BETWEEN 3 AND 5 THEN 'returning_3_5'
            WHEN COUNT(DISTINCT session_id) BETWEEN 6 AND 10 THEN 'returning_6_10'
            ELSE 'returning_11_plus'
        END AS user_segment
    FROM read_parquet('s3://{{s3_path}}/events/**/*.parquet')
    WHERE ts >= '{{start_date}}'::TIMESTAMP
        AND ts < '{{end_date}}'::TIMESTAMP
        AND user_id IS NOT NULL
    GROUP BY user_id
)
SELECT
    s.user_segment,
    COUNT(*) AS user_count,
    SUM(s.total_sessions) AS total_sessions,
    SUM(s.total_events) AS total_events,
    AVG(s.total_sessions) AS avg_sessions_per_user,
    AVG(s.active_days) AS avg_active_days,
    AVG(s.total_unique_pages) AS avg_unique_pages
FROM user_sessions_summary s
INNER JOIN session_gaps g ON s.user_id = g.user_id
GROUP BY s.user_segment
ORDER BY
    CASE s.user_segment
        WHEN 'new' THEN 1
        WHEN 'returning_2' THEN 2
        WHEN 'returning_3_5' THEN 3
        WHEN 'returning_6_10' THEN 4
        ELSE 5
    END;
"#.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert_eq!(config.session_timeout_minutes, 30);
        assert_eq!(config.max_session_hours, 4);
    }

    #[test]
    fn test_session_reconstruction_sql() {
        let config = SessionConfig::default();
        let sql = SessionStitcher::session_reconstruction_sql(&config);

        assert!(sql.contains("session_timeout_minutes"));
        assert!(sql.contains("reconstructed_session"));
        assert!(sql.contains("gap_minutes"));
    }

    #[test]
    fn test_user_journey_sql() {
        let config = SessionConfig::default();
        let sql = SessionStitcher::user_journey_sql("user-123", &config);

        assert!(sql.contains("user_id = 'user-123'"));
        assert!(sql.contains("session_seq"));
    }

    #[test]
    fn test_attribution_sql() {
        let sql = SessionStitcher::attribution_sql(None);
        assert!(sql.contains("multi-touch attribution"));
        assert!(sql.contains("attribution_model"));

        let sql_conversion = SessionStitcher::attribution_sql(Some("purchase"));
        assert!(sql_conversion.contains("type = 'purchase'"));
    }

    #[test]
    fn test_common_paths_sql() {
        let sql = SessionStitcher::common_paths_sql();
        assert!(sql.contains("ARRAY_AGG"));
        assert!(sql.contains("path_frequencies"));
    }

    #[test]
    fn test_session_flow_matrix_sql() {
        let sql = SessionStitcher::session_flow_matrix_sql();
        assert!(sql.contains("LEAD(url)"));
        assert!(sql.contains("transition_matrix"));
    }

    #[test]
    fn test_cohort_journey_sql() {
        let sql = SessionStitcher::cohort_journey_sql("acquisition");
        assert!(sql.contains("acquisition_network"));

        let sql = SessionStitcher::cohort_journey_sql("campaign");
        assert!(sql.contains("campaign"));
    }

    #[test]
    fn test_funnel_with_paths_sql() {
        let steps = vec!["pageview", "signup", "purchase"];
        let sql = SessionStitcher::funnel_with_paths_sql(&steps);

        assert!(sql.contains("funnel_step"));
        assert!(sql.contains("conversion_rate_pct"));
    }

    #[test]
    fn test_drop_off_analysis_sql() {
        let sql = SessionStitcher::drop_off_analysis_sql();
        assert!(sql.contains("last_event_type"));
        assert!(sql.contains("drop_off"));
    }

    #[test]
    fn test_returning_user_sql() {
        let sql = SessionStitcher::returning_user_sql();
        assert!(sql.contains("user_segment"));
        assert!(sql.contains("returning"));
    }
}
