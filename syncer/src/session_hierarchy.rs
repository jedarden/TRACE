//! Session hierarchy reconstruction for journey analysis
//!
//! This module provides functionality for reconstructing the hierarchical
//! relationship between users, sessions, campaigns, and conversions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// User session hierarchy tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSessionTree {
    /// User identifier
    pub user_id: String,

    /// All sessions for this user
    pub sessions: Vec<SessionNode>,

    /// User's acquisition source (first touch)
    pub acquisition_source: Option<AcquisitionSource>,

    /// Conversions across all sessions
    pub conversions: Vec<ConversionNode>,

    /// Total engagement metrics
    pub engagement: UserEngagement,
}

/// Individual session node in the user journey
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionNode {
    /// Session identifier
    pub session_id: String,

    /// Session sequence number for this user
    pub sequence_number: i64,

    /// Session start timestamp
    pub start_ts: DateTime<Utc>,

    /// Session end timestamp
    pub end_ts: DateTime<Utc>,

    /// Entry/landing page
    pub landing_page: String,

    /// Exit page
    pub exit_page: Option<String>,

    /// Session events
    pub events: Vec<SessionEvent>,

    /// Campaign attribution for this session
    pub attribution: Option<SessionAttribution>,

    /// Whether session resulted in conversion
    pub converted: bool,

    /// Session quality metrics
    pub quality: SessionQuality,
}

/// Event within a session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvent {
    /// Event timestamp
    pub ts: DateTime<Utc>,

    /// Event type
    pub event_type: String,

    /// Page URL
    pub url: String,

    /// Event sequence within session
    pub sequence: i64,

    /// Associated campaign (if any)
    pub campaign: Option<String>,
}

/// Attribution data for a session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionAttribution {
    /// Network/source
    pub network: Option<String>,

    /// Campaign ID
    pub campaign_id: Option<String>,

    /// Campaign name
    pub campaign_name: Option<String>,

    /// Creative ID
    pub creative_id: Option<String>,

    /// Attribution touch position in user journey
    pub touch_position: i64,

    /// Days since user acquisition
    pub days_since_acquisition: i64,
}

/// User acquisition source (first touch)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquisitionSource {
    /// Network
    pub network: Option<String>,

    /// Campaign ID
    pub campaign_id: Option<String>,

    /// Creative ID
    pub creative_id: Option<String>,

    /// First touch timestamp
    pub first_touch_ts: DateTime<Utc>,
}

/// Conversion event node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionNode {
    /// Conversion timestamp
    pub ts: DateTime<Utc>,

    /// Conversion type
    pub conversion_type: String,

    /// Session where conversion occurred
    pub session_id: String,

    /// Conversion value (if available)
    pub value: Option<f64>,

    /// Attribution data
    pub attribution: ConversionAttribution,
}

/// Attribution for a conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionAttribution {
    /// Number of touchpoints before conversion
    pub total_touches: i64,

    /// Days from first touch to conversion
    pub days_to_convert: i64,

    /// Sessions before conversion
    pub sessions_before_conversion: i64,

    /// First touch attribution
    pub first_touch: Option<TouchpointData>,

    /// Last touch attribution
    pub last_touch: Option<TouchpointData>,
}

/// Touchpoint data for attribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TouchpointData {
    /// Network
    pub network: Option<String>,

    /// Campaign ID
    pub campaign_id: Option<String>,

    /// Creative ID
    pub creative_id: Option<String>,

    /// Touch timestamp
    pub ts: DateTime<Utc>,
}

/// Session quality metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionQuality {
    /// Number of events
    pub event_count: i64,

    /// Unique pages visited
    pub unique_pages: i64,

    /// Duration in seconds
    pub duration_seconds: i64,

    /// Whether session bounced (single page)
    pub is_bounce: bool,

    /// Scroll depth percentage (average)
    pub avg_scroll_depth: Option<f64>,

    /// Dwell time in milliseconds (average)
    pub avg_dwell_time_ms: Option<i64>,
}

/// User engagement aggregate metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEngagement {
    /// Total sessions
    pub total_sessions: i64,

    /// Total events
    pub total_events: i64,

    /// Total engagement seconds
    pub total_engagement_seconds: i64,

    /// Unique pages visited
    pub unique_pages: i64,

    /// First session timestamp
    pub first_session_ts: DateTime<Utc>,

    /// Last session timestamp
    pub last_session_ts: DateTime<Utc>,

    /// Active days count
    pub active_days: i64,

    /// Total conversions
    pub total_conversions: i64,
}

/// Session hierarchy builder
pub struct SessionHierarchyBuilder;

impl SessionHierarchyBuilder {
    /// Build a user session tree from raw event data
    pub fn build_user_tree(
        user_id: &str,
        events: Vec<RawSessionEvent>,
        conversions: Vec<RawConversion>,
    ) -> UserSessionTree {
        let sorted_events = {
            let mut events = events;
            events.sort_by(|a, b| a.ts.cmp(&b.ts));
            events
        };

        // Group events into sessions
        let sessions = Self::group_into_sessions(user_id, sorted_events);

        // Find acquisition source
        let acquisition_source = Self::find_acquisition_source(&sessions);

        // Build conversion nodes
        let conversion_nodes: Vec<ConversionNode> = conversions
            .into_iter()
            .map(|c| Self::build_conversion_node(c, &sessions))
            .collect();

        // Calculate engagement metrics
        let engagement = Self::calculate_engagement(&sessions, &conversion_nodes);

        UserSessionTree {
            user_id: user_id.to_string(),
            sessions,
            acquisition_source,
            conversions: conversion_nodes,
            engagement,
        }
    }

    /// Group events into sessions using gap-based sessionization
    fn group_into_sessions(user_id: &str, events: Vec<RawSessionEvent>) -> Vec<SessionNode> {
        let mut sessions = Vec::new();
        let mut current_session_events: Vec<RawSessionEvent> = Vec::new();
        let mut session_seq = 0;
        let mut last_ts: Option<DateTime<Utc>> = None;

        for event in events {
            let should_new_session = if let Some(last) = last_ts {
                let gap = event.ts.signed_duration_since(last).num_minutes();
                gap > 30 // 30 minute timeout
            } else {
                false
            };

            if should_new_session && !current_session_events.is_empty() {
                sessions.push(Self::build_session_node(
                    user_id,
                    session_seq,
                    std::mem::take(&mut current_session_events),
                ));
                session_seq += 1;
            }

            last_ts = Some(event.ts);
            current_session_events.push(event);
        }

        // Don't forget the last session
        if !current_session_events.is_empty() {
            sessions.push(Self::build_session_node(
                user_id,
                session_seq,
                current_session_events,
            ));
        }

        sessions
    }

    /// Build a session node from a group of events
    fn build_session_node(
        user_id: &str,
        seq: i64,
        mut events: Vec<RawSessionEvent>,
    ) -> SessionNode {
        events.sort_by(|a, b| a.ts.cmp(&b.ts));

        let start_ts = events.first().map(|e| e.ts).unwrap();
        let end_ts = events.last().map(|e| e.ts).unwrap();
        let landing_page = events
            .first()
            .map(|e| e.url.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let exit_page = events.last().map(|e| e.url.clone());

        let session_events: Vec<SessionEvent> = events
            .iter()
            .enumerate()
            .map(|(i, e)| SessionEvent {
                ts: e.ts,
                event_type: e.event_type.clone(),
                url: e.url.clone(),
                sequence: i as i64,
                campaign: e.campaign_id.clone(),
            })
            .collect();

        let unique_pages = events
            .iter()
            .map(|e| &e.url)
            .collect::<HashSet<_>>()
            .len() as i64;

        let duration_seconds = end_ts.signed_duration_since(start_ts).num_seconds();

        let attribution = events
            .iter()
            .filter(|e| e.network.is_some() || e.campaign_id.is_some())
            .next()
            .map(|e| SessionAttribution {
                network: e.network.clone(),
                campaign_id: e.campaign_id.clone(),
                campaign_name: e.campaign_name.clone(),
                creative_id: e.creative_id.clone(),
                touch_position: seq + 1,
                days_since_acquisition: 0, // Calculated at tree level
            });

        let quality = SessionQuality {
            event_count: events.len() as i64,
            unique_pages,
            duration_seconds,
            is_bounce: events.len() == 1,
            avg_scroll_depth: None,  // Would need aggregation
            avg_dwell_time_ms: None, // Would need aggregation
        };

        // Generate session ID
        let session_id = format!("{}_{}", user_id, seq);

        SessionNode {
            session_id,
            sequence_number: seq,
            start_ts,
            end_ts,
            landing_page,
            exit_page,
            events: session_events,
            attribution,
            converted: false, // Will be set when matching conversions
            quality,
        }
    }

    /// Find the user's acquisition source (first touch)
    fn find_acquisition_source(sessions: &[SessionNode]) -> Option<AcquisitionSource> {
        sessions
            .first()
            .and_then(|s| s.attribution.as_ref())
            .map(|attr| AcquisitionSource {
                network: attr.network.clone(),
                campaign_id: attr.campaign_id.clone(),
                creative_id: attr.creative_id.clone(),
                first_touch_ts: sessions.first().map(|s| s.start_ts).unwrap(),
            })
    }

    /// Build a conversion node
    fn build_conversion_node(raw: RawConversion, sessions: &[SessionNode]) -> ConversionNode {
        let total_touches = sessions.len() as i64;
        let first_session = sessions.first();
        let days_to_convert = first_session
            .map(|s| raw.ts.signed_duration_since(s.start_ts).num_days())
            .unwrap_or(0);

        let first_touch = first_session.and_then(|s| s.attribution.as_ref()).map(|a| {
            TouchpointData {
                network: a.network.clone(),
                campaign_id: a.campaign_id.clone(),
                creative_id: a.creative_id.clone(),
                ts: sessions.first().map(|s| s.start_ts).unwrap(),
            }
        });

        let last_touch = sessions
            .iter()
            .filter(|s| {
                s.start_ts <= raw.ts && s.end_ts >= raw.ts
            })
            .last()
            .and_then(|s| s.attribution.as_ref())
            .map(|a| TouchpointData {
                network: a.network.clone(),
                campaign_id: a.campaign_id.clone(),
                creative_id: a.creative_id.clone(),
                ts: raw.ts,
            });

        ConversionNode {
            ts: raw.ts,
            conversion_type: raw.conversion_type,
            session_id: raw.session_id,
            value: raw.value,
            attribution: ConversionAttribution {
                total_touches,
                days_to_convert,
                sessions_before_conversion: sessions.len() as i64,
                first_touch,
                last_touch,
            },
        }
    }

    /// Calculate aggregate engagement metrics
    fn calculate_engagement(
        sessions: &[SessionNode],
        conversions: &[ConversionNode],
    ) -> UserEngagement {
        let total_sessions = sessions.len() as i64;
        let total_events = sessions.iter().map(|s| s.quality.event_count).sum();
        let total_engagement_seconds: i64 =
            sessions.iter().map(|s| s.quality.duration_seconds).sum();

        let unique_pages: HashSet<_> = sessions
            .iter()
            .flat_map(|s| s.events.iter().map(|e| e.url.clone()))
            .collect();

        let first_session_ts = sessions
            .first()
            .map(|s| s.start_ts)
            .unwrap_or_else(|| Utc::now());
        let last_session_ts = sessions
            .last()
            .map(|s| s.end_ts)
            .unwrap_or_else(|| Utc::now());

        let active_days: HashSet<_> = sessions
            .iter()
            .map(|s| s.start_ts.format("%Y-%m-%d").to_string())
            .collect();

        UserEngagement {
            total_sessions,
            total_events,
            total_engagement_seconds,
            unique_pages: unique_pages.len() as i64,
            first_session_ts,
            last_session_ts,
            active_days: active_days.len() as i64,
            total_conversions: conversions.len() as i64,
        }
    }
}

/// Raw event data for building session trees
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSessionEvent {
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub url: String,
    pub session_id: Option<String>,
    pub network: Option<String>,
    pub campaign_id: Option<String>,
    pub campaign_name: Option<String>,
    pub creative_id: Option<String>,
}

/// Raw conversion data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawConversion {
    pub ts: DateTime<Utc>,
    pub conversion_type: String,
    pub session_id: String,
    pub value: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_grouping() {
        let events = vec![
            RawSessionEvent {
                ts: Utc::now(),
                event_type: "pageview".to_string(),
                url: "https://example.com".to_string(),
                session_id: None,
                network: Some("taboola".to_string()),
                campaign_id: Some("camp-1".to_string()),
                campaign_name: None,
                creative_id: None,
            },
            RawSessionEvent {
                ts: Utc::now() + chrono::Duration::minutes(5),
                event_type: "click".to_string(),
                url: "https://example.com/page2".to_string(),
                session_id: None,
                network: None,
                campaign_id: None,
                campaign_name: None,
                creative_id: None,
            },
        ];

        let sessions = SessionHierarchyBuilder::group_into_sessions("user-123", events);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].sequence_number, 0);
        assert_eq!(sessions[0].quality.event_count, 2);
    }

    #[test]
    fn test_session_gap_split() {
        let now = Utc::now();
        let events = vec![
            RawSessionEvent {
                ts: now,
                event_type: "pageview".to_string(),
                url: "https://example.com".to_string(),
                session_id: None,
                network: None,
                campaign_id: None,
                campaign_name: None,
                creative_id: None,
            },
            RawSessionEvent {
                ts: now + chrono::Duration::minutes(35), // Gap > 30 min
                event_type: "pageview".to_string(),
                url: "https://example.com/page2".to_string(),
                session_id: None,
                network: None,
                campaign_id: None,
                campaign_name: None,
                creative_id: None,
            },
        ];

        let sessions = SessionHierarchyBuilder::group_into_sessions("user-456", events);
        assert_eq!(sessions.len(), 2);
    }
}
