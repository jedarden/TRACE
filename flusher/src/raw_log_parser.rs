//! Raw log line parser for collector events
//!
//! Parses raw collector log lines (RawRequest format) into structured events.
//! Extracts timestamp, IP, user agent, URL, query params, and determines event type.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Raw request from collector (matches collector schema)
#[derive(Debug, Deserialize, Serialize)]
pub struct RawRequest {
    /// ISO 8601 timestamp when request was received
    pub ts: String,
    /// HTTP method (GET or POST)
    pub method: String,
    /// Full request path including query string
    pub path: String,
    /// Request headers
    pub headers: RawHeaders,
    /// Raw query parameters (if GET request)
    #[serde(default)]
    pub query_params: Option<String>,
    /// Raw body (if POST request)
    pub body: Option<String>,
    /// Client IP (from X-Forwarded-For or X-Real-IP)
    #[serde(default)]
    pub client_ip: Option<String>,
}

/// Headers captured from the request
#[derive(Debug, Deserialize, Serialize)]
pub struct RawHeaders {
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub referer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub x_forwarded_for: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub x_real_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub accept_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub accept_encoding: Option<String>,
}

/// Parsed event type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventType {
    /// Page load event
    Pageview,
    /// Dwell time heartbeat
    Heartbeat,
    /// Outbound link click
    Click,
    /// Unknown event type
    Unknown,
}

impl EventType {
    /// Parse event type from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "pageview" | "load" | "pv" => EventType::Pageview,
            "dwell" | "heartbeat" => EventType::Heartbeat,
            "click" => EventType::Click,
            _ => EventType::Unknown,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Pageview => "pageview",
            EventType::Heartbeat => "heartbeat",
            EventType::Click => "click",
            EventType::Unknown => "unknown",
        }
    }
}

/// Structured event parsed from RawRequest
#[derive(Debug, Clone)]
pub struct Event {
    /// Event timestamp
    pub ts: DateTime<Utc>,
    /// Client IP address
    pub ip: Option<String>,
    /// User agent string
    pub ua: Option<String>,
    /// Full URL (including query string)
    pub url: String,
    /// Event type
    pub event_type: EventType,
    /// Query parameters as key-value map
    pub params: HashMap<String, String>,
    /// Session ID (if present)
    pub session_id: Option<String>,
    /// User ID (if present)
    pub user_id: Option<String>,
    /// Cookie ID (if present)
    pub cookie_id: Option<String>,
    /// Referer (if present)
    pub referer: Option<String>,
    /// Referrer network (if detected)
    pub referrer_network: Option<String>,
}

/// Parser for raw collector log lines
pub struct RawLogParser;

impl RawLogParser {
    /// Parse a single JSON line into an Event
    pub fn parse_line(line: &str) -> Result<Event> {
        let raw: RawRequest =
            serde_json::from_str(line).context("Failed to parse RawRequest JSON")?;

        // Parse timestamp
        let ts = DateTime::<Utc>::parse_from_rfc3339(&raw.ts)
            .context("Failed to parse timestamp")?
            .with_timezone(&Utc);

        // Extract IP (prefer client_ip, then x_forwarded_for, then x_real_ip)
        let ip = raw
            .client_ip
            .or_else(|| {
                raw.headers
                    .x_forwarded_for
                    .as_ref()
                    .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
            })
            .or_else(|| {
                raw.headers
                    .x_real_ip
                    .as_ref()
                    .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
            });

        // Extract user agent
        let ua = raw.headers.user_agent;

        // Determine event type and extract data based on method
        let (event_type, params, session_id, user_id, cookie_id) = match raw.method.as_str() {
            "POST" => Self::parse_post_request(&raw.body, &raw.path)?,
            "GET" => Self::parse_get_request(&raw.query_params, &raw.path)?,
            _ => (EventType::Unknown, HashMap::new(), None, None, None),
        };

        // Build URL from path and params
        let url = Self::build_url(&raw.path, &params);

        // Extract referer
        let referer = raw.headers.referer;

        // Detect referrer network from referer URL
        let referrer_network = referer
            .as_ref()
            .and_then(|r| Self::detect_referrer_network(r));

        Ok(Event {
            ts,
            ip,
            ua,
            url,
            event_type,
            params,
            session_id,
            user_id,
            cookie_id,
            referer,
            referrer_network,
        })
    }

    /// Parse POST request body to extract event type and params
    fn parse_post_request(
        body: &Option<String>,
        path: &str,
    ) -> Result<(EventType, HashMap<String, String>, Option<String>, Option<String>, Option<String>)> {
        let Some(body_str) = body else {
            return Ok((EventType::Unknown, HashMap::new(), None, None, None));
        };

        // Try to parse as JSON
        if let Ok(json_data) = serde_json::from_str::<serde_json::Value>(body_str) {
            // Extract event type from "type" field
            let event_type = json_data
                .get("type")
                .and_then(|v| v.as_str())
                .map(EventType::from_str)
                .unwrap_or(EventType::Unknown);

            // Extract IDs
            let session_id = json_data.get("sid").and_then(|v| v.as_str()).map(|s| s.to_string());
            let user_id = json_data.get("uid").and_then(|v| v.as_str()).map(|s| s.to_string());
            let cookie_id = json_data.get("cid").and_then(|v| v.as_str()).map(|s| s.to_string());

            // Extract all other fields as params
            let mut params = HashMap::new();
            if let Some(obj) = json_data.as_object() {
                for (key, value) in obj {
                    if key != "type" && key != "sid" && key != "uid" && key != "cid" {
                        if let Some(s) = value.as_str() {
                            params.insert(key.clone(), s.to_string());
                        } else if let Some(n) = value.as_i64() {
                            params.insert(key.clone(), n.to_string());
                        } else if let Some(f) = value.as_f64() {
                            params.insert(key.clone(), f.to_string());
                        } else if let Some(b) = value.as_bool() {
                            params.insert(key.clone(), b.to_string());
                        }
                    }
                }
            }

            return Ok((event_type, params, session_id, user_id, cookie_id));
        }

        // If not JSON, try URL-encoded form data
        Ok((
            EventType::Unknown,
            Self::parse_query_string(body_str)?,
            None,
            None,
            None,
        ))
    }

    /// Parse GET request query params to extract event type and params
    fn parse_get_request(
        query_params: &Option<String>,
        path: &str,
    ) -> Result<(EventType, HashMap<String, String>, Option<String>, Option<String>, Option<String>)> {
        let params = Self::parse_query_string(query_params.as_deref().unwrap_or(""))?;

        // Determine event type from "type" param
        let event_type = params
            .get("type")
            .map(|t| EventType::from_str(t))
            .unwrap_or(EventType::Pageview); // Default to pageview for pixel requests

        // Extract IDs
        let session_id = params.get("sid").cloned();
        let user_id = params.get("uid").cloned();
        let cookie_id = params.get("cid").cloned();

        Ok((event_type, params, session_id, user_id, cookie_id))
    }

    /// Parse URL query string into HashMap
    fn parse_query_string(query: &str) -> Result<HashMap<String, String>> {
        let mut params = HashMap::new();

        for pair in query.split('&') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };

            let decoded_key = urlencoding::decode(key).unwrap_or_else(|_| key.to_string());
            let decoded_value = urlencoding::decode(value).unwrap_or_else(|_| value.to_string());

            params.insert(decoded_key.to_string(), decoded_value.to_string());
        }

        Ok(params)
    }

    /// Build full URL from path and params
    fn build_url(path: &str, params: &HashMap<String, String>) -> String {
        if params.is_empty() {
            return path.to_string();
        }

        let query_string: Vec<String> = params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    urlencoding::encode(k),
                    urlencoding::encode(v)
                )
            })
            .collect();

        format!("{}?{}", path, query_string.join("&"))
    }

    /// Detect referrer network from referer URL
    fn detect_referrer_network(referer: &str) -> Option<String> {
        let url = url::Url::parse(referer).ok()?;
        let domain = url.domain()?;

        // Common referrer networks
        if domain.contains("google") {
            Some("google".to_string())
        } else if domain.contains("facebook") || domain.contains("fb.com") {
            Some("facebook".to_string())
        } else if domain.contains("twitter") || domain.contains("x.com") {
            Some("twitter".to_string())
        } else if domain.contains("linkedin") {
            Some("linkedin".to_string())
        } else if domain.contains("taboola") {
            Some("taboola".to_string())
        } else if domain.contains("outbrain") {
            Some("outbrain".to_string())
        } else if domain.contains("mgid") {
            Some("mgid".to_string())
        } else if domain.contains("revcontent") {
            Some("revcontent".to_string())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_post_pageview() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0",
                "referer": "https://google.com"
            },
            "query_params": null,
            "body": "{\"type\":\"pageview\",\"sid\":\"sess-123\",\"uid\":\"user-456\",\"url\":\"https://example.com\",\"title\":\"Test Page\"}",
            "client_ip": "1.2.3.4"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Pageview);
        assert_eq!(event.ip, Some("1.2.3.4".to_string()));
        assert_eq!(event.ua, Some("Mozilla/5.0".to_string()));
        assert_eq!(event.session_id, Some("sess-123".to_string()));
        assert_eq!(event.user_id, Some("user-456".to_string()));
        assert_eq!(event.params.get("url"), Some(&"https://example.com".to_string()));
        assert_eq!(event.referer, Some("https://google.com".to_string()));
        assert_eq!(event.referrer_network, Some("google".to_string()));
    }

    #[test]
    fn test_parse_post_dwell() {
        let json = r#"{
            "ts": "2026-05-08T14:30:30Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"dwell\",\"sid\":\"sess-123\",\"dwell\":30000,\"dwell_sec\":30}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Heartbeat);
        assert_eq!(event.session_id, Some("sess-123".to_string()));
        assert_eq!(event.params.get("dwell"), Some(&"30000".to_string()));
        assert_eq!(event.params.get("dwell_sec"), Some(&"30".to_string()));
    }

    #[test]
    fn test_parse_post_click() {
        let json = r#"{
            "ts": "2026-05-08T14:31:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"click\",\"sid\":\"sess-123\",\"link_url\":\"https://example.com\",\"outbound\":true}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Click);
        assert_eq!(event.params.get("link_url"), Some(&"https://example.com".to_string()));
        assert_eq!(event.params.get("outbound"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_get_pixel() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "GET",
            "path": "/p",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "query_params": "url=https%3A%2F%2Fexample.com&type=pageview&sid=sess-789",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Pageview);
        assert_eq!(event.session_id, Some("sess-789".to_string()));
        assert_eq!(
            event.params.get("url"),
            Some(&"https://example.com".to_string())
        );
    }

    #[test]
    fn test_parse_with_x_forwarded_for() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0",
                "x_forwarded_for": "10.0.0.1, 1.2.3.4"
            },
            "body": "{\"type\":\"pageview\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        // Should use first IP from x_forwarded_for
        assert_eq!(event.ip, Some("10.0.0.1".to_string()));
    }

    #[test]
    fn test_parse_with_referer_taboola() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "referer": "https://taboola.com/example"
            },
            "body": "{\"type\":\"pageview\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.referrer_network, Some("taboola".to_string()));
    }

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(EventType::from_str("pageview"), EventType::Pageview);
        assert_eq!(EventType::from_str("load"), EventType::Pageview);
        assert_eq!(EventType::from_str("dwell"), EventType::Heartbeat);
        assert_eq!(EventType::from_str("heartbeat"), EventType::Heartbeat);
        assert_eq!(EventType::from_str("click"), EventType::Click);
        assert_eq!(EventType::from_str("unknown"), EventType::Unknown);
    }

    #[test]
    fn test_parse_query_string() {
        let query = "utm_source=test&utm_medium=cpc&param=value";
        let params = RawLogParser::parse_query_string(query).unwrap();

        assert_eq!(params.get("utm_source"), Some(&"test".to_string()));
        assert_eq!(params.get("utm_medium"), Some(&"cpc".to_string()));
        assert_eq!(params.get("param"), Some(&"value".to_string()));
    }

    #[test]
    fn test_parse_url_encoded_query_string() {
        let query = "url=https%3A%2F%2Fexample.com&title=Test%20Page";
        let params = RawLogParser::parse_query_string(query).unwrap();

        assert_eq!(
            params.get("url"),
            Some(&"https://example.com".to_string())
        );
        assert_eq!(params.get("title"), Some(&"Test Page".to_string()));
    }

    #[test]
    fn test_build_url() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "test".to_string());
        params.insert("utm_medium".to_string(), "cpc".to_string());

        let url = RawLogParser::build_url("/p", &params);

        assert!(url.contains("/p?"));
        assert!(url.contains("utm_source=test"));
        assert!(url.contains("utm_medium=cpc"));
    }

    #[test]
    fn test_detect_referrer_networks() {
        assert_eq!(
            RawLogParser::detect_referrer_network("https://www.google.com/search?q=test"),
            Some("google".to_string())
        );
        assert_eq!(
            RawLogParser::detect_referrer_network("https://www.facebook.com/posts/123"),
            Some("facebook".to_string())
        );
        assert_eq!(
            RawLogParser::detect_referrer_network("https://twitter.com/user/status/123"),
            Some("twitter".to_string())
        );
        assert_eq!(
            RawLogParser::detect_referrer_network("https://taboola.com/example"),
            Some("taboola".to_string())
        );
        assert_eq!(
            RawLogParser::detect_referrer_network("https://unknown-site.com"),
            None
        );
    }

    #[test]
    fn test_invalid_json() {
        let json = "invalid json";
        assert!(RawLogParser::parse_line(json).is_err());
    }

    #[test]
    fn test_invalid_timestamp() {
        let json = r#"{
            "ts": "invalid-timestamp",
            "method": "POST",
            "path": "/e",
            "headers": {},
            "body": "{\"type\":\"pageview\"}"
        }"#;

        assert!(RawLogParser::parse_line(json).is_err());
    }
}
