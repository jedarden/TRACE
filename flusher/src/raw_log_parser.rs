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
    /// Scroll depth threshold reached
    Scroll,
    /// Conversion event (carries conversion_type and revenue in params)
    Conversion,
    /// Purchase event (a conversion flavor the attribution queries query
    /// alongside type = 'conversion')
    Purchase,
    /// Signup event (a conversion flavor the attribution queries query
    /// alongside type = 'conversion')
    Signup,
    /// Ad impression event (carries imp_id for dedup and viewability
    /// metrics in params)
    Impression,
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
            "scroll" => EventType::Scroll,
            "conversion" => EventType::Conversion,
            "purchase" => EventType::Purchase,
            "signup" => EventType::Signup,
            "impression" | "imp" => EventType::Impression,
            _ => EventType::Unknown,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Pageview => "pageview",
            EventType::Heartbeat => "heartbeat",
            EventType::Click => "click",
            EventType::Scroll => "scroll",
            EventType::Conversion => "conversion",
            EventType::Purchase => "purchase",
            EventType::Signup => "signup",
            EventType::Impression => "impression",
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

/// Components extracted from a request: event type, params, and the
/// session, user, and cookie IDs
type ParsedRequest = (
    EventType,
    HashMap<String, String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

impl RawLogParser {
    /// Parse a single JSON line into an Event
    pub fn parse_line(line: &str) -> Result<Event> {
        let raw: RawRequest =
            serde_json::from_str(line).context("Failed to parse RawRequest JSON")?;

        // Parse timestamp
        let ts = DateTime::parse_from_rfc3339(&raw.ts)
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

        // Determine event type and extract data based on method. The
        // conversion endpoint (/c) defaults hits to conversion events and
        // the impression endpoint (/i) to impression events so a bare pixel
        // or postback ping — where the caller often cannot add a type
        // parameter — still lands as the right type in ad_events. Bare hits
        // on the other endpoints keep their original defaults (pageview for
        // pixels, unknown for bodyless POSTs).
        let endpoint_default = Self::default_type_for_path(&raw.path);
        let (event_type, mut params, session_id, user_id, cookie_id) = match raw.method.as_str() {
            "POST" => {
                Self::parse_post_request(&raw.body, endpoint_default.unwrap_or(EventType::Unknown))?
            }
            "GET" => Self::parse_get_request(
                &raw.query_params,
                endpoint_default.unwrap_or(EventType::Pageview),
            )?,
            _ => (EventType::Unknown, HashMap::new(), None, None, None),
        };

        // A non-numeric revenue value would break the documented attribution
        // queries, which cast (params->>'revenue')::DECIMAL. Drop it rather
        // than poison the conversion sums; everything else stays raw.
        Self::sanitize_revenue(&mut params);

        // Same protection for the impression viewability metric: the
        // impression reports cast (params->>'in_view_ms')::BIGINT, and
        // view time cannot be negative.
        Self::sanitize_in_view_ms(&mut params);

        // The url column carries the request target exactly as the client
        // sent it whenever a query string arrived: rebuilding it from the
        // parsed params would drop blank keys, collapse repeated keys, and
        // re-encode every value — losing the raw URL the collector logged.
        // With no query string, POST payloads (JSON and form-encoded
        // postbacks) still surface their params in the URL so postback hits
        // stay readable; that synthesis is sorted and last-value-wins, never
        // claimed to be raw.
        let url = match raw.query_params.as_deref() {
            Some(query) => format!("{}?{}", raw.path, query),
            None => Self::build_url(&raw.path, &params),
        };

        // Extract referer. The tag's document.referrer (sent in the POST body
        // or the GET query string) is the actual traffic source and takes
        // precedence; the HTTP Referer header is the fallback for requests
        // without a tag payload (e.g. bare pixel hits). An empty payload
        // value (direct traffic / stripped referrer policy) is treated as
        // absent so it does not shadow the header.
        let referer = params
            .get("referrer")
            .filter(|r| !r.is_empty())
            .cloned()
            .or(raw.headers.referer);

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

    /// Parse POST request body to extract event type and params.
    /// `default_type` applies when the body carries no explicit event type
    /// (the conversion endpoint passes Conversion here).
    fn parse_post_request(body: &Option<String>, default_type: EventType) -> Result<ParsedRequest> {
        let Some(body_str) = body else {
            return Ok((default_type, HashMap::new(), None, None, None));
        };

        // Try to parse as JSON
        if let Ok(json_data) = serde_json::from_str::<serde_json::Value>(body_str) {
            // Extract event type from "type" field
            let event_type = json_data
                .get("type")
                .and_then(|v| v.as_str())
                .map(EventType::from_str)
                .unwrap_or(default_type);

            // Extract IDs
            let session_id = json_data
                .get("sid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let user_id = json_data
                .get("uid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let cookie_id = json_data
                .get("cid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

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

        // Not JSON: URL-encoded form data, the format ad networks use for
        // server-to-server postbacks. Parse it like a query string so the
        // type, IDs, and revenue survive.
        let params = Self::parse_query_string(body_str)?;
        let event_type = params
            .get("type")
            .map(|t| EventType::from_str(t))
            .unwrap_or(default_type);
        let session_id = params.get("sid").cloned();
        let user_id = params.get("uid").cloned();
        let cookie_id = params.get("cid").cloned();

        Ok((event_type, params, session_id, user_id, cookie_id))
    }

    /// Parse GET request query params to extract event type and params.
    /// `default_type` applies when no explicit `type` param is present
    /// (pageview for the pixel endpoints, conversion for /c).
    fn parse_get_request(
        query_params: &Option<String>,
        default_type: EventType,
    ) -> Result<ParsedRequest> {
        let params = Self::parse_query_string(query_params.as_deref().unwrap_or(""))?;

        // Determine event type from "type" param
        let event_type = params
            .get("type")
            .map(|t| EventType::from_str(t))
            .unwrap_or(default_type);

        // Extract IDs
        let session_id = params.get("sid").cloned();
        let user_id = params.get("uid").cloned();
        let cookie_id = params.get("cid").cloned();

        Ok((event_type, params, session_id, user_id, cookie_id))
    }

    /// The event type a bare hit on this path defaults to when the request
    /// carries no explicit `type`: `/c` (conversion pixel/postback) and `/i`
    /// (impression pixel/postback). Only the first path segment is compared,
    /// so `/collect` is not matched — its hits keep the ordinary
    /// pageview/unknown defaults.
    fn default_type_for_path(path: &str) -> Option<EventType> {
        let without_query = path.split('?').next().unwrap_or(path);
        let first_segment = without_query.split('/').find(|s| !s.is_empty());
        match first_segment {
            Some("c") => Some(EventType::Conversion),
            Some("i") => Some(EventType::Impression),
            _ => None,
        }
    }

    /// Drop a `revenue` param that is not a finite number. The documented
    /// attribution and ROI queries cast `(params->>'revenue')::DECIMAL`, so
    /// one malformed value would fail every query that touches the column.
    fn sanitize_revenue(params: &mut HashMap<String, String>) {
        let valid = params
            .get("revenue")
            .map(|r| r.parse::<f64>().map(|v| v.is_finite()).unwrap_or(false))
            .unwrap_or(true);
        if !valid {
            params.remove("revenue");
        }
    }

    /// Drop an `in_view_ms` param that is not a non-negative integer. The
    /// impression reports cast `(params->>'in_view_ms')::BIGINT` for average
    /// viewable time, so one malformed value would fail every one of them;
    /// a view duration in milliseconds can never be negative.
    fn sanitize_in_view_ms(params: &mut HashMap<String, String>) {
        let valid = params
            .get("in_view_ms")
            .map(|v| v.parse::<i64>().map(|n| n >= 0).unwrap_or(false))
            .unwrap_or(true);
        if !valid {
            params.remove("in_view_ms");
        }
    }

    /// Parse URL query string into HashMap
    ///
    /// Values are percent-decoded (`+` stays literal — this is percent
    /// decoding, not form decoding, matching the tag's encodeURIComponent).
    /// A pair with no `=` (a bare flag like `?installed`) maps to an empty
    /// string, same as `installed=`: the params MAP has no way to represent a
    /// valueless key, and keeping the key preserves its presence for
    /// `params->>'installed'` readers. A key repeated in the query string
    /// keeps its LAST value — MAP<STRING,STRING> storage requires unique
    /// keys, and last-wins is the deterministic collapse (see
    /// docs/notes/query-parameter-handling.md). The event URL carries the
    /// raw query string verbatim, so nothing is lost for repeated or blank
    /// keys.
    fn parse_query_string(query: &str) -> Result<HashMap<String, String>> {
        let mut params = HashMap::new();

        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }

            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));

            let decoded_key = urlencoding::decode(key).unwrap_or_else(|_| key.to_string().into());
            let decoded_value =
                urlencoding::decode(value).unwrap_or_else(|_| value.to_string().into());

            params.insert(decoded_key.to_string(), decoded_value.to_string());
        }

        Ok(params)
    }

    /// Build full URL from path and params
    ///
    /// Keys are emitted in sorted order: a HashMap iterates in arbitrary
    /// order, so without the sort the same param set would produce a
    /// different URL string on every replay of the same raw log. This
    /// builder synthesizes a URL from parsed params (POST payloads); the
    /// request-target path in parse_line preserves the raw query string
    /// instead whenever one arrived.
    fn build_url(path: &str, params: &HashMap<String, String>) -> String {
        if params.is_empty() {
            return path.to_string();
        }

        let mut keys: Vec<&String> = params.keys().collect();
        keys.sort();
        let query_string: Vec<String> = keys
            .into_iter()
            .map(|k| {
                format!(
                    "{}={}",
                    urlencoding::encode(k),
                    urlencoding::encode(params[k].as_str())
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
    use std::fs::File;

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
        assert_eq!(
            event.params.get("url"),
            Some(&"https://example.com".to_string())
        );
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
        assert_eq!(
            event.params.get("link_url"),
            Some(&"https://example.com".to_string())
        );
        assert_eq!(event.params.get("outbound"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_post_scroll() {
        let json = r#"{
            "ts": "2026-05-08T14:32:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"scroll\",\"sid\":\"sess-123\",\"url\":\"https://example.com\",\"scroll_depth\":75,\"max_scroll_depth\":78}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Scroll);
        assert_eq!(event.session_id, Some("sess-123".to_string()));
        assert_eq!(event.params.get("scroll_depth"), Some(&"75".to_string()));
        assert_eq!(
            event.params.get("max_scroll_depth"),
            Some(&"78".to_string())
        );
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

    /// The tag sends document.referrer in the POST body. For beacon POSTs the
    /// browser sets the Referer header to the page itself, so the payload
    /// value (the real traffic source) must win.
    #[test]
    fn test_parse_post_body_referrer_takes_precedence_over_header() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0",
                "referer": "https://example.com/article"
            },
            "body": "{\"type\":\"pageview\",\"sid\":\"sess-123\",\"referrer\":\"https://taboola.com/story\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.referer, Some("https://taboola.com/story".to_string()));
        assert_eq!(event.referrer_network, Some("taboola".to_string()));
    }

    /// Pixel GETs carry document.referrer as a query parameter.
    #[test]
    fn test_parse_get_query_referrer() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "GET",
            "path": "/p",
            "headers": {},
            "query_params": "url=https%3A%2F%2Fexample.com&type=pageview&referrer=https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Dtest",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(
            event.referer,
            Some("https://www.google.com/search?q=test".to_string())
        );
        assert_eq!(event.referrer_network, Some("google".to_string()));
    }

    /// An empty payload referrer (direct traffic, or a referrer policy that
    /// stripped it) must not shadow the HTTP Referer header.
    #[test]
    fn test_parse_empty_payload_referrer_falls_back_to_header() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "referer": "https://outbrain.com/example"
            },
            "body": "{\"type\":\"pageview\",\"referrer\":\"\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(
            event.referer,
            Some("https://outbrain.com/example".to_string())
        );
        assert_eq!(event.referrer_network, Some("outbrain".to_string()));
    }

    #[test]
    fn test_parse_no_referrer_anywhere() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"pageview\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.referer, None);
        assert_eq!(event.referrer_network, None);
    }

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(EventType::from_str("pageview"), EventType::Pageview);
        assert_eq!(EventType::from_str("load"), EventType::Pageview);
        assert_eq!(EventType::from_str("dwell"), EventType::Heartbeat);
        assert_eq!(EventType::from_str("heartbeat"), EventType::Heartbeat);
        assert_eq!(EventType::from_str("click"), EventType::Click);
        assert_eq!(EventType::from_str("scroll"), EventType::Scroll);
        assert_eq!(EventType::from_str("conversion"), EventType::Conversion);
        assert_eq!(EventType::from_str("purchase"), EventType::Purchase);
        assert_eq!(EventType::from_str("signup"), EventType::Signup);
        assert_eq!(EventType::from_str("impression"), EventType::Impression);
        assert_eq!(EventType::from_str("imp"), EventType::Impression);
        assert_eq!(EventType::from_str("IMPRESSION"), EventType::Impression);
        assert_eq!(EventType::from_str("unknown"), EventType::Unknown);
    }

    #[test]
    fn test_event_type_conversion_as_str() {
        assert_eq!(EventType::Conversion.as_str(), "conversion");
        assert_eq!(EventType::Purchase.as_str(), "purchase");
        assert_eq!(EventType::Signup.as_str(), "signup");
        assert_eq!(EventType::Impression.as_str(), "impression");
    }

    /// The JS tag's conversion API POSTs type=conversion with the conversion
    /// kind and revenue alongside. This is the event every documented
    /// attribution query counts on.
    #[test]
    fn test_parse_post_conversion_with_revenue() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"conversion\",\"sid\":\"sess-123\",\"conversion_type\":\"purchase\",\"revenue\":49.99,\"currency\":\"USD\"}",
            "client_ip": "1.2.3.4"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
        assert_eq!(event.session_id, Some("sess-123".to_string()));
        assert_eq!(
            event.params.get("conversion_type"),
            Some(&"purchase".to_string())
        );
        assert_eq!(event.params.get("revenue"), Some(&"49.99".to_string()));
        assert_eq!(event.params.get("currency"), Some(&"USD".to_string()));
    }

    /// A pixel GET can carry the conversion explicitly via type=conversion.
    #[test]
    fn test_parse_get_conversion_pixel() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "GET",
            "path": "/p",
            "headers": {},
            "query_params": "type=conversion&conversion_type=lead&revenue=12.50&sid=sess-789",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
        assert_eq!(event.session_id, Some("sess-789".to_string()));
        assert_eq!(
            event.params.get("conversion_type"),
            Some(&"lead".to_string())
        );
        assert_eq!(event.params.get("revenue"), Some(&"12.50".to_string()));
    }

    /// A bare hit on the conversion endpoint (no type param — the common
    /// shape for ad-network postback pixels) defaults to a conversion.
    #[test]
    fn test_parse_conversion_endpoint_defaults_to_conversion() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "GET",
            "path": "/c",
            "headers": {},
            "query_params": "sid=sess-5&uid=user-5&revenue=20&conversion_type=purchase",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
        assert_eq!(event.session_id, Some("sess-5".to_string()));
        assert_eq!(event.user_id, Some("user-5".to_string()));
        assert_eq!(event.params.get("revenue"), Some(&"20".to_string()));
    }

    /// /collect must not inherit the conversion default — its bare hits stay
    /// pageviews as before.
    #[test]
    fn test_parse_collect_endpoint_does_not_default_to_conversion() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "GET",
            "path": "/collect",
            "headers": {},
            "query_params": "url=https%3A%2F%2Fexample.com",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Pageview);
    }

    /// Form-encoded postbacks (the standard ad-network server-to-server
    /// format) keep their IDs and revenue instead of collapsing to unknown.
    #[test]
    fn test_parse_form_encoded_postback() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "POST",
            "path": "/c",
            "headers": {},
            "query_params": null,
            "body": "sid=sess-9&uid=user-9&conversion_type=purchase&revenue=33.75&utm_source=taboola&utm_campaign=c-77"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
        assert_eq!(event.session_id, Some("sess-9".to_string()));
        assert_eq!(event.user_id, Some("user-9".to_string()));
        assert_eq!(
            event.params.get("conversion_type"),
            Some(&"purchase".to_string())
        );
        assert_eq!(event.params.get("revenue"), Some(&"33.75".to_string()));
        assert_eq!(event.params.get("utm_campaign"), Some(&"c-77".to_string()));
    }

    /// A JSON postback without a type field on /c defaults to conversion.
    #[test]
    fn test_parse_json_postback_without_type_defaults_to_conversion() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "POST",
            "path": "/c",
            "headers": {},
            "query_params": null,
            "body": "{\"sid\":\"sess-11\",\"revenue\":5}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
        assert_eq!(event.params.get("revenue"), Some(&"5".to_string()));
    }

    /// An explicit type always wins over the endpoint default.
    #[test]
    fn test_parse_explicit_type_overrides_conversion_endpoint() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "GET",
            "path": "/c",
            "headers": {},
            "query_params": "type=pageview&url=https%3A%2F%2Fexample.com",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Pageview);
    }

    /// A bodyless POST to /c is still a conversion (bare postback ping).
    #[test]
    fn test_parse_bodyless_conversion_postback() {
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "POST",
            "path": "/c",
            "headers": {},
            "query_params": null,
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Conversion);
    }

    /// purchase and signup are first-class event types — the attribution
    /// analysis query filters on all three (conversion/purchase/signup).
    #[test]
    fn test_parse_purchase_and_signup_types() {
        for (type_str, expected) in [
            ("purchase", EventType::Purchase),
            ("signup", EventType::Signup),
        ] {
            let json = format!(
                r#"{{"ts":"2026-05-08T14:40:00Z","method":"POST","path":"/e","headers":{{}},"body":"{{\"type\":\"{type_str}\",\"sid\":\"s1\"}}"}}"#
            );
            let event = RawLogParser::parse_line(&json).unwrap();
            assert_eq!(event.event_type, expected);
            assert_eq!(event.event_type.as_str(), type_str);
        }
    }

    /// The JS tag's impression API POSTs type=impression with the dedup ID
    /// and viewability metrics alongside. This is the event the funnel and
    /// impression reports count (type = 'impression').
    #[test]
    fn test_parse_post_impression() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {
                "user_agent": "Mozilla/5.0"
            },
            "body": "{\"type\":\"impression\",\"sid\":\"sess-123\",\"uid\":\"user-456\",\"imp_id\":\"pv-1:creative-7\",\"creative_id\":\"creative-7\",\"ad_slot\":\"hero\",\"in_view_ms\":2400,\"utm_source\":\"taboola\",\"utm_campaign\":\"camp-1\"}",
            "client_ip": "1.2.3.4"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Impression);
        assert_eq!(event.session_id, Some("sess-123".to_string()));
        assert_eq!(event.user_id, Some("user-456".to_string()));
        assert_eq!(
            event.params.get("imp_id"),
            Some(&"pv-1:creative-7".to_string())
        );
        assert_eq!(
            event.params.get("creative_id"),
            Some(&"creative-7".to_string())
        );
        assert_eq!(event.params.get("ad_slot"), Some(&"hero".to_string()));
        assert_eq!(event.params.get("in_view_ms"), Some(&"2400".to_string()));
        assert_eq!(event.params.get("utm_source"), Some(&"taboola".to_string()));
        assert_eq!(
            event.params.get("utm_campaign"),
            Some(&"camp-1".to_string())
        );
    }

    /// A pixel GET can carry the impression explicitly via type=impression.
    #[test]
    fn test_parse_get_impression_pixel() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "GET",
            "path": "/p",
            "headers": {},
            "query_params": "type=impression&imp_id=imp-9&creative_id=creative-2&sid=sess-789",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Impression);
        assert_eq!(event.session_id, Some("sess-789".to_string()));
        assert_eq!(event.params.get("imp_id"), Some(&"imp-9".to_string()));
        assert_eq!(
            event.params.get("creative_id"),
            Some(&"creative-2".to_string())
        );
    }

    /// A bare hit on the impression endpoint (no type param — the shape of
    /// an ad-server render ping) defaults to an impression.
    #[test]
    fn test_parse_impression_endpoint_defaults_to_impression() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "GET",
            "path": "/i",
            "headers": {},
            "query_params": "imp_id=imp-5&sid=sess-5&utm_source=taboola&utm_campaign=c-77",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Impression);
        assert_eq!(event.session_id, Some("sess-5".to_string()));
        assert_eq!(event.params.get("imp_id"), Some(&"imp-5".to_string()));
        assert_eq!(event.params.get("utm_campaign"), Some(&"c-77".to_string()));
    }

    /// Form-encoded impression postbacks (an ad server reporting renders
    /// server-to-server) keep their IDs instead of collapsing to unknown.
    #[test]
    fn test_parse_form_encoded_impression_postback() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "POST",
            "path": "/i",
            "headers": {},
            "query_params": null,
            "body": "imp_id=imp-11&sid=sess-9&uid=user-9&creative_id=creative-4&utm_source=mgid&utm_campaign=c-12"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Impression);
        assert_eq!(event.session_id, Some("sess-9".to_string()));
        assert_eq!(event.user_id, Some("user-9".to_string()));
        assert_eq!(event.params.get("imp_id"), Some(&"imp-11".to_string()));
        assert_eq!(
            event.params.get("creative_id"),
            Some(&"creative-4".to_string())
        );
        assert_eq!(event.params.get("utm_source"), Some(&"mgid".to_string()));
    }

    /// A bodyless POST to /i is still an impression (bare render ping).
    #[test]
    fn test_parse_bodyless_impression_postback() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "POST",
            "path": "/i",
            "headers": {},
            "query_params": null,
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Impression);
    }

    /// An explicit type always wins over the impression endpoint default.
    #[test]
    fn test_parse_explicit_type_overrides_impression_endpoint() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "GET",
            "path": "/i",
            "headers": {},
            "query_params": "type=pageview&url=https%3A%2F%2Fexample.com",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Pageview);
    }

    /// /collect must not inherit the impression default — its bare POST
    /// hits stay unknown, like before /i existed.
    #[test]
    fn test_parse_collect_endpoint_does_not_default_to_impression() {
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "POST",
            "path": "/collect",
            "headers": {},
            "query_params": null,
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(event.event_type, EventType::Unknown);
    }

    /// Non-numeric revenue is dropped so (params->>'revenue')::DECIMAL in
    /// the attribution queries cannot fail on one malformed event; finite
    /// numeric revenue is kept verbatim.
    #[test]
    fn test_revenue_sanitization() {
        let make_line = |revenue: &str| {
            format!(
                r#"{{"ts":"2026-05-08T14:40:00Z","method":"POST","path":"/e","headers":{{}},"body":"{{\"type\":\"conversion\",\"revenue\":\"{revenue}\"}}"}}"#
            )
        };

        // Numeric revenue kept as-is
        let event = RawLogParser::parse_line(&make_line("42.50")).unwrap();
        assert_eq!(event.params.get("revenue"), Some(&"42.50".to_string()));

        // Negative and exponent forms are valid numbers
        let event = RawLogParser::parse_line(&make_line("-3.5")).unwrap();
        assert_eq!(event.params.get("revenue"), Some(&"-3.5".to_string()));

        // Garbage, infinity, and NaN are dropped
        for bad in ["abc", "inf", "nan", "12ab"] {
            let event = RawLogParser::parse_line(&make_line(bad)).unwrap();
            assert_eq!(
                event.params.get("revenue"),
                None,
                "revenue {bad:?} should have been dropped"
            );
            // The event itself survives — only the bad value goes
            assert_eq!(event.event_type, EventType::Conversion);
        }

        // No revenue at all is untouched
        let json = r#"{
            "ts": "2026-05-08T14:40:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {},
            "body": "{\"type\":\"conversion\"}"
        }"#;
        let event = RawLogParser::parse_line(json).unwrap();
        assert_eq!(event.params.get("revenue"), None);
    }

    /// Non-integer or negative in_view_ms is dropped so
    /// (params->>'in_view_ms')::BIGINT in the impression reports cannot
    /// fail on one malformed event; valid values are kept verbatim.
    #[test]
    fn test_in_view_ms_sanitization() {
        let make_line = |in_view_ms: &str| {
            format!(
                r#"{{"ts":"2026-05-08T14:45:00Z","method":"POST","path":"/e","headers":{{}},"body":"{{\"type\":\"impression\",\"imp_id\":\"imp-1\",\"in_view_ms\":\"{in_view_ms}\"}}"}}"#
            )
        };

        // Integer milliseconds kept as-is, zero included (never in view)
        let event = RawLogParser::parse_line(&make_line("2400")).unwrap();
        assert_eq!(event.params.get("in_view_ms"), Some(&"2400".to_string()));
        let event = RawLogParser::parse_line(&make_line("0")).unwrap();
        assert_eq!(event.params.get("in_view_ms"), Some(&"0".to_string()));

        // Garbage, fractional, and negative durations are dropped
        for bad in ["abc", "1.5", "-100", "12ab", ""] {
            let event = RawLogParser::parse_line(&make_line(bad)).unwrap();
            assert_eq!(
                event.params.get("in_view_ms"),
                None,
                "in_view_ms {bad:?} should have been dropped"
            );
            // The event itself survives — only the bad value goes
            assert_eq!(event.event_type, EventType::Impression);
            assert_eq!(event.params.get("imp_id"), Some(&"imp-1".to_string()));
        }

        // No in_view_ms at all is untouched (non-viewability callers)
        let json = r#"{
            "ts": "2026-05-08T14:45:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {},
            "body": "{\"type\":\"impression\",\"imp_id\":\"imp-2\"}"
        }"#;
        let event = RawLogParser::parse_line(json).unwrap();
        assert_eq!(event.params.get("in_view_ms"), None);
    }

    #[test]
    fn test_default_type_for_path() {
        // Conversion endpoint
        assert_eq!(
            RawLogParser::default_type_for_path("/c"),
            Some(EventType::Conversion)
        );
        assert_eq!(
            RawLogParser::default_type_for_path("/c?revenue=1"),
            Some(EventType::Conversion)
        );
        assert_eq!(
            RawLogParser::default_type_for_path("/c/c?revenue=1"),
            Some(EventType::Conversion)
        );
        // Impression endpoint
        assert_eq!(
            RawLogParser::default_type_for_path("/i"),
            Some(EventType::Impression)
        );
        assert_eq!(
            RawLogParser::default_type_for_path("/i?imp_id=x"),
            Some(EventType::Impression)
        );
        // /collect shares the /c prefix but is a different endpoint
        assert_eq!(RawLogParser::default_type_for_path("/collect"), None);
        assert_eq!(RawLogParser::default_type_for_path("/collect?url=x"), None);
        // Longer segments starting with i are not the impression endpoint
        assert_eq!(RawLogParser::default_type_for_path("/img"), None);
        assert_eq!(RawLogParser::default_type_for_path("/impressions"), None);
        assert_eq!(RawLogParser::default_type_for_path("/p"), None);
        assert_eq!(RawLogParser::default_type_for_path("/e"), None);
        // A /c or /i deeper in the path is not the endpoint
        assert_eq!(RawLogParser::default_type_for_path("/p/c"), None);
        assert_eq!(RawLogParser::default_type_for_path("/p/i"), None);
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

        assert_eq!(params.get("url"), Some(&"https://example.com".to_string()));
        assert_eq!(params.get("title"), Some(&"Test Page".to_string()));
    }

    /// A key with no `=` (a bare flag like `?installed`) must survive as an
    /// empty-string value, identical to `installed=` — the MAP storage has no
    /// way to distinguish the two, and keeping the key preserves its presence
    /// for `params->>'installed'` readers. Empty pairs (`&&`) must not create
    /// a key.
    #[test]
    fn test_parse_blank_params_keep_key_as_empty_string() {
        let params = RawLogParser::parse_query_string("installed&empty=&sid=s1").unwrap();

        assert_eq!(params.get("installed"), Some(&"".to_string()));
        assert_eq!(params.get("empty"), Some(&"".to_string()));
        assert_eq!(params.get("sid"), Some(&"s1".to_string()));
        assert!(!params.contains_key(""), "empty pair must not become a key");
    }

    /// A repeated key keeps its LAST value — the only deterministic choice
    /// available once MAP<STRING,STRING> storage (which requires unique keys)
    /// collapses the duplicates.
    #[test]
    fn test_parse_repeated_params_last_value_wins() {
        let params = RawLogParser::parse_query_string(
            "utm_source=taboola&utm_source=outbrain&cid=1&cid=2&cid=3",
        )
        .unwrap();

        assert_eq!(params.get("utm_source"), Some(&"outbrain".to_string()));
        assert_eq!(params.get("cid"), Some(&"3".to_string()));
    }

    /// Values are percent-decoded. `+` stays literal (this is percent
    /// decoding, not form decoding — the tag's encodeURIComponent never emits
    /// `+` for a space), and an invalid escape falls back to the raw text.
    #[test]
    fn test_parse_encoded_params_decode_percent_escapes() {
        let params =
            RawLogParser::parse_query_string("q=a%20b%2Bc&emoji=%F0%9F%8E%AF&plus=a+b&bad=%ZZ")
                .unwrap();

        assert_eq!(params.get("q"), Some(&"a b+c".to_string()));
        assert_eq!(params.get("emoji"), Some(&"\u{1F3AF}".to_string()));
        assert_eq!(params.get("plus"), Some(&"a+b".to_string()));
        assert_eq!(params.get("bad"), Some(&"%ZZ".to_string()));
    }

    /// Arbitrary / unknown parameters must flow through untouched — no
    /// allowlist, per the zero-configuration design.
    #[test]
    fn test_parse_arbitrary_params_survive() {
        let params = RawLogParser::parse_query_string(
            "tb_click_id=abc-123&gl=us&gclid=xCv9&weird_param!=%value&x=y",
        )
        .unwrap();

        assert_eq!(params.get("tb_click_id"), Some(&"abc-123".to_string()));
        assert_eq!(params.get("gclid"), Some(&"xCv9".to_string()));
        assert_eq!(params.get("weird_param!"), Some(&"%value".to_string()));
        assert_eq!(params.get("x"), Some(&"y".to_string()));
    }

    /// The event URL must be the request target exactly as the client sent
    /// it — blank keys, repeated keys, ordering, and encoding choices all
    /// preserved verbatim — rather than a re-encoding of the parsed map.
    #[test]
    fn test_parse_preserves_raw_request_target() {
        let json = r#"{
            "ts": "2026-05-08T14:30:00Z",
            "method": "GET",
            "path": "/p",
            "headers": {},
            "query_params": "utm_source=one&utm_source=two&flag&c=x%20y&",
            "body": null
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();

        assert_eq!(
            event.url,
            "/p?utm_source=one&utm_source=two&flag&c=x%20y&".to_string()
        );
    }

    /// Parsing the same request twice must produce the same URL string.
    /// The URL is built per-event, so any dependence on HashMap iteration
    /// order shows up as run-to-run (and replay-to-replay) drift.
    #[test]
    fn test_parse_url_is_deterministic_across_parses() {
        let body = "{\"type\":\"pageview\",\"sid\":\"s1\",\"url\":\"https://example.com/lp\",\"title\":\"T\",\"utm_source\":\"taboola\",\"utm_medium\":\"native\",\"tb_image\":\"i1\",\"tb_headline\":\"H\",\"z_last\":\"z\",\"a_first\":\"a\"}";
        let make_line = || {
            format!(
                r#"{{"ts":"2026-05-08T14:30:00Z","method":"POST","path":"/e","headers":{{}},"query_params":null,"body":"{}"}}"#,
                body.replace('"', "\\\"")
            )
        };

        let first = RawLogParser::parse_line(&make_line()).unwrap().url;
        for _ in 0..50 {
            let event = RawLogParser::parse_line(&make_line()).unwrap();
            assert_eq!(event.url, first, "URL changed between identical parses");
        }

        // The synthesized URL (POST body params, no query string) must also
        // emit keys in a stable, sorted order. type/sid/uid/cid become typed
        // fields and do not reappear in params.
        assert_eq!(
            first,
            "/e?a_first=a&tb_headline=H&tb_image=i1&title=T&url=https%3A%2F%2Fexample.com%2Flp&utm_medium=native&utm_source=taboola&z_last=z"
                .to_string()
        );
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

    // --- End-to-end: raw collector log line -> parser -> Parquet -> read back ---
    //
    // The Parquet converters and column readers live in main.rs; these tests
    // carry local copies of the two read helpers so they stay self-contained
    // in this module.

    /// Read a nullable UTF8 column back out of in-memory Parquet.
    fn read_string_column(parquet_data: &[u8], column: &str) -> Vec<Option<String>> {
        use arrow::array::{Array, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), parquet_data).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap())
            .unwrap()
            .build()
            .unwrap();

        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("column {} missing from Parquet schema", column));
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {} is not Utf8", column));
            for i in 0..strings.len() {
                values.push(if strings.is_null(i) {
                    None
                } else {
                    Some(strings.value(i).to_string())
                });
            }
        }
        values
    }

    /// Read the params map column back out of in-memory Parquet as one
    /// HashMap per row.
    fn read_params_column(parquet_data: &[u8], column: &str) -> Vec<HashMap<String, String>> {
        use arrow::array::{Array, MapArray, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), parquet_data).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file.path()).unwrap())
            .unwrap()
            .build()
            .unwrap();

        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let array = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("column {} missing from Parquet schema", column));
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap_or_else(|| panic!("column {} is not a Map", column));
            for i in 0..map.len() {
                let entries = map.value(i);
                let keys = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let vals = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let mut row = HashMap::new();
                for j in 0..keys.len() {
                    row.insert(keys.value(j).to_string(), vals.value(j).to_string());
                }
                rows.push(row);
            }
        }
        rows
    }

    /// Full pipeline: a pixel GET carrying arbitrary, encoded, blank, and
    /// repeated query parameters must land in Parquet with the raw request
    /// target byte-identical in the url column, and the params MAP holding
    /// the decoded, last-value-wins view (blank keys as empty strings).
    #[test]
    fn test_query_params_e2e_pixel_round_trip_preserves_raw_url() {
        let raw_target = "url=https%3A%2F%2Fexample.com%2Flp%3Futm_source%3Dtaboola&type=pageview&sid=sess-7&tb_click_id=abc-123&installed&empty=&utm_source=one&utm_source=two&title=Test%20Page&";
        let json = format!(
            r#"{{
                "ts": "2026-05-08T14:30:00Z",
                "method": "GET",
                "path": "/p",
                "headers": {{}},
                "query_params": "{}",
                "body": null
            }}"#,
            raw_target
        );

        let event = RawLogParser::parse_line(&json).unwrap();
        assert_eq!(event.event_type, EventType::Pageview);

        let parquet_data = crate::parsed_events_to_parquet(vec![event]).unwrap();

        // The raw URL survives the whole pipeline verbatim: ordering,
        // encoding, blank keys, repeated keys, even the trailing `&`.
        assert_eq!(
            read_string_column(&parquet_data, "url"),
            vec![Some(format!("/p?{}", raw_target))]
        );

        // The params MAP is the decoded analysis view: arbitrary keys kept,
        // percent escapes decoded, blank keys as empty strings, repeated
        // keys collapsed to the last value.
        let params = read_params_column(&parquet_data, "params");
        assert_eq!(params.len(), 1);
        assert_eq!(
            params[0].get("url").map(String::as_str),
            Some("https://example.com/lp?utm_source=taboola")
        );
        assert_eq!(
            params[0].get("title").map(String::as_str),
            Some("Test Page")
        );
        assert_eq!(
            params[0].get("tb_click_id").map(String::as_str),
            Some("abc-123")
        );
        assert_eq!(params[0].get("installed").map(String::as_str), Some(""));
        assert_eq!(params[0].get("empty").map(String::as_str), Some(""));
        assert_eq!(
            params[0].get("utm_source").map(String::as_str),
            Some("two"),
            "repeated key must deterministically keep its last value"
        );
    }

    /// A POST whose request target carries a query string keeps that query
    /// in the url column verbatim — it used to be dropped entirely, because
    /// POST urls were rebuilt from the body params alone.
    #[test]
    fn test_query_params_e2e_post_with_query_string_preserves_raw_url() {
        let json = r#"{
            "ts": "2026-05-08T14:31:00Z",
            "method": "POST",
            "path": "/e",
            "headers": {},
            "query_params": "utm_source=taboola&sid=sess-9",
            "body": "{\"type\":\"pageview\",\"uid\":\"user-9\"}"
        }"#;

        let event = RawLogParser::parse_line(json).unwrap();
        let parquet_data = crate::parsed_events_to_parquet(vec![event]).unwrap();

        assert_eq!(
            read_string_column(&parquet_data, "url"),
            vec![Some("/e?utm_source=taboola&sid=sess-9".to_string())]
        );
        // Identity fields are promoted out of the params MAP into their
        // typed columns while ordinary body params remain in the MAP.
        assert_eq!(
            read_string_column(&parquet_data, "user_id"),
            vec![Some("user-9".to_string())]
        );
    }

    /// Reprocessing the same raw log line must produce byte-identical
    /// Parquet url values every time — the rebuilt-URL path (POST payload,
    /// no query string) must not leak HashMap iteration order into the
    /// stored column.
    #[test]
    fn test_query_params_e2e_replay_is_byte_identical() {
        let line = r#"{
            "ts": "2026-05-08T14:32:00Z",
            "method": "POST",
            "path": "/c",
            "headers": {},
            "query_params": null,
            "body": "{\"type\":\"conversion\",\"conversion_type\":\"purchase\",\"revenue\":20,\"utm_source\":\"taboola\",\"utm_campaign\":\"c-77\",\"z_param\":\"z\",\"a_param\":\"a\"}"
        }"#;

        let first = read_string_column(
            &crate::parsed_events_to_parquet(vec![RawLogParser::parse_line(line).unwrap()])
                .unwrap(),
            "url",
        );

        for _ in 0..20 {
            let event = RawLogParser::parse_line(line).unwrap();
            let parquet_data = crate::parsed_events_to_parquet(vec![event]).unwrap();
            assert_eq!(read_string_column(&parquet_data, "url"), first);
        }
    }
}
