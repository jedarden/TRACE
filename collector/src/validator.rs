//! Event schema validation and normalization
//!
//! Validates incoming events against a defined schema and sanitizes inputs
//! to prevent injection attacks and ensure data quality.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Maximum payload size in bytes (1MB)
const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Maximum string field length (prevents abuse)
const MAX_STRING_LENGTH: usize = 4096;

/// Valid event types
const VALID_EVENT_TYPES: &[&str] = &["pageview", "click", "scroll", "dwell"];

/// Validation error with detailed context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Field that failed validation
    pub field: String,
    /// Human-readable error message
    pub message: String,
    /// The invalid value (truncated if too large)
    pub value: Option<String>,
}

impl ValidationError {
    fn new(field: &str, message: &str) -> Self {
        Self {
            field: field.to_string(),
            message: message.to_string(),
            value: None,
        }
    }

    fn with_value(field: &str, message: &str, value: &str) -> Self {
        Self {
            field: field.to_string(),
            message: message.to_string(),
            value: Some(truncate_string(value, 256)),
        }
    }
}

/// Validation result - list of errors or success
pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// Validated and normalized event data
#[derive(Debug, Clone)]
pub struct ValidatedEvent {
    /// Normalized event type
    pub event_type: String,
    /// Sanitized URL
    pub url: String,
    /// Sanitized and filtered query parameters
    pub params: HashMap<String, String>,
    /// Validated session ID (if present)
    pub session_id: Option<String>,
    /// Validated user ID (if present)
    pub user_id: Option<String>,
    /// All extra fields from payload
    pub extra: HashMap<String, serde_json::Value>,
}

/// Event schema validator
pub struct EventValidator;

impl EventValidator {
    /// Validate and normalize an event payload
    pub fn validate(
        event_type: &str,
        url: Option<&String>,
        params: &HashMap<String, String>,
        extra: &HashMap<String, serde_json::Value>,
    ) -> Result<ValidatedEvent, Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Validate event type
        let normalized_type = Self::validate_event_type(event_type, &mut errors);

        // Validate and normalize URL
        let normalized_url = Self::validate_url(url, &mut errors);

        // Sanitize parameters
        let sanitized_params = Self::sanitize_params(params);

        // Validate session/user IDs
        let session_id = Self::validate_session_id(extra, &mut errors);
        let user_id = Self::validate_user_id(extra, &mut errors);

        // Validate payload size
        Self::validate_payload_size(extra, &mut errors);

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(ValidatedEvent {
            event_type: normalized_type,
            url: normalized_url,
            params: sanitized_params,
            session_id,
            user_id,
            extra: extra.clone(),
        })
    }

    /// Validate event type against allowed values
    fn validate_event_type(event_type: &str, errors: &mut Vec<ValidationError>) -> String {
        let normalized = event_type.to_lowercase().trim().to_string();

        if VALID_EVENT_TYPES.contains(&normalized.as_str()) {
            normalized
        } else {
            errors.push(ValidationError::with_value(
                "type",
                &format!("Invalid event type. Must be one of: {}", VALID_EVENT_TYPES.join(", ")),
                event_type,
            ));
            // Default to pageview for unknown types
            "pageview".to_string()
        }
    }

    /// Validate and normalize URL
    fn validate_url(url: Option<&String>, errors: &mut Vec<ValidationError>) -> String {
        match url {
            Some(u) if !u.trim().is_empty() => {
                // Check length
                if u.len() > MAX_STRING_LENGTH {
                    errors.push(ValidationError::new(
                        "url",
                        &format!("URL exceeds maximum length of {} characters", MAX_STRING_LENGTH),
                    ));
                    return "unknown".to_string();
                }

                // Basic URL format check
                if u.starts_with("http://") || u.starts_with("https://") {
                    u.clone()
                } else if u.starts_with("//") {
                    format!("https:{}", u)
                } else if !u.contains('/') {
                    // Likely just a domain
                    format!("https://{}", u)
                } else {
                    // Accept as-is but log warning
                    u.clone()
                }
            }
            _ => "unknown".to_string(),
        }
    }

    /// Sanitize query parameters (remove dangerous keys, truncate values)
    pub fn sanitize_params(params: &HashMap<String, String>) -> HashMap<String, String> {
        params
            .iter()
            .filter(|(k, _)| Self::is_safe_param_key(k))
            .map(|(k, v)| (k.clone(), sanitize_string(v)))
            .collect()
    }

    /// Check if a parameter key is safe (not a system/dangerous key)
    fn is_safe_param_key(key: &str) -> bool {
        let key_lower = key.to_lowercase();

        // Block potentially dangerous keys
        let blocked = [
            "password", "passwd", "secret", "token", "api_key", "apikey",
            "access_token", "auth", "cookie", "session", "csrf",
        ];

        !blocked.iter().any(|b| key_lower.contains(b))
    }

    /// Validate session ID format (UUID or alphanumeric)
    fn validate_session_id(
        extra: &HashMap<String, serde_json::Value>,
        errors: &mut Vec<ValidationError>,
    ) -> Option<String> {
        Self::extract_and_validate_id(extra, "session_id", errors)
            .or_else(|| Self::extract_and_validate_id(extra, "trace_session", errors))
    }

    /// Validate user ID format
    fn validate_user_id(
        extra: &HashMap<String, serde_json::Value>,
        errors: &mut Vec<ValidationError>,
    ) -> Option<String> {
        Self::extract_and_validate_id(extra, "user_id", errors)
    }

    /// Extract and validate an ID field from extra data
    fn extract_and_validate_id(
        extra: &HashMap<String, serde_json::Value>,
        key: &str,
        errors: &mut Vec<ValidationError>,
    ) -> Option<String> {
        extra.get(key).and_then(|v| {
            if let serde_json::Value::String(s) = v {
                if !s.is_empty() && s.len() <= 256 {
                    // Check for reasonable ID format (alphanumeric, hyphens, underscores)
                    if s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
                        Some(s.clone())
                    } else {
                        errors.push(ValidationError::with_value(
                            key,
                            "ID contains invalid characters (only alphanumeric, hyphen, underscore allowed)",
                            s,
                        ));
                        None
                    }
                } else if s.len() > 256 {
                    errors.push(ValidationError::new(
                        key,
                        "ID exceeds maximum length of 256 characters",
                    ));
                    None
                } else {
                    None
                }
            } else {
                None
            }
        })
    }

    /// Validate total payload size
    fn validate_payload_size(extra: &HashMap<String, serde_json::Value>, errors: &mut Vec<ValidationError>) {
        // Estimate payload size from extra fields
        let size: usize = extra
            .iter()
            .map(|(k, v)| k.len() + v.to_string().len())
            .sum();

        if size > MAX_PAYLOAD_SIZE {
            errors.push(ValidationError::new(
                "payload",
                &format!("Payload size exceeds maximum of {} bytes", MAX_PAYLOAD_SIZE),
            ));
        }
    }
}

/// Sanitize a string value (truncate, remove null bytes)
fn sanitize_string(s: &str) -> String {
    // Remove null bytes and control characters except tab/newline
    let sanitized: String = s
        .chars()
        .filter(|c| *c != '\0' && (*c >= ' ' || *c == '\t' || *c == '\n' || *c == '\r'))
        .collect();

    truncate_string(&sanitized, MAX_STRING_LENGTH)
}

/// Truncate string to max length
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        s.chars().take(max_len).collect()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_event_type_valid() {
        let result = EventValidator::validate_event_type("pageview", &mut Vec::new());
        assert_eq!(result, "pageview");
    }

    #[test]
    fn test_validate_event_type_case_insensitive() {
        let result = EventValidator::validate_event_type("PAGEVIEW", &mut Vec::new());
        assert_eq!(result, "pageview");
    }

    #[test]
    fn test_validate_event_type_invalid() {
        let mut errors = Vec::new();
        let result = EventValidator::validate_event_type("invalid_type", &mut errors);
        assert_eq!(result, "pageview"); // defaults to pageview
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "type");
    }

    #[test]
    fn test_validate_url_valid_https() {
        let url = Some(&"https://example.com/path".to_string());
        let result = EventValidator::validate_url(url, &mut Vec::new());
        assert_eq!(result, "https://example.com/path");
    }

    #[test]
    fn test_validate_url_adds_https() {
        let url = Some(&"example.com/path".to_string());
        let result = EventValidator::validate_url(url, &mut Vec::new());
        assert_eq!(result, "https://example.com/path");
    }

    #[test]
    fn test_validate_url_too_long() {
        let long_url = "https://example.com/".to_string() + &"a".repeat(MAX_STRING_LENGTH + 100);
        let url = Some(&long_url);
        let mut errors = Vec::new();
        let result = EventValidator::validate_url(url, &mut errors);
        assert_eq!(result, "unknown");
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_sanitize_params_blocks_dangerous_keys() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("password".to_string(), "secret123".to_string());

        let sanitized = EventValidator::sanitize_params(&params);
        assert_eq!(sanitized.len(), 1);
        assert!(sanitized.contains_key("utm_source"));
        assert!(!sanitized.contains_key("password"));
    }

    #[test]
    fn test_sanitize_string_removes_null_bytes() {
        let input = "test\x00string";
        let result = sanitize_string(input);
        assert_eq!(result, "teststring");
    }

    #[test]
    fn test_sanitize_string_truncates() {
        let input = "a".repeat(MAX_STRING_LENGTH + 100);
        let result = sanitize_string(&input);
        assert_eq!(result.len(), MAX_STRING_LENGTH);
    }

    #[test]
    fn test_validate_full_event_success() {
        let mut params = HashMap::new();
        params.insert("utm_source".to_string(), "taboola".to_string());
        params.insert("tb_image".to_string(), "img123".to_string());

        let mut extra = HashMap::new();
        extra.insert("session_id".to_string(), serde_json::Value::String("abc-123".to_string()));

        let result = EventValidator::validate(
            "pageview",
            Some(&"https://example.com".to_string()),
            &params,
            &extra,
        );

        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.event_type, "pageview");
        assert_eq!(validated.url, "https://example.com");
        assert_eq!(validated.session_id, Some("abc-123".to_string()));
    }

    #[test]
    fn test_validate_full_event_with_errors() {
        let params = HashMap::new();
        let mut extra = HashMap::new();
        extra.insert("session_id".to_string(), serde_json::Value::String("invalid@id!".to_string()));

        let result = EventValidator::validate(
            "invalid_type",
            Some(&"a".repeat(MAX_STRING_LENGTH + 100)),
            &params,
            &extra,
        );

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.len() >= 2); // type error and URL error at minimum
    }

    #[test]
    fn test_validate_id_rejects_invalid_chars() {
        let mut extra = HashMap::new();
        extra.insert("user_id".to_string(), serde_json::Value::String("user@example.com".to_string()));

        let mut errors = Vec::new();
        let result = EventValidator::validate_user_id(&extra, &mut errors);
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_all_valid_event_types() {
        for event_type in VALID_EVENT_TYPES {
            let result = EventValidator::validate_event_type(event_type, &mut Vec::new());
            assert_eq!(result, *event_type);
        }
    }
}
