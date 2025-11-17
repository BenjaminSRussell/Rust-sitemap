//! JSON validation and serialization utilities.
//!
//! This module provides robust JSON handling to prevent:
//! - Invalid JSON syntax errors
//! - Missing required fields
//! - Type mismatches
//! - Schema violations

use serde::{Deserialize, Serialize};
use std::fmt;

/// JSON validation error with detailed context
#[derive(Debug)]
pub enum JsonError {
    /// Syntax error during parsing (line/column info)
    Syntax { msg: String, line: usize, column: usize },
    /// Data validation error (missing field, type mismatch)
    Data(String),
    /// Unexpected EOF during parsing
    UnexpectedEof(String),
    /// Serialization error
    Serialization(String),
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonError::Syntax { msg, line, column } => {
                write!(f, "JSON syntax error at line {}, column {}: {}", line, column, msg)
            }
            JsonError::Data(msg) => write!(f, "JSON validation error: {}", msg),
            JsonError::UnexpectedEof(msg) => write!(f, "Incomplete JSON: {}", msg),
            JsonError::Serialization(msg) => write!(f, "JSON serialization error: {}", msg),
        }
    }
}

impl std::error::Error for JsonError {}

impl From<serde_json::Error> for JsonError {
    fn from(e: serde_json::Error) -> Self {
        if e.is_syntax() {
            JsonError::Syntax {
                msg: e.to_string(),
                line: e.line(),
                column: e.column(),
            }
        } else if e.is_data() {
            JsonError::Data(format!("Missing required field or type mismatch: {}", e))
        } else if e.is_eof() {
            JsonError::UnexpectedEof(e.to_string())
        } else {
            JsonError::Data(e.to_string())
        }
    }
}

/// Safe JSON deserialization with detailed error reporting
pub fn safe_deserialize<'a, T>(json: &'a str) -> Result<T, JsonError>
where
    T: Deserialize<'a>,
{
    // Pre-validation: check for empty or null inputs
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Err(JsonError::Data("Empty JSON input".to_string()));
    }
    if trimmed == "null" {
        return Err(JsonError::Data("Null JSON input".to_string()));
    }

    serde_json::from_str(json).map_err(JsonError::from)
}

/// Safe JSON serialization with error handling
pub fn safe_serialize<T>(value: &T) -> Result<String, JsonError>
where
    T: Serialize,
{
    serde_json::to_string(value)
        .map_err(|e| JsonError::Serialization(e.to_string()))
}

/// Safe JSON serialization with pretty printing
pub fn safe_serialize_pretty<T>(value: &T) -> Result<String, JsonError>
where
    T: Serialize,
{
    serde_json::to_string_pretty(value)
        .map_err(|e| JsonError::Serialization(e.to_string()))
}

/// Validate that a JSON string is well-formed without deserializing
pub fn validate_json_syntax(json: &str) -> Result<(), JsonError> {
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Err(JsonError::Data("Empty JSON input".to_string()));
    }

    // Quick validation by parsing as generic Value
    serde_json::from_str::<serde_json::Value>(json)
        .map(|_| ())
        .map_err(JsonError::from)
}

/// Serialize with logging on failure (convenience wrapper for crawler code)
pub fn serialize_with_logging<T>(value: &T, context: &str) -> Option<String>
where
    T: Serialize,
{
    match safe_serialize(value) {
        Ok(json) => Some(json),
        Err(e) => {
            eprintln!("Warning: Failed to serialize {}: {}", context, e);
            None
        }
    }
}

/// Deserialize with logging on failure (convenience wrapper for crawler code)
pub fn deserialize_with_logging<'a, T>(json: &'a str, context: &str) -> Option<T>
where
    T: Deserialize<'a>,
{
    match safe_deserialize(json) {
        Ok(value) => Some(value),
        Err(e) => {
            eprintln!("Warning: Failed to deserialize {}: {}", context, e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        count: i32,
    }

    #[test]
    fn test_safe_serialize() {
        let data = TestStruct {
            name: "test".to_string(),
            count: 42,
        };
        let json = safe_serialize(&data).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("42"));
    }

    #[test]
    fn test_safe_deserialize_valid() {
        let json = r#"{"name":"test","count":42}"#;
        let data: TestStruct = safe_deserialize(json).unwrap();
        assert_eq!(data.name, "test");
        assert_eq!(data.count, 42);
    }

    #[test]
    fn test_safe_deserialize_syntax_error() {
        let json = r#"{"name":"test","count":42"#; // Missing closing brace
        let result: Result<TestStruct, JsonError> = safe_deserialize(json);
        assert!(matches!(result, Err(JsonError::UnexpectedEof(_))));
    }

    #[test]
    fn test_safe_deserialize_missing_field() {
        let json = r#"{"name":"test"}"#; // Missing 'count' field
        let result: Result<TestStruct, JsonError> = safe_deserialize(json);
        assert!(matches!(result, Err(JsonError::Data(_))));
    }

    #[test]
    fn test_safe_deserialize_type_mismatch() {
        let json = r#"{"name":"test","count":"not_a_number"}"#;
        let result: Result<TestStruct, JsonError> = safe_deserialize(json);
        assert!(matches!(result, Err(JsonError::Data(_))));
    }

    #[test]
    fn test_safe_deserialize_empty_input() {
        let result: Result<TestStruct, JsonError> = safe_deserialize("");
        assert!(matches!(result, Err(JsonError::Data(_))));
    }

    #[test]
    fn test_safe_deserialize_null_input() {
        let result: Result<TestStruct, JsonError> = safe_deserialize("null");
        assert!(matches!(result, Err(JsonError::Data(_))));
    }

    #[test]
    fn test_validate_json_syntax_valid() {
        let json = r#"{"name":"test","count":42}"#;
        assert!(validate_json_syntax(json).is_ok());
    }

    #[test]
    fn test_validate_json_syntax_invalid() {
        let json = r#"{"name":"test","count":42"#;
        assert!(validate_json_syntax(json).is_err());
    }

    #[test]
    fn test_serialize_with_logging() {
        let data = TestStruct {
            name: "test".to_string(),
            count: 42,
        };
        let json = serialize_with_logging(&data, "test struct");
        assert!(json.is_some());
        assert!(json.unwrap().contains("test"));
    }

    #[test]
    fn test_deserialize_with_logging_valid() {
        let json = r#"{"name":"test","count":42}"#;
        let data: Option<TestStruct> = deserialize_with_logging(json, "test struct");
        assert!(data.is_some());
        assert_eq!(data.unwrap().name, "test");
    }

    #[test]
    fn test_deserialize_with_logging_invalid() {
        let json = r#"{"name":"test"}"#; // Missing field
        let data: Option<TestStruct> = deserialize_with_logging(json, "test struct");
        assert!(data.is_none());
    }
}
