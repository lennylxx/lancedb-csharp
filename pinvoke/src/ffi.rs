use arrow_schema::{DataType, Schema};
use libc::c_char;
use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::Arc;

/// Converts a C string pointer to an owned Rust String.
/// The caller retains ownership of the original C string.
pub fn to_string(c_string: *const c_char) -> String {
    assert!(!c_string.is_null(), "Received null pointer");
    let c_str = unsafe { CStr::from_ptr(c_string) };
    c_str.to_str().expect("Invalid UTF-8 data").to_owned()
}

/// Create a minimal Arrow schema with a single "id" integer field.
/// Used for creating empty tables for testing.
pub fn minimal_schema() -> Arc<Schema> {
    use arrow_schema::Field;
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

/// Parses a distance type string into a LanceDB DistanceType enum.
pub fn parse_distance_type(s: &str) -> Result<lancedb::DistanceType, String> {
    match s.to_lowercase().as_str() {
        "l2" => Ok(lancedb::DistanceType::L2),
        "cosine" => Ok(lancedb::DistanceType::Cosine),
        "dot" => Ok(lancedb::DistanceType::Dot),
        _ => Err(format!("Unknown distance type: {}", s)),
    }
}

/// Parses an optional JSON-encoded map from a nullable C string.
/// Returns None if the pointer is null.
pub fn parse_optional_json_map(json_ptr: *const c_char) -> Option<HashMap<String, String>> {
    if json_ptr.is_null() {
        return None;
    }
    let json_str = to_string(json_ptr);
    serde_json::from_str(&json_str).ok()
}
