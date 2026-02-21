use arrow_schema::Schema;
use libc::c_char;
use std::ffi::CStr;
use std::sync::Arc;

pub fn get_static_str(c_string: *const c_char) -> &'static str {
    assert!(!c_string.is_null(), "Received null pointer");
    let c_str = unsafe { CStr::from_ptr(c_string) };
    c_str.to_str().expect("Invalid UTF-8 data")
}

/// Create a minimal Arrow schema with a single "id" integer field.
/// Used for creating empty tables for testing.
pub fn minimal_schema() -> Arc<Schema> {
    use arrow_schema::{DataType, Field};
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}
