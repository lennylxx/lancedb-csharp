//! Tests for low-level FFI utility functions (string conversion, etc.).

use lancedb_ffi::ffi;
use std::ptr;

#[test]
fn test_to_string_returns_owned_string() {
    let c_str = std::ffi::CString::new("hello world").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "hello world");
    // c_str is still valid here — to_string must not take ownership
    assert_eq!(c_str.to_str().unwrap(), "hello world");
}

#[test]
fn test_to_string_utf8() {
    let c_str = std::ffi::CString::new("你好世界").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "你好世界");
}

#[test]
fn test_to_string_null_returns_empty() {
    assert_eq!(ffi::to_string(ptr::null()), "");
}

#[test]
fn test_parse_optional_json_map_null_returns_none() {
    assert!(ffi::parse_optional_json_map(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_json_map_valid_json() {
    let json = std::ffi::CString::new(r#"{"key1":"val1","key2":"val2"}"#).unwrap();
    let result = ffi::parse_optional_json_map(json.as_ptr()).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result.get("key1").unwrap(), "val1");
    assert_eq!(result.get("key2").unwrap(), "val2");
}

#[test]
fn test_parse_optional_json_map_empty_object() {
    let json = std::ffi::CString::new("{}").unwrap();
    let result = ffi::parse_optional_json_map(json.as_ptr()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_parse_optional_json_list_null_returns_none() {
    assert!(ffi::parse_optional_json_list(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_json_list_valid_json() {
    let json = std::ffi::CString::new(r#"["team","project"]"#).unwrap();
    let result = ffi::parse_optional_json_list(json.as_ptr()).unwrap();
    assert_eq!(result, vec!["team", "project"]);
}

#[test]
fn test_parse_optional_json_list_empty_array() {
    let json = std::ffi::CString::new("[]").unwrap();
    let result = ffi::parse_optional_json_list(json.as_ptr()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_parse_optional_string_null_returns_none() {
    assert!(ffi::parse_optional_string(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_string_valid() {
    let c_str = std::ffi::CString::new("s3://my-bucket/tables/foo").unwrap();
    let result = ffi::parse_optional_string(c_str.as_ptr()).unwrap();
    assert_eq!(result, "s3://my-bucket/tables/foo");
}

#[test]
fn test_ipc_to_schema_null_returns_error() {
    let result = ffi::ipc_to_schema(ptr::null(), 0);
    assert!(result.is_err());
}

#[test]
fn test_ipc_to_schema_valid_ipc() {
    use arrow_schema::{DataType, Field, Schema};

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, false),
    ]));
    let ipc_bytes = lancedb::ipc::schema_to_ipc_file(&schema).unwrap();

    let result = ffi::ipc_to_schema(ipc_bytes.as_ptr(), ipc_bytes.len()).unwrap();
    assert_eq!(result.fields().len(), 2);
    assert_eq!(result.field(0).name(), "name");
    assert_eq!(result.field(1).name(), "age");
}

#[test]
fn test_set_last_error_and_get_returns_message() {
    ffi::set_last_error("something went wrong");
    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert_eq!(err_str, "something went wrong");
    lancedb_ffi::free_string(err_ptr);
}

#[test]
fn test_get_last_error_returns_null_when_no_error() {
    // Clear any prior error
    let _ = ffi::ffi_get_last_error();
    let err_ptr = ffi::ffi_get_last_error();
    assert!(err_ptr.is_null());
}

#[test]
fn test_get_last_error_clears_after_read() {
    ffi::set_last_error("first error");
    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    lancedb_ffi::free_string(err_ptr);

    let err_ptr2 = ffi::ffi_get_last_error();
    assert!(err_ptr2.is_null());
}
