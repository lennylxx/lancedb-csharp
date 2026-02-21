//! Tests for low-level FFI utility functions (string conversion, etc.).

use lancedb_pinvoke::ffi;
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
#[should_panic(expected = "Received null pointer")]
fn test_to_string_null_panics() {
    ffi::to_string(ptr::null());
}
