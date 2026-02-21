//! Integration tests for the FFI functions.
//! These simulate calling the FFI from C# by using raw pointers directly.
//! Async FFI functions are tested by calling the underlying lancedb API
//! directly (since extern "C" callbacks can't capture state), while
//! synchronous FFI functions are called exactly as C# would.

use lancedb::connection::Connection;
use lancedb::table::Table;
use lancedb_pinvoke::*;
use std::ptr;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: connects to a local database and returns a raw Connection pointer
/// matching the FFI ownership model (caller owns the Arc).
fn connect_sync(uri: &str) -> *const Connection {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let connection = rt.block_on(async {
        lancedb::connection::connect(uri).execute().await.unwrap()
    });
    Arc::into_raw(Arc::new(connection))
}

/// Helper: creates an empty table and returns a raw Table pointer.
/// Borrows the connection pointer without consuming it.
fn create_table_sync(connection_ptr: *const Connection, name: &str) -> *const Table {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let schema = ffi::minimal_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = rt.block_on(async {
        connection.create_empty_table(name, schema).execute().await.unwrap()
    });
    Arc::into_raw(Arc::new(table))
}

#[test]
fn test_database_connect_and_close() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();

    let ptr = connect_sync(uri);
    assert!(!ptr.is_null());

    // Close should not crash
    database_close(ptr);
}

#[test]
fn test_table_get_name_does_not_consume_pointer() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let conn_ptr = connect_sync(uri);
    let table_ptr = create_table_sync(conn_ptr, "my_table");

    // Call table_get_name multiple times — should not crash
    let name1 = table_get_name(table_ptr);
    let name2 = table_get_name(table_ptr);
    let name3 = table_get_name(table_ptr);

    assert!(!name1.is_null());
    assert!(!name2.is_null());
    assert!(!name3.is_null());

    // Verify the name is correct
    let name_str = unsafe { std::ffi::CStr::from_ptr(name1) }.to_str().unwrap();
    assert_eq!(name_str, "my_table");

    // Free strings
    free_string(name1);
    free_string(name2);
    free_string(name3);

    // Cleanup
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_table_create_query_does_not_consume_pointer() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let conn_ptr = connect_sync(uri);
    let table_ptr = create_table_sync(conn_ptr, "query_table");

    // Create multiple queries — table pointer should remain valid
    let query1 = table_create_query(table_ptr);
    let query2 = table_create_query(table_ptr);

    assert!(!query1.is_null());
    assert!(!query2.is_null());

    // Table name should still work after creating queries
    let name = table_get_name(table_ptr);
    let name_str = unsafe { std::ffi::CStr::from_ptr(name) }.to_str().unwrap();
    assert_eq!(name_str, "query_table");

    // Cleanup
    free_string(name);
    query_free(query1);
    query_free(query2);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_table_is_open() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let conn_ptr = connect_sync(uri);
    let table_ptr = create_table_sync(conn_ptr, "open_table");

    assert!(table_is_open(table_ptr));
    assert!(!table_is_open(ptr::null()));

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_free_string_null_is_safe() {
    // Freeing a null string should not crash
    free_string(ptr::null_mut());
}

#[test]
fn test_table_close_null_is_safe() {
    // Closing a null table should not crash
    table_close(ptr::null());
}

#[test]
fn test_query_free_null_is_safe() {
    query_free(ptr::null());
}

#[test]
fn test_vector_query_free_null_is_safe() {
    vector_query_free(ptr::null());
}

#[test]
fn test_ffi_to_string_returns_owned_string() {
    let c_str = std::ffi::CString::new("hello world").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "hello world");
    // c_str is still valid here — to_string must not take ownership
    assert_eq!(c_str.to_str().unwrap(), "hello world");
}

#[test]
fn test_ffi_to_string_utf8() {
    let c_str = std::ffi::CString::new("你好世界").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "你好世界");
}

#[test]
#[should_panic(expected = "Received null pointer")]
fn test_ffi_to_string_null_panics() {
    ffi::to_string(ptr::null());
}
