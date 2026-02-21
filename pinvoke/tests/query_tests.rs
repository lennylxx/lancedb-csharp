//! Tests for query FFI functions.
//! Verifies that creating queries does not consume the table pointer.

mod common;

use lancedb_ffi::*;
use std::ptr;
use tempfile::TempDir;

#[test]
fn test_table_create_query_does_not_consume_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_table");

    let query1 = table_create_query(table_ptr);
    let query2 = table_create_query(table_ptr);

    assert!(!query1.is_null());
    assert!(!query2.is_null());

    // Table name should still work after creating queries
    let name = table_get_name(table_ptr);
    let name_str = unsafe { std::ffi::CStr::from_ptr(name) }
        .to_str()
        .unwrap();
    assert_eq!(name_str, "query_table");

    free_string(name);
    query_free(query1);
    query_free(query2);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_free_null_is_safe() {
    query_free(ptr::null());
}

#[test]
fn test_vector_query_free_null_is_safe() {
    vector_query_free(ptr::null());
}
