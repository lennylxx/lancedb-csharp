//! Tests for table FFI functions.
//! Verifies that borrow operations do not consume the Arc pointer.

mod common;

use lancedb_pinvoke::*;
use std::ptr;
use tempfile::TempDir;

#[test]
fn test_table_get_name_does_not_consume_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "my_table");

    let name1 = table_get_name(table_ptr);
    let name2 = table_get_name(table_ptr);
    let name3 = table_get_name(table_ptr);

    assert!(!name1.is_null());
    let name_str = unsafe { std::ffi::CStr::from_ptr(name1) }
        .to_str()
        .unwrap();
    assert_eq!(name_str, "my_table");

    free_string(name1);
    free_string(name2);
    free_string(name3);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_table_is_open() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "open_table");

    assert!(table_is_open(table_ptr));
    assert!(!table_is_open(ptr::null()));

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_table_close_null_is_safe() {
    table_close(ptr::null());
}

#[test]
fn test_free_string_null_is_safe() {
    free_string(ptr::null_mut());
}
