//! Tests for database connection FFI functions.

mod common;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lancedb_ffi::*;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_connection_connect_and_close() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();

    let ptr = common::connect_sync(uri);
    assert!(!ptr.is_null());

    connection_close(ptr);
}

#[test]
fn test_table_names_empty_database() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ptr = common::connect_sync(uri);

    let names = common::table_names_sync(ptr);
    assert!(names.is_empty());

    connection_close(ptr);
}

#[test]
fn test_table_names_returns_sorted_names() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ptr = common::connect_sync(uri);

    let _t1 = common::create_table_sync(ptr, "zebra");
    let _t2 = common::create_table_sync(ptr, "alpha");

    let names = common::table_names_sync(ptr);
    assert_eq!(names, vec!["alpha", "zebra"]);

    table_close(_t1);
    table_close(_t2);
    connection_close(ptr);
}

#[test]
fn test_drop_table() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ptr = common::connect_sync(uri);

    let t = common::create_table_sync(ptr, "to_drop");
    table_close(t);

    common::drop_table_sync(ptr, "to_drop");

    let names = common::table_names_sync(ptr);
    assert!(names.is_empty());

    connection_close(ptr);
}

#[test]
fn test_drop_all_tables() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ptr = common::connect_sync(uri);

    let t1 = common::create_table_sync(ptr, "a");
    let t2 = common::create_table_sync(ptr, "b");
    table_close(t1);
    table_close(t2);

    common::drop_all_tables_sync(ptr);

    let names = common::table_names_sync(ptr);
    assert!(names.is_empty());

    connection_close(ptr);
}

#[test]
fn test_create_table_with_data() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "my_table", vec![batch]);
    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    let names = common::table_names_sync(conn_ptr);
    assert_eq!(names, vec!["my_table"]);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_create_table_exist_ok_returns_existing() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let table1 = common::create_table_with_data_sync(conn_ptr, "dup_table", vec![batch.clone()]);
    let table2 = common::create_table_exist_ok_sync(conn_ptr, "dup_table", vec![batch]);

    assert_eq!(common::count_rows_sync(table2, None), 3);

    table_close(table1);
    table_close(table2);
    connection_close(conn_ptr);
}

#[test]
fn test_create_empty_table_with_custom_schema() {
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, false),
    ]));

    let table_ptr = common::create_empty_table_with_schema_sync(conn_ptr, "custom_schema", schema);

    assert_eq!(common::count_rows_sync(table_ptr, None), 0);

    let schema = common::schema_sync(table_ptr);
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(1).name(), "score");

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// -----------------------------------------------------------------------
// Session tests
// -----------------------------------------------------------------------

#[test]
fn test_connection_connect_with_session_both_cache_sizes() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();

    let ptr = common::connect_with_session_sync(
        uri,
        512 * 1024 * 1024,
        128 * 1024 * 1024,
    );
    assert!(!ptr.is_null());

    let t = common::create_table_sync(ptr, "session_test");
    let names = common::table_names_sync(ptr);
    assert_eq!(names, vec!["session_test"]);

    table_close(t);
    connection_close(ptr);
}

#[test]
fn test_connection_connect_with_session_default_sizes() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();

    // Use lance defaults: 6 GiB index, 1 GiB metadata
    let ptr = common::connect_with_session_sync(
        uri,
        6 * 1024 * 1024 * 1024,
        1024 * 1024 * 1024,
    );
    assert!(!ptr.is_null());

    let names = common::table_names_sync(ptr);
    assert!(names.is_empty());

    connection_close(ptr);
}

#[test]
fn test_connection_connect_ffi_with_session() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let uri_cstr = std::ffi::CString::new(tmp.path().to_str().unwrap()).unwrap();

    connection_connect(
        uri_cstr.as_ptr(),
        f64::NAN,
        std::ptr::null(),
        512 * 1024 * 1024, // index_cache_size_bytes
        128 * 1024 * 1024, // metadata_cache_size_bytes
        common::ffi_callback,
    );

    let result = common::ffi_wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    let names = common::table_names_sync(conn_ptr);
    assert!(names.is_empty());

    connection_close(conn_ptr);
}

#[test]
fn test_connection_connect_ffi_with_session_defaults() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let uri_cstr = std::ffi::CString::new(tmp.path().to_str().unwrap()).unwrap();

    // Both 0 → use lance defaults (session is created)
    connection_connect(
        uri_cstr.as_ptr(),
        f64::NAN,
        std::ptr::null(),
        0,  // index_cache_size_bytes: 0 → use default
        0,  // metadata_cache_size_bytes: 0 → use default
        common::ffi_callback,
    );

    let result = common::ffi_wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    connection_close(conn_ptr);
}

#[test]
fn test_connection_connect_ffi_without_session() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let uri_cstr = std::ffi::CString::new(tmp.path().to_str().unwrap()).unwrap();

    // Both -1 → no session
    connection_connect(
        uri_cstr.as_ptr(),
        f64::NAN,
        std::ptr::null(),
        -1, // index_cache_size_bytes: negative → no session
        -1, // metadata_cache_size_bytes: negative → no session
        common::ffi_callback,
    );

    let result = common::ffi_wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    connection_close(conn_ptr);
}
