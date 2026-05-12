//! Tests for database connection FFI functions.

mod common;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lancedb_ffi::*;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_connection_connect_with_session_returns_handle() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let uri_cstr = std::ffi::CString::new(tmp.path().to_str().unwrap()).unwrap();

    connection_connect(
        uri_cstr.as_ptr(),
        f64::NAN,
        std::ptr::null(),
        512 * 1024 * 1024, // index_cache_size_bytes
        128 * 1024 * 1024, // metadata_cache_size_bytes
        common::ffi_callback,
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    let names = common::table_names_sync(conn_ptr);
    assert!(names.is_empty());

    connection_close(conn_ptr);
}

#[test]
fn test_connection_connect_with_session_defaults_returns_handle() {
    let ctx = common::FfiTestContext::new();
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
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    connection_close(conn_ptr);
}

#[test]
fn test_connection_connect_without_session_returns_handle() {
    let ctx = common::FfiTestContext::new();
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
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());
    let conn_ptr = result as *const lancedb::connection::Connection;

    connection_close(conn_ptr);
}

#[test]
fn test_connection_table_names_returns_joined_names() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let t1 = common::create_table_sync(conn_ptr, "alpha");
    let t2 = common::create_table_sync(conn_ptr, "beta");

    let ctx = common::FfiTestContext::new();
    connection_table_names(
        conn_ptr,
        std::ptr::null(), // start_after
        0,                // limit: 0 → no limit
        std::ptr::null(), // namespace_json
        common::ffi_callback,
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());

    let joined = unsafe { std::ffi::CStr::from_ptr(result as *const libc::c_char) }
        .to_str()
        .unwrap()
        .to_string();
    let names: Vec<&str> = joined.split('\n').collect();
    assert!(names.contains(&"alpha"), "expected 'alpha' in {:?}", names);
    assert!(names.contains(&"beta"), "expected 'beta' in {:?}", names);
    assert_eq!(names.len(), 2);

    free_string(result as *mut libc::c_char);
    table_close(t1);
    table_close(t2);
    connection_close(conn_ptr);
}

#[test]
fn test_connection_create_empty_table_null_schema_uses_minimal() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let table_name = std::ffi::CString::new("created_empty_ffi").unwrap();

    let ctx = common::FfiTestContext::new();
    connection_create_empty_table(
        conn_ptr,
        table_name.as_ptr(),
        std::ptr::null_mut(), // schema_cdata: null → minimal_schema
        std::ptr::null(),     // mode
        std::ptr::null(),     // storage_options_json
        std::ptr::null(),     // location
        std::ptr::null(),     // namespace_json
        false,                // exist_ok
        common::ffi_callback,
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());
    let table_ptr = result as *const lancedb::table::Table;

    let names = common::table_names_sync(conn_ptr);
    assert!(names.contains(&"created_empty_ffi".to_string()));

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_connection_create_table_with_data_returns_table() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();

    use arrow_array::Array;
    let struct_array: arrow_array::StructArray = batch.into();
    let data = struct_array.to_data();
    let mut ffi_array = arrow_data::ffi::FFI_ArrowArray::new(&data);
    let mut ffi_schema =
        arrow_schema::ffi::FFI_ArrowSchema::try_from(data.data_type()).unwrap();

    let table_name = std::ffi::CString::new("created_with_data_ffi").unwrap();

    let ctx = common::FfiTestContext::new();
    connection_create_table(
        conn_ptr,
        table_name.as_ptr(),
        &mut ffi_array,
        &mut ffi_schema,
        1,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        false,
        common::ffi_callback,
        ctx.user_data(),
    );

    let result = ctx.wait_success();
    assert!(!result.is_null());
    let table_ptr = result as *const lancedb::table::Table;

    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    connection_close(conn_ptr);
}
