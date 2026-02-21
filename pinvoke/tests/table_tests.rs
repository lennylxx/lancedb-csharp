//! Tests for table FFI functions.
//! Verifies that borrow operations do not consume the Arc pointer.

mod common;

use lancedb_ffi::*;
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

#[test]
fn test_count_rows_empty_table() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "count_test");

    let count = common::count_rows_sync(table_ptr, None);
    assert_eq!(count, 0);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_schema_returns_valid_ipc() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "schema_test");

    let ipc_bytes = common::schema_ipc_sync(table_ptr);
    assert!(!ipc_bytes.is_empty());

    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = arrow_ipc::reader::FileReader::try_new(cursor, None).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(*schema.field(0).data_type(), arrow_schema::DataType::Int32);
    assert!(!schema.field(0).is_nullable());

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_free_ffi_bytes_null_is_safe() {
    free_ffi_bytes(ptr::null_mut());
}

#[test]
fn test_add_data_and_count_rows() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "add_test");

    assert_eq!(common::count_rows_sync(table_ptr, None), 0);

    let ipc_bytes = create_test_ipc_data(3);
    common::add_ipc_sync(table_ptr, ipc_bytes);

    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_add_data_append_mode() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "append_test");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(2));
    common::add_ipc_sync(table_ptr, create_test_ipc_data(3));

    assert_eq!(common::count_rows_sync(table_ptr, None), 5);

    table_close(table_ptr);
    database_close(conn_ptr);
}

fn create_test_ipc_data(num_rows: usize) -> Vec<u8> {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap();
    lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap()
}

#[test]
fn test_version_increments_on_add() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "version_test");

    let v1 = common::version_sync(table_ptr);
    common::add_ipc_sync(table_ptr, create_test_ipc_data(3));
    let v2 = common::version_sync(table_ptr);

    assert!(v2 > v1);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_checkout_and_restore() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "checkout_test");

    let v1 = common::version_sync(table_ptr);
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));
    let v2 = common::version_sync(table_ptr);
    assert!(v2 > v1);

    common::checkout_sync(table_ptr, v1);
    assert_eq!(common::count_rows_sync(table_ptr, None), 0);

    common::checkout_latest_sync(table_ptr);
    assert_eq!(common::count_rows_sync(table_ptr, None), 5);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_create_btree_index_and_list_indices() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "index_test");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(100));
    common::create_btree_index_sync(table_ptr, "id");

    let indices = common::list_indices_sync(table_ptr);
    assert!(!indices.is_empty());
    assert!(indices.iter().any(|i| i.columns.contains(&"id".to_string())));

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_list_indices_empty_table() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "no_index");

    let indices = common::list_indices_sync(table_ptr);
    assert!(indices.is_empty());

    table_close(table_ptr);
    database_close(conn_ptr);
}
