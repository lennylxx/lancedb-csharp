//! Tests for database connection FFI functions.

mod common;

use lancedb_ffi::*;
use tempfile::TempDir;

#[test]
fn test_database_connect_and_close() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();

    let ptr = common::connect_sync(uri);
    assert!(!ptr.is_null());

    database_close(ptr);
}

#[test]
fn test_table_names_empty_database() {
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let ptr = common::connect_sync(uri);

    let names = common::table_names_sync(ptr);
    assert!(names.is_empty());

    database_close(ptr);
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
    database_close(ptr);
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

    database_close(ptr);
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

    database_close(ptr);
}

#[test]
fn test_create_table_with_data() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();

    let table_ptr = common::create_table_with_data_sync(conn_ptr, "my_table", ipc_bytes);
    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    let names = common::table_names_sync(conn_ptr);
    assert_eq!(names, vec!["my_table"]);

    table_close(table_ptr);
    database_close(conn_ptr);
}
