//! Tests for database connection FFI functions.

mod common;

use lancedb_pinvoke::*;
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
