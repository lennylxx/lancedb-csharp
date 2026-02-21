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
