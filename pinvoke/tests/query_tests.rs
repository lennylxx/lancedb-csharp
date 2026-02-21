//! Tests for query FFI functions.
//! Verifies that creating queries does not consume the table pointer
//! and that builder methods return new valid pointers.

mod common;

use lancedb_ffi::*;
use std::ffi::CString;
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

#[test]
fn test_query_select_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "select_test");

    let query = table_create_query(table_ptr);
    let json = CString::new(r#"["id"]"#).unwrap();
    let new_query = query_select(query, json.as_ptr());

    assert!(!new_query.is_null());
    assert_ne!(query as usize, new_query as usize);

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_only_if_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "only_if_test");

    let query = table_create_query(table_ptr);
    let predicate = CString::new("id > 0").unwrap();
    let new_query = query_only_if(query, predicate.as_ptr());

    assert!(!new_query.is_null());
    assert_ne!(query as usize, new_query as usize);

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_limit_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "limit_test");

    let query = table_create_query(table_ptr);
    let new_query = query_limit(query, 10);

    assert!(!new_query.is_null());

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_offset_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "offset_test");

    let query = table_create_query(table_ptr);
    let new_query = query_offset(query, 5);

    assert!(!new_query.is_null());

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_with_row_id_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "rowid_test");

    let query = table_create_query(table_ptr);
    let new_query = query_with_row_id(query);

    assert!(!new_query.is_null());

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_chaining_multiple_builders() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "chain_test");

    let q1 = table_create_query(table_ptr);
    let json = CString::new(r#"["id"]"#).unwrap();
    let q2 = query_select(q1, json.as_ptr());
    let pred = CString::new("id > 0").unwrap();
    let q3 = query_only_if(q2, pred.as_ptr());
    let q4 = query_limit(q3, 10);

    assert!(!q4.is_null());

    query_free(q1);
    query_free(q2);
    query_free(q3);
    query_free(q4);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_full_text_search_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "fts_test");

    let query = table_create_query(table_ptr);
    let text = CString::new("hello world").unwrap();
    let new_query = query_full_text_search(query, text.as_ptr());

    assert!(!new_query.is_null());
    assert_ne!(query as usize, new_query as usize);

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_fast_search_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "fast_search_test");

    let query = table_create_query(table_ptr);
    let new_query = query_fast_search(query);

    assert!(!new_query.is_null());
    assert_ne!(query as usize, new_query as usize);

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_postfilter_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "postfilter_test");

    let query = table_create_query(table_ptr);
    let new_query = query_postfilter(query);

    assert!(!new_query.is_null());
    assert_ne!(query as usize, new_query as usize);

    query_free(query);
    query_free(new_query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_full_text_search_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_fts_test");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let text = CString::new("search text").unwrap();
    let new_vq = vector_query_full_text_search(vq, text.as_ptr());

    assert!(!new_vq.is_null());
    assert_ne!(vq as usize, new_vq as usize);

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_fast_search_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_fast_test");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let new_vq = vector_query_fast_search(vq);

    assert!(!new_vq.is_null());

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_ef_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_ef_test");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let new_vq = vector_query_ef(vq, 128);

    assert!(!new_vq.is_null());

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_distance_range_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_dist_test");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let new_vq = vector_query_distance_range(vq, 0.0, 1.0);

    assert!(!new_vq.is_null());

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_column_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_col_test");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let col = CString::new("vector").unwrap();
    let new_vq = vector_query_column(vq, col.as_ptr());

    assert!(!new_vq.is_null());
    assert_ne!(vq as usize, new_vq as usize);

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}
