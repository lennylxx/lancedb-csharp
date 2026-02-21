//! Tests for query FFI functions.
//! Verifies that creating queries does not consume the table pointer
//! and that builder methods return new valid pointers.

mod common;

use lancedb_ffi::*;
use lancedb_ffi::ffi;
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

#[test]
fn test_query_explain_plan_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "explain_q");
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    let query = table_create_query(table_ptr);
    query_explain_plan(query, false, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let plan = unsafe { std::ffi::CStr::from_ptr(result as *const libc::c_char) }
        .to_str()
        .unwrap();
    assert!(!plan.is_empty());

    free_string(result as *mut libc::c_char);
    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_analyze_plan_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "analyze_q");
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    let query = table_create_query(table_ptr);
    query_analyze_plan(query, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let plan = unsafe { std::ffi::CStr::from_ptr(result as *const libc::c_char) }
        .to_str()
        .unwrap();
    assert!(!plan.is_empty());

    free_string(result as *mut libc::c_char);
    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_output_schema_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "output_s");
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    let query = table_create_query(table_ptr);
    query_output_schema(query, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let ffi_bytes = result as *mut FfiBytes;
    let len = unsafe { (*ffi_bytes).len };
    assert!(len > 0);

    free_ffi_bytes(ffi_bytes);
    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_execute_with_timeout_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "exec_timeout");
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    let query = table_create_query(table_ptr);
    // timeout_ms=30000, max_batch_length=0 (default)
    query_execute(query, 30000, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    free_ffi_bytes(result as *mut FfiBytes);
    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_execute_with_max_batch_length_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "exec_batch");
    common::add_ipc_sync(table_ptr, create_test_ipc_data(10));

    let query = table_create_query(table_ptr);
    // timeout_ms=-1 (no timeout), max_batch_length=2
    query_execute(query, -1, 2, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    free_ffi_bytes(result as *mut FfiBytes);
    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_minimum_nprobes_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_min_np");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let new_vq = vector_query_minimum_nprobes(vq, 5);
    assert!(!new_vq.is_null());
    assert_ne!(vq as usize, new_vq as usize);

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_maximum_nprobes_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_max_np");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let new_vq = vector_query_maximum_nprobes(vq, 50);
    assert!(!new_vq.is_null());
    assert_ne!(vq as usize, new_vq as usize);

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_maximum_nprobes_zero_sets_none() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_max_np0");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    // 0 means None (no limit on nprobes)
    let new_vq = vector_query_maximum_nprobes(vq, 0);
    assert!(!new_vq.is_null());

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_add_query_vector_returns_new_pointer() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_add_vec");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let extra: [f32; 3] = [4.0, 5.0, 6.0];
    let new_vq = vector_query_add_query_vector(vq, extra.as_ptr(), 3);
    assert!(!new_vq.is_null());
    assert_ne!(vq as usize, new_vq as usize);

    query_free(query);
    vector_query_free(vq);
    vector_query_free(new_vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_explain_plan_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let ipc = create_vector_ipc_data(5, 3);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "vq_explain", ipc);

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    vector_query_explain_plan(vq, false, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let plan = unsafe { std::ffi::CStr::from_ptr(result as *const libc::c_char) }
        .to_str()
        .unwrap();
    assert!(!plan.is_empty());

    free_string(result as *mut libc::c_char);
    query_free(query);
    vector_query_free(vq);
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

fn create_vector_ipc_data(num_rows: usize, dim: usize) -> Vec<u8> {
    use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let values: Vec<f32> = (0..num_rows * dim).map(|i| i as f32).collect();
    let values_array = Float32Array::from(values);
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let vector_array =
        FixedSizeListArray::try_new(field, dim as i32, Arc::new(values_array), None).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
    ]));

    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(ids)), Arc::new(vector_array)],
    )
    .unwrap();
    lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap()
}

// ----- Error handling tests -----

#[test]
fn test_query_select_invalid_json_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "select_err");

    let query = table_create_query(table_ptr);
    let bad_json = CString::new("not valid json").unwrap();
    let result = query_select(query, bad_json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(err_str.contains("Invalid select JSON"), "got: {}", err_str);
    free_string(err_ptr);

    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_select_invalid_json_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_select_err");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let bad_json = CString::new("{{{bad}}}").unwrap();
    let result = vector_query_select(vq, bad_json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(err_str.contains("Invalid select JSON"), "got: {}", err_str);
    free_string(err_ptr);

    query_free(query);
    vector_query_free(vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_nearest_to_null_vector_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "nearest_null");

    let query = table_create_query(table_ptr);
    let result = query_nearest_to(query, ptr::null(), 3);

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(err_str.contains("null"), "got: {}", err_str);
    free_string(err_ptr);

    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_select_json_number_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "select_num");

    let query = table_create_query(table_ptr);
    let json = CString::new("42").unwrap();
    let result = query_select(query, json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(
        err_str.contains("must be a JSON array or object"),
        "got: {}",
        err_str
    );
    free_string(err_ptr);

    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_select_array_with_non_string_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "select_nonstr");

    let query = table_create_query(table_ptr);
    let json = CString::new(r#"["id", 123]"#).unwrap();
    let result = query_select(query, json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(
        err_str.contains("must be strings"),
        "got: {}",
        err_str
    );
    free_string(err_ptr);

    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_select_object_with_non_string_value_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "select_objval");

    let query = table_create_query(table_ptr);
    let json = CString::new(r#"{"alias": 42}"#).unwrap();
    let result = query_select(query, json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(
        err_str.contains("must be strings"),
        "got: {}",
        err_str
    );
    free_string(err_ptr);

    query_free(query);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_select_json_number_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_sel_num");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let json = CString::new("true").unwrap();
    let result = vector_query_select(vq, json.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(
        err_str.contains("must be a JSON array or object"),
        "got: {}",
        err_str
    );
    free_string(err_ptr);

    query_free(query);
    vector_query_free(vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_query_nearest_to_empty_vector_succeeds_at_build_time() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "nearest_empty");

    let query = table_create_query(table_ptr);
    let empty: [f64; 0] = [];
    let result = query_nearest_to(query, empty.as_ptr(), 0);

    // lancedb accepts empty vectors at build time (fails at execution)
    assert!(!result.is_null());

    query_free(query);
    vector_query_free(result);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_vector_query_distance_type_invalid_returns_null_and_sets_error() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "vq_dt_err");

    let query = table_create_query(table_ptr);
    let vector: [f64; 3] = [1.0, 2.0, 3.0];
    let vq = query_nearest_to(query, vector.as_ptr(), 3);

    let bad_dt = CString::new("manhattan").unwrap();
    let result = vector_query_distance_type(vq, bad_dt.as_ptr());

    assert!(result.is_null());

    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert!(err_str.contains("manhattan"), "got: {}", err_str);
    free_string(err_ptr);

    query_free(query);
    vector_query_free(vq);
    table_close(table_ptr);
    database_close(conn_ptr);
}
