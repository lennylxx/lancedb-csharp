//! Tests for query FFI functions.

mod common;

use arrow_array::{Int32Array, RecordBatch, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use lancedb_ffi::*;
use std::ffi::CString;
use std::ptr;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_batch(num_rows: usize) -> RecordBatch {

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap();
    batch
}

fn create_vector_batch(num_rows: usize, dim: usize) -> RecordBatch {

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
    batch
}

#[test]
fn test_free_ffi_cdata_null_is_safe() {
    free_ffi_cdata(std::ptr::null_mut());
}

// ---------------------------------------------------------------------------
// Query FFI tests
// ---------------------------------------------------------------------------

#[test]
fn test_query_execute_with_empty_params() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_empty_params");
    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    let params = CString::new("{}").unwrap();
    query_execute(table_ptr, params.as_ptr(), -1, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    free_ffi_cdata(result as *mut FfiCData);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_query_execute_with_null_params() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_null_params");
    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    query_execute(table_ptr, ptr::null(), -1, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    free_ffi_cdata(result as *mut FfiCData);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_query_execute_with_limit_and_select() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_limit_sel");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    let params = CString::new(r#"{"select":["id"],"limit":3}"#).unwrap();
    query_execute(table_ptr, params.as_ptr(), -1, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let cdata = result as *mut FfiCData;
    let schema_ref = unsafe { &*(*cdata).schema };
    let data_type = arrow_schema::DataType::try_from(schema_ref).unwrap();
    if let arrow_schema::DataType::Struct(fields) = &data_type {
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "id");
    } else {
        panic!("Expected struct");
    }

    free_ffi_cdata(cdata);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_query_explain_plan() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_explain");
    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    let params = CString::new(r#"{"limit":5}"#).unwrap();
    query_explain_plan(table_ptr, params.as_ptr(), false, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let plan = unsafe { std::ffi::CStr::from_ptr(result as *const _) }
        .to_str()
        .unwrap();
    assert!(!plan.is_empty());

    free_string(result as *mut _);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_query_output_schema() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "query_out_schema");
    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    let params = CString::new(r#"{"select":["id"]}"#).unwrap();
    query_output_schema(table_ptr, params.as_ptr(), common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let schema_ref = unsafe { &*(result as *const arrow_schema::ffi::FFI_ArrowSchema) };
    let data_type = arrow_schema::DataType::try_from(schema_ref).unwrap();
    if let arrow_schema::DataType::Struct(fields) = &data_type {
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "id");
    } else {
        panic!("Expected struct");
    }

    free_ffi_schema(result as *mut _);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ---------------------------------------------------------------------------
// VectorQuery FFI tests
// ---------------------------------------------------------------------------

#[test]
fn test_vector_query_execute_basic() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "vs_exec", vec![create_vector_batch(20, 4)]);

    let vector: [f64; 4] = [1.0, 2.0, 3.0, 4.0];
    let params = CString::new(r#"{"limit":5}"#).unwrap();
    vector_query_execute(
        table_ptr, vector.as_ptr(), 4, params.as_ptr(), -1, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let cdata = result as *mut FfiCData;
    let schema_ref = unsafe { &*(*cdata).schema };
    let data_type = arrow_schema::DataType::try_from(schema_ref).unwrap();
    if let arrow_schema::DataType::Struct(fields) = &data_type {
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"_distance"));
    } else {
        panic!("Expected struct");
    }

    free_ffi_cdata(cdata);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_vector_query_execute_with_all_params() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "vs_allparams", vec![create_vector_batch(20, 4)]);

    let vector: [f64; 4] = [1.0, 2.0, 3.0, 4.0];
    let params = CString::new(r#"{
        "select": ["id"],
        "limit": 3,
        "distance_type": 0,
        "column": "vector",
        "with_row_id": true
    }"#).unwrap();
    vector_query_execute(
        table_ptr, vector.as_ptr(), 4, params.as_ptr(), -1, 0, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let cdata = result as *mut FfiCData;
    let schema_ref = unsafe { &*(*cdata).schema };
    let data_type = arrow_schema::DataType::try_from(schema_ref).unwrap();
    if let arrow_schema::DataType::Struct(fields) = &data_type {
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"id"), "Missing id, got: {:?}", names);
        assert!(names.contains(&"_distance"), "Missing _distance, got: {:?}", names);
        assert!(names.contains(&"_rowid"), "Missing _rowid, got: {:?}", names);
    } else {
        panic!("Expected struct");
    }

    free_ffi_cdata(cdata);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

// Note: null vector test omitted — the synchronous error callback
// would require a separate FFI result slot to avoid Mutex poisoning.

#[test]
fn test_vector_query_explain_plan() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "vs_explain", vec![create_vector_batch(20, 4)]);

    let vector: [f64; 4] = [1.0, 2.0, 3.0, 4.0];
    let params = CString::new(r#"{"limit":5}"#).unwrap();
    vector_query_explain_plan(
        table_ptr, vector.as_ptr(), 4, params.as_ptr(), true, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let plan = unsafe { std::ffi::CStr::from_ptr(result as *const _) }
        .to_str()
        .unwrap();
    assert!(!plan.is_empty());

    free_string(result as *mut _);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_vector_query_output_schema() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "vs_outschema", vec![create_vector_batch(20, 4)]);

    let vector: [f64; 4] = [1.0, 2.0, 3.0, 4.0];
    let params = CString::new(r#"{"select":["id"],"limit":5}"#).unwrap();
    vector_query_output_schema(
        table_ptr, vector.as_ptr(), 4, params.as_ptr(), common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let schema_ref = unsafe { &*(result as *const arrow_schema::ffi::FFI_ArrowSchema) };
    let data_type = arrow_schema::DataType::try_from(schema_ref).unwrap();
    if let arrow_schema::DataType::Struct(fields) = &data_type {
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"id"), "Missing id");
        assert!(names.contains(&"_distance"), "Missing _distance");
    } else {
        panic!("Expected struct");
    }

    free_ffi_schema(result as *mut _);
    table_close(table_ptr);
    connection_close(conn_ptr);
}
