//! Tests for table FFI functions.
//! Verifies that borrow operations do not consume the Arc pointer.

mod common;

use arrow_array::Array;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb_ffi::*;
use std::ptr;
use std::sync::Arc;
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
    connection_close(conn_ptr);
}

#[test]
fn test_table_is_open_open_table_returns_true() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "open_table");

    assert!(table_is_open(table_ptr));
    assert!(!table_is_open(ptr::null()));

    table_close(table_ptr);
    connection_close(conn_ptr);
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
fn test_free_ffi_cdata_null_is_safe() {
    free_ffi_cdata(ptr::null_mut());
}

fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
}

fn create_id_value_batch(ids: &[i32], values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn test_table_create_index_with_name_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_name_ffi");

    common::add_sync(table_ptr, vec![create_test_batch(100)]);

    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    let config_json = std::ffi::CString::new("{}").unwrap();
    let name = std::ffi::CString::new("my_named_idx").unwrap();

    table_create_index(
        table_ptr,
        columns_json.as_ptr(),
        7, // BTree
        config_json.as_ptr(),
        true,
        name.as_ptr(),
        true,
        -1,
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let indices = common::list_indices_sync(table_ptr);
    assert!(indices.iter().any(|i| i.name == "my_named_idx"));

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_create_index_train_false_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_no_train_ffi");

    common::add_sync(table_ptr, vec![create_test_batch(100)]);

    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    let config_json = std::ffi::CString::new("{}").unwrap();

    table_create_index(
        table_ptr,
        columns_json.as_ptr(),
        7, // BTree
        config_json.as_ptr(),
        true,
        ptr::null(),
        false,
        -1,
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let indices = common::list_indices_sync(table_ptr);
    assert!(!indices.is_empty());

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_optimize_with_params_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "opt_params_ffi");

    common::add_sync(table_ptr, vec![create_test_batch(5)]);
    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    // cleanup_older_than_ms = 0 (prune immediately), delete_unverified = true
    table_optimize(table_ptr, 0, true, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    // The result is a JSON string pointer; free it
    free_string(result as *mut libc::c_char);

    assert_eq!(common::count_rows_sync(table_ptr, None), 10);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_optimize_default_params_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "opt_default_ffi");

    common::add_sync(table_ptr, vec![create_test_batch(5)]);

    // cleanup_older_than_ms = -1 (default), delete_unverified = false
    table_optimize(table_ptr, -1, false, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());
    free_string(result as *mut libc::c_char);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_count_rows_returns_count() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr =
        common::create_table_with_data_sync(conn_ptr, "count_ffi", vec![create_test_batch(7)]);

    let ctx = common::FfiTestContext::new();
    table_count_rows(
        table_ptr,
        ptr::null(), // no filter
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert_eq!(result as usize, 7);

    // Filtered call: id < 3 should match 3 rows (0, 1, 2).
    let filter = std::ffi::CString::new("id < 3").unwrap();
    let ctx2 = common::FfiTestContext::new();
    table_count_rows(
        table_ptr,
        filter.as_ptr(),
        common::ffi_callback,
        ctx2.user_data(),
    );
    let result2 = ctx2.wait_success();
    assert_eq!(result2 as usize, 3);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_add_appends_batch() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr =
        common::create_table_with_data_sync(conn_ptr, "add_ffi", vec![create_test_batch(2)]);

    let (mut ffi_array, mut ffi_schema) = batch_to_cdata(&create_test_batch(5));

    let ctx = common::FfiTestContext::new();
    table_add(
        table_ptr,
        &mut ffi_array,
        &mut ffi_schema,
        1,           // batch_count
        ptr::null(), // mode: null → append
        common::ffi_callback,
        ctx.user_data(),
    );
    let _version = ctx.wait_success();

    assert_eq!(common::count_rows_sync(table_ptr, None), 7);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_list_indices_returns_json() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr,
        "list_idx_ffi",
        vec![create_test_batch(10)],
    );
    common::create_btree_index_sync(table_ptr, "id");

    let ctx = common::FfiTestContext::new();
    table_list_indices(table_ptr, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let json = unsafe { std::ffi::CStr::from_ptr(result as *const libc::c_char) }
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        json.contains("\"columns\""),
        "expected index JSON to include 'columns' field, got {}",
        json
    );
    assert!(
        json.contains("\"id\""),
        "expected index JSON to reference 'id' column, got {}",
        json
    );

    free_string(result as *mut libc::c_char);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

/// Converts a RecordBatch to C Data Interface structs for FFI ingestion tests.
fn batch_to_cdata(
    batch: &RecordBatch,
) -> (arrow_data::ffi::FFI_ArrowArray, arrow_schema::ffi::FFI_ArrowSchema) {
    use arrow_array::Array;
    let struct_array: arrow_array::StructArray = batch.clone().into();
    let data = struct_array.to_data();
    let ffi_array = arrow_data::ffi::FFI_ArrowArray::new(&data);
    let ffi_schema =
        arrow_schema::ffi::FFI_ArrowSchema::try_from(data.data_type()).unwrap();
    (ffi_array, ffi_schema)
}

#[test]
fn test_table_merge_insert_upsert_updates_and_inserts() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_upsert_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[2, 3, 4], &["B", "C", "D"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),   // no matched filter
        true,          // when_not_matched_insert_all
        false,         // when_not_matched_by_source_delete
        ptr::null(),   // no delete filter
        &mut ffi_array,
        &mut ffi_schema,
        1,             // batch_count
        true,          // use_index
        -1,            // timeout_ms (no timeout)
        -1,            // use_lsm_write (sentinel: leave default)
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    assert_eq!(common::count_rows_sync(table_ptr, None), 4);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_merge_insert_insert_only_appends_new_rows() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2], &["a", "b"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_ins_only_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[2, 3], &["B", "C"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        false,         // when_matched_update_all
        ptr::null(),
        true,          // when_not_matched_insert_all
        false,
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,             // batch_count
        true,          // use_index
        -1,            // timeout_ms
        -1,            // use_lsm_write (sentinel: leave default)
        common::ffi_callback,
        ctx.user_data(),
    );
    ctx.wait_success();

    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_merge_insert_delete_not_in_source_removes_rows() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_del_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[2], &["B"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),
        false,         // when_not_matched_insert_all
        true,          // when_not_matched_by_source_delete
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,             // batch_count
        true,          // use_index
        -1,            // timeout_ms
        -1,            // use_lsm_write (sentinel: leave default)
        common::ffi_callback,
        ctx.user_data(),
    );
    ctx.wait_success();

    assert_eq!(common::count_rows_sync(table_ptr, None), 1);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_take_offsets_returns_rows() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[10, 20, 30, 40, 50], &["a", "b", "c", "d", "e"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_off_ffi", vec![initial]);

    let offsets: Vec<u64> = vec![0, 2, 4];
    table_take_offsets(
        table_ptr,
        offsets.as_ptr(),
        offsets.len(),
        ptr::null(),
        false,
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    // Result is FfiCData pointer with Arrow C Data Interface
    let cdata = result as *mut FfiCData;
    let (array_ptr, schema_ptr) = unsafe { ((*cdata).array, (*cdata).schema) };
    assert!(!array_ptr.is_null());
    assert!(!schema_ptr.is_null());

    // Import and verify 3 rows
    let schema = unsafe { arrow_schema::ffi::FFI_ArrowSchema::from_raw(schema_ptr) };
    let array = unsafe { arrow_data::ffi::FFI_ArrowArray::from_raw(array_ptr) };
    let data = unsafe { arrow_array::ffi::from_ffi(array, &schema).unwrap() };
    let struct_array = arrow_array::StructArray::from(data);
    assert_eq!(struct_array.len(), 3);

    // FfiCData shell is now empty (pointers consumed by from_raw), just free the outer box
    unsafe { drop(Box::from_raw(cdata)) };

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_take_offsets_with_columns_returns_subset() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[10, 20, 30], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_off_cols_ffi", vec![initial]);

    let offsets: Vec<u64> = vec![0, 1];
    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    table_take_offsets(
        table_ptr,
        offsets.as_ptr(),
        offsets.len(),
        columns_json.as_ptr(),
        false,
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();

    let cdata = result as *mut FfiCData;
    let (array_ptr, schema_ptr) = unsafe { ((*cdata).array, (*cdata).schema) };

    let schema = unsafe { arrow_schema::ffi::FFI_ArrowSchema::from_raw(schema_ptr) };
    let array = unsafe { arrow_data::ffi::FFI_ArrowArray::from_raw(array_ptr) };
    let data = unsafe { arrow_array::ffi::from_ffi(array, &schema).unwrap() };
    let struct_array = arrow_array::StructArray::from(data);
    assert_eq!(struct_array.num_columns(), 1);
    assert_eq!(struct_array.column_names()[0], "id");

    unsafe { drop(Box::from_raw(cdata)) };
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_take_row_ids_returns_rows() {
    use arrow_array::cast::AsArray;
    use lancedb::query::{ExecutableQuery, QueryBase};

    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[10, 20, 30], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_rid_ffi", vec![initial]);

    // Get row IDs via lancedb API
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let row_ids: Vec<u64> = rt.block_on(async {
        use futures::TryStreamExt;
        let query = table.query().with_row_id();
        let stream = query.execute().await.unwrap();
        let _schema = stream.schema().clone();
        let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await.unwrap();
        let batch = &batches[0];
        let col = batch
            .column_by_name("_rowid")
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>();
        vec![col.value(0), col.value(2)]
    });

    // Now call the FFI function
    table_take_row_ids(
        table_ptr,
        row_ids.as_ptr(),
        row_ids.len(),
        ptr::null(),
        false,
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();

    let cdata = result as *mut FfiCData;
    let (array_ptr, schema_ptr) = unsafe { ((*cdata).array, (*cdata).schema) };

    let schema = unsafe { arrow_schema::ffi::FFI_ArrowSchema::from_raw(schema_ptr) };
    let array = unsafe { arrow_data::ffi::FFI_ArrowArray::from_raw(array_ptr) };
    let data = unsafe { arrow_array::ffi::from_ffi(array, &schema).unwrap() };
    let struct_array = arrow_array::StructArray::from(data);
    assert_eq!(struct_array.len(), 2);

    unsafe { drop(Box::from_raw(cdata)) };
    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== Free function null-safety tests =====

#[test]
fn test_table_delete_result_free_null_is_safe() {
    table_delete_result_free(ptr::null_mut());
}

#[test]
fn test_table_update_result_free_null_is_safe() {
    table_update_result_free(ptr::null_mut());
}

#[test]
fn test_table_merge_result_free_null_is_safe() {
    table_merge_result_free(ptr::null_mut());
}

#[test]
fn test_table_index_stats_free_null_is_safe() {
    table_index_stats_free(ptr::null_mut());
}

#[test]
fn test_table_stats_free_null_is_safe() {
    table_stats_free(ptr::null_mut());
}

// ===== table_delete FFI: returns FfiDeleteResult =====

#[test]
fn test_table_delete_returns_delete_result() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let batch = create_id_value_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "delete_result_ffi", vec![batch]);

    let predicate = std::ffi::CString::new("id > 3").unwrap();
    table_delete(table_ptr, predicate.as_ptr(), common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_result = result as *mut FfiDeleteResult;
    let delete_result = unsafe { &*ffi_result };
    assert_eq!(delete_result.num_deleted_rows, 2);
    assert!(delete_result.version > 0);

    table_delete_result_free(ffi_result);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_delete_no_matching_rows_returns_zero() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let batch = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "delete_none_ffi", vec![batch]);

    let predicate = std::ffi::CString::new("id > 100").unwrap();
    table_delete(table_ptr, predicate.as_ptr(), common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_result = result as *mut FfiDeleteResult;
    let delete_result = unsafe { &*ffi_result };
    assert_eq!(delete_result.num_deleted_rows, 0);

    table_delete_result_free(ffi_result);
    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_update FFI: returns FfiUpdateResult =====

#[test]
fn test_table_update_returns_update_result() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let batch = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "update_result_ffi", vec![batch]);

    let filter = std::ffi::CString::new("id >= 2").unwrap();
    let columns_json = std::ffi::CString::new(r#"[["value","'updated'"]]"#).unwrap();
    table_update(
        table_ptr,
        filter.as_ptr(),
        columns_json.as_ptr(),
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_result = result as *mut FfiUpdateResult;
    let update_result = unsafe { &*ffi_result };
    assert_eq!(update_result.rows_updated, 2);
    assert!(update_result.version > 0);

    table_update_result_free(ffi_result);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_merge_insert FFI: returns FfiMergeResult =====

#[test]
fn test_table_merge_insert_returns_merge_result() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_result_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[2, 3, 4, 5], &["B", "C", "D", "E"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),
        true,          // when_not_matched_insert_all
        false,
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,
        true,
        -1,
        -1,            // use_lsm_write (sentinel: leave default)
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_result = result as *mut FfiMergeResult;
    let merge_result = unsafe { &*ffi_result };
    assert!(merge_result.version > 0);
    assert_eq!(merge_result.num_inserted_rows, 2);
    assert_eq!(merge_result.num_updated_rows, 2);
    assert_eq!(merge_result.num_deleted_rows, 0);

    table_merge_result_free(ffi_result);
    assert_eq!(common::count_rows_sync(table_ptr, None), 5);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_index_stats FFI: returns FfiIndexStats =====

#[test]
fn test_table_index_stats_returns_stats() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_stats_ffi");

    common::add_sync(table_ptr, vec![create_test_batch(100)]);
    common::create_btree_index_sync(table_ptr, "id");

    let indices = common::list_indices_sync(table_ptr);
    let idx_name = &indices[0].name;
    let name_cstr = std::ffi::CString::new(idx_name.as_str()).unwrap();

    table_index_stats(table_ptr, name_cstr.as_ptr(), common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_stats = result as *mut FfiIndexStats;
    let stats = unsafe { &*ffi_stats };
    assert_eq!(stats.num_indexed_rows, 100);
    assert_eq!(stats.num_unindexed_rows, 0);
    assert_eq!(stats.index_type, 7); // BTree

    table_index_stats_free(ffi_stats);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_index_stats_nonexistent_returns_null() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_stats_none_ffi");

    let name_cstr = std::ffi::CString::new("nonexistent_index").unwrap();
    table_index_stats(table_ptr, name_cstr.as_ptr(), common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(result.is_null());

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_stats FFI: returns FfiTableStats =====

#[test]
fn test_table_stats_returns_stats() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let batch = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "table_stats_ffi", vec![batch]);

    table_stats(table_ptr, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_stats = result as *mut FfiTableStats;
    let stats = unsafe { &*ffi_stats };
    assert_eq!(stats.num_rows, 3);
    assert!(stats.total_bytes > 0);
    assert!(stats.fragment_stats.num_fragments > 0);

    table_stats_free(ffi_stats);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_initial_storage_options / table_latest_storage_options FFI =====

#[test]
fn test_table_initial_storage_options_local_returns_null() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "init_opts_ffi");

    table_initial_storage_options(table_ptr, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(result.is_null());

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_latest_storage_options_local_returns_null() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "latest_opts_ffi");

    table_latest_storage_options(table_ptr, common::ffi_callback, ctx.user_data());
    let result = ctx.wait_success();
    assert!(result.is_null());

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_set_unenforced_primary_key_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "pk_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    table_set_unenforced_primary_key(
        table_ptr,
        columns_json.as_ptr(),
        common::ffi_callback,
        ctx.user_data(),
    );
    ctx.wait_success();

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_set_unset_lsm_write_spec_unsharded_round_trips() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "lsm_unsharded_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    let column = std::ffi::CString::new("").unwrap();
    let indexes_json = std::ffi::CString::new("[]").unwrap();
    let defaults_json = std::ffi::CString::new("{}").unwrap();

    let set_ctx = common::FfiTestContext::new();
    table_set_lsm_write_spec(
        table_ptr,
        2, // unsharded
        column.as_ptr(),
        0,
        indexes_json.as_ptr(),
        defaults_json.as_ptr(),
        common::ffi_callback,
        set_ctx.user_data(),
    );
    set_ctx.wait_success();

    let unset_ctx = common::FfiTestContext::new();
    table_unset_lsm_write_spec(table_ptr, common::ffi_callback, unset_ctx.user_data());
    unset_ctx.wait_success();

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_set_lsm_write_spec_bucket_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "lsm_bucket_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    let column = std::ffi::CString::new("id").unwrap();
    let indexes_json = std::ffi::CString::new("[]").unwrap();
    let defaults_json = std::ffi::CString::new("{}").unwrap();

    table_set_lsm_write_spec(
        table_ptr,
        0, // bucket
        column.as_ptr(),
        16,
        indexes_json.as_ptr(),
        defaults_json.as_ptr(),
        common::ffi_callback,
        ctx.user_data(),
    );
    ctx.wait_success();

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_unset_lsm_write_spec_not_set_errors() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "lsm_unset_none_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    table_unset_lsm_write_spec(table_ptr, common::ffi_callback, ctx.user_data());
    let (result, error) = ctx.wait_raw();
    assert!(result.is_null());
    assert!(!error.is_null());
    free_string(error as *mut libc::c_char);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_set_lsm_write_spec_invalid_kind_errors() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "lsm_bad_kind_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    let column = std::ffi::CString::new("").unwrap();
    let indexes_json = std::ffi::CString::new("[]").unwrap();
    let defaults_json = std::ffi::CString::new("{}").unwrap();

    table_set_lsm_write_spec(
        table_ptr,
        99, // invalid
        column.as_ptr(),
        0,
        indexes_json.as_ptr(),
        defaults_json.as_ptr(),
        common::ffi_callback,
        ctx.user_data(),
    );
    let (result, error) = ctx.wait_raw();
    assert!(result.is_null());
    assert!(!error.is_null());
    free_string(error as *mut libc::c_char);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_close_lsm_writers FFI =====

#[test]
fn test_table_close_lsm_writers_no_writers_cached_succeeds() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "lsm_close_noop_ffi");
    common::add_sync(table_ptr, vec![create_test_batch(10)]);

    // No LSM spec installed, no writers cached: must succeed as a no-op.
    table_close_lsm_writers(table_ptr, common::ffi_callback, ctx.user_data());
    ctx.wait_success();

    table_close(table_ptr);
    connection_close(conn_ptr);
}

// ===== table_merge_insert: use_lsm_write sentinel =====

#[test]
fn test_table_merge_insert_use_lsm_write_false_falls_back_to_standard_path() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "merge_lsm_opt_out_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[3, 4, 5], &["C", "D", "E"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    // No LsmWriteSpec installed on the table, and use_lsm_write=0 (false)
    // explicitly opts out. The standard merge_insert path must run.
    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),
        true,          // when_not_matched_insert_all
        false,
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,
        true,          // use_index
        -1,            // timeout_ms
        0,             // use_lsm_write = false (opt out)
        common::ffi_callback,
        ctx.user_data(),
    );
    let result = ctx.wait_success();
    assert!(!result.is_null());

    let ffi_result = result as *mut FfiMergeResult;
    let merge_result = unsafe { &*ffi_result };
    // Standard path populates the insert/update breakdown.
    assert_eq!(merge_result.num_inserted_rows, 2);
    assert_eq!(merge_result.num_updated_rows, 1);
    assert_eq!(merge_result.num_deleted_rows, 0);
    table_merge_result_free(ffi_result);

    assert_eq!(common::count_rows_sync(table_ptr, None), 5);
    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_merge_insert_use_lsm_write_true_without_spec_errors() {
    let ctx = common::FfiTestContext::new();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "merge_lsm_no_spec_ffi", vec![initial]);

    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[4], &["D"]));
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    // use_lsm_write=1 (true) requires an LsmWriteSpec on the table; without
    // one, the operation must fail.
    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,
        ptr::null(),
        true,
        false,
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,
        true,
        -1,
        1,             // use_lsm_write = true (require LSM, no spec installed)
        common::ffi_callback,
        ctx.user_data(),
    );
    let (result, error) = ctx.wait_raw();
    assert!(result.is_null());
    assert!(!error.is_null());
    free_string(error as *mut libc::c_char);

    table_close(table_ptr);
    connection_close(conn_ptr);
}

#[test]
fn test_table_merge_insert_lsm_path_then_close_lsm_writers() {
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_batch(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(
        conn_ptr, "merge_lsm_close_ffi", vec![initial]);

    // 1. Mark `id` as the unenforced primary key.
    let pk_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();
    let pk_ctx = common::FfiTestContext::new();
    table_set_unenforced_primary_key(
        table_ptr,
        pk_columns.as_ptr(),
        common::ffi_callback,
        pk_ctx.user_data(),
    );
    pk_ctx.wait_success();

    // 2. Install a bucket spec on `id` with one bucket (every row routes there).
    let bucket_col = std::ffi::CString::new("id").unwrap();
    let indexes_json = std::ffi::CString::new("[]").unwrap();
    let defaults_json = std::ffi::CString::new("{}").unwrap();
    let spec_ctx = common::FfiTestContext::new();
    table_set_lsm_write_spec(
        table_ptr,
        0, // bucket
        bucket_col.as_ptr(),
        1,
        indexes_json.as_ptr(),
        defaults_json.as_ptr(),
        common::ffi_callback,
        spec_ctx.user_data(),
    );
    spec_ctx.wait_success();

    // 3. merge_insert through the LSM path. Empty `on` defaults to the PK.
    let (mut ffi_array, mut ffi_schema) =
        batch_to_cdata(&create_id_value_batch(&[3, 4, 5], &["C", "D", "E"]));
    let on_columns = std::ffi::CString::new(r#"[]"#).unwrap();
    let merge_ctx = common::FfiTestContext::new();
    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,
        ptr::null(),
        true,
        false,
        ptr::null(),
        &mut ffi_array,
        &mut ffi_schema,
        1,
        true,
        -1,
        1, // use_lsm_write = true
        common::ffi_callback,
        merge_ctx.user_data(),
    );
    let result = merge_ctx.wait_success();
    assert!(!result.is_null());
    let ffi_result = result as *mut FfiMergeResult;
    let merge_result = unsafe { &*ffi_result };
    // LSM path: num_rows reports total written; insert/update breakdown stays 0
    // until compaction.
    assert_eq!(merge_result.num_rows, 3);
    assert_eq!(merge_result.num_inserted_rows, 0);
    assert_eq!(merge_result.num_updated_rows, 0);
    assert_eq!(merge_result.version, 0);
    table_merge_result_free(ffi_result);

    // 4. Close the cached shard writer. Required before a different shard
    // can be written; safe to call here even with a single bucket.
    let close_ctx = common::FfiTestContext::new();
    table_close_lsm_writers(table_ptr, common::ffi_callback, close_ctx.user_data());
    close_ctx.wait_success();

    // 5. A subsequent merge_insert reopens the writer lazily and succeeds.
    let (mut ffi_array2, mut ffi_schema2) =
        batch_to_cdata(&create_id_value_batch(&[6], &["F"]));
    let merge_ctx2 = common::FfiTestContext::new();
    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,
        ptr::null(),
        true,
        false,
        ptr::null(),
        &mut ffi_array2,
        &mut ffi_schema2,
        1,
        true,
        -1,
        1, // use_lsm_write = true
        common::ffi_callback,
        merge_ctx2.user_data(),
    );
    let result2 = merge_ctx2.wait_success();
    assert!(!result2.is_null());
    let ffi_result2 = result2 as *mut FfiMergeResult;
    let merge_result2 = unsafe { &*ffi_result2 };
    assert_eq!(merge_result2.num_rows, 1);
    table_merge_result_free(ffi_result2);

    table_close(table_ptr);
    connection_close(conn_ptr);
}
