//! Tests for table FFI functions.
//! Verifies that borrow operations do not consume the Arc pointer.

mod common;

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

#[test]
fn test_add_columns_with_sql_expression() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "add_cols", ipc_bytes);

    common::add_columns_sync(
        table_ptr,
        vec![("doubled".to_string(), "id * 2".to_string())],
    );

    let schema_bytes = common::schema_ipc_sync(table_ptr);
    assert!(schema_bytes.len() > 0);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_alter_columns_rename() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lancedb::table::ColumnAlteration;
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("old_name", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "alter_cols", ipc_bytes);

    let mut alt = ColumnAlteration::new("old_name".to_string());
    alt.rename = Some("new_name".to_string());
    common::alter_columns_sync(table_ptr, vec![alt]);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_drop_columns() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int32, false),
        Field::new("remove", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![4, 5, 6])),
        ],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "drop_cols", ipc_bytes);

    common::drop_columns_sync(table_ptr, &["remove"]);

    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_optimize_after_modifications() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "optimize_test", ipc_bytes);

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
    ).unwrap();
    let ipc_bytes2 = lancedb::ipc::batches_to_ipc_file(&[batch2]).unwrap();
    common::add_ipc_sync(table_ptr, ipc_bytes2);

    common::optimize_sync(table_ptr);

    assert_eq!(common::count_rows_sync(table_ptr, None), 6);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_tags_create_and_list() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "tags_test", ipc_bytes);

    let version = common::version_sync(table_ptr);
    common::create_tag_sync(table_ptr, "v1", version);

    let tags = common::list_tags_sync(table_ptr);
    assert!(tags.contains_key("v1"));
    assert_eq!(tags["v1"].version, version);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_tags_delete() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let ipc_bytes = lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap();
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "tags_del", ipc_bytes);

    let version = common::version_sync(table_ptr);
    common::create_tag_sync(table_ptr, "temp_tag", version);
    assert!(common::list_tags_sync(table_ptr).contains_key("temp_tag"));

    common::delete_tag_sync(table_ptr, "temp_tag");
    assert!(!common::list_tags_sync(table_ptr).contains_key("temp_tag"));

    table_close(table_ptr);
    database_close(conn_ptr);
}

fn create_id_value_ipc(ids: &[i32], values: &[&str]) -> Vec<u8> {
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap();
    lancedb::ipc::batches_to_ipc_file(&[batch]).unwrap()
}

#[test]
fn test_create_index_with_name_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_name_ffi");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(100));

    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    let index_type = std::ffi::CString::new("BTree").unwrap();
    let config_json = std::ffi::CString::new("{}").unwrap();
    let name = std::ffi::CString::new("my_named_idx").unwrap();

    table_create_index(
        table_ptr,
        columns_json.as_ptr(),
        index_type.as_ptr(),
        config_json.as_ptr(),
        true,
        name.as_ptr(),
        true,
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let indices = common::list_indices_sync(table_ptr);
    assert!(indices.iter().any(|i| i.name == "my_named_idx"));

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_create_index_train_false_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "idx_no_train_ffi");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(100));

    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    let index_type = std::ffi::CString::new("BTree").unwrap();
    let config_json = std::ffi::CString::new("{}").unwrap();

    table_create_index(
        table_ptr,
        columns_json.as_ptr(),
        index_type.as_ptr(),
        config_json.as_ptr(),
        true,
        ptr::null(),
        false,
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    let indices = common::list_indices_sync(table_ptr);
    assert!(!indices.is_empty());

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_optimize_with_params_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "opt_params_ffi");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));
    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    // cleanup_older_than_ms = 0 (prune immediately), delete_unverified = true
    table_optimize(table_ptr, 0, true, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    // The result is a JSON string pointer; free it
    free_string(result as *mut libc::c_char);

    assert_eq!(common::count_rows_sync(table_ptr, None), 10);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_optimize_default_params_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());
    let table_ptr = common::create_table_sync(conn_ptr, "opt_default_ffi");

    common::add_ipc_sync(table_ptr, create_test_ipc_data(5));

    // cleanup_older_than_ms = -1 (default), delete_unverified = false
    table_optimize(table_ptr, -1, false, common::ffi_callback);
    let result = common::ffi_wait_success();
    assert!(!result.is_null());
    free_string(result as *mut libc::c_char);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_merge_insert_upsert_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_upsert_ffi", initial);

    let new_data = create_id_value_ipc(&[2, 3, 4], &["B", "C", "D"]);
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),   // no matched filter
        true,          // when_not_matched_insert_all
        false,         // when_not_matched_by_source_delete
        ptr::null(),   // no delete filter
        new_data.as_ptr(),
        new_data.len(),
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    assert_eq!(common::count_rows_sync(table_ptr, None), 4);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_merge_insert_insert_only_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[1, 2], &["a", "b"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_ins_only_ffi", initial);

    let new_data = create_id_value_ipc(&[2, 3], &["B", "C"]);
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        false,         // when_matched_update_all
        ptr::null(),
        true,          // when_not_matched_insert_all
        false,
        ptr::null(),
        new_data.as_ptr(),
        new_data.len(),
        common::ffi_callback,
    );
    common::ffi_wait_success();

    assert_eq!(common::count_rows_sync(table_ptr, None), 3);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_merge_insert_delete_not_in_source_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[1, 2, 3], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "merge_del_ffi", initial);

    let new_data = create_id_value_ipc(&[2], &["B"]);
    let on_columns = std::ffi::CString::new(r#"["id"]"#).unwrap();

    table_merge_insert(
        table_ptr,
        on_columns.as_ptr(),
        true,          // when_matched_update_all
        ptr::null(),
        false,         // when_not_matched_insert_all
        true,          // when_not_matched_by_source_delete
        ptr::null(),
        new_data.as_ptr(),
        new_data.len(),
        common::ffi_callback,
    );
    common::ffi_wait_success();

    assert_eq!(common::count_rows_sync(table_ptr, None), 1);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_take_offsets_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[10, 20, 30, 40, 50], &["a", "b", "c", "d", "e"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_off_ffi", initial);

    let offsets: Vec<u64> = vec![0, 2, 4];
    table_take_offsets(
        table_ptr,
        offsets.as_ptr(),
        offsets.len(),
        ptr::null(),
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();
    assert!(!result.is_null());

    // Result is FfiBytes pointer
    let ffi_bytes = result as *mut FfiBytes;
    let data_ptr = unsafe { (*ffi_bytes).data };
    let len = unsafe { (*ffi_bytes).len };
    assert!(len > 0);

    let ipc_data = unsafe { std::slice::from_raw_parts(data_ptr, len) }.to_vec();
    let reader = lancedb::ipc::ipc_file_to_batches(ipc_data).unwrap();
    let batches: Vec<arrow_array::RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    free_ffi_bytes(ffi_bytes);

    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_take_offsets_with_columns_ffi() {
    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[10, 20, 30], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_off_cols_ffi", initial);

    let offsets: Vec<u64> = vec![0, 1];
    let columns_json = std::ffi::CString::new(r#"["id"]"#).unwrap();
    table_take_offsets(
        table_ptr,
        offsets.as_ptr(),
        offsets.len(),
        columns_json.as_ptr(),
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();

    let ffi_bytes = result as *mut FfiBytes;
    let data_ptr = unsafe { (*ffi_bytes).data };
    let len = unsafe { (*ffi_bytes).len };

    let ipc_data = unsafe { std::slice::from_raw_parts(data_ptr, len) }.to_vec();
    let reader = lancedb::ipc::ipc_file_to_batches(ipc_data).unwrap();
    let batches: Vec<arrow_array::RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");

    free_ffi_bytes(ffi_bytes);
    table_close(table_ptr);
    database_close(conn_ptr);
}

#[test]
fn test_take_row_ids_ffi() {
    use arrow_array::cast::AsArray;
    use lancedb::query::{ExecutableQuery, QueryBase};

    let _lock = common::ffi_lock();
    let tmp = TempDir::new().unwrap();
    let conn_ptr = common::connect_sync(tmp.path().to_str().unwrap());

    let initial = create_id_value_ipc(&[10, 20, 30], &["a", "b", "c"]);
    let table_ptr = common::create_table_with_data_sync(conn_ptr, "take_rid_ffi", initial);

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
        common::ffi_callback,
    );
    let result = common::ffi_wait_success();

    let ffi_bytes = result as *mut FfiBytes;
    let data_ptr = unsafe { (*ffi_bytes).data };
    let len = unsafe { (*ffi_bytes).len };

    let ipc_data = unsafe { std::slice::from_raw_parts(data_ptr, len) }.to_vec();
    let reader = lancedb::ipc::ipc_file_to_batches(ipc_data).unwrap();
    let batches: Vec<arrow_array::RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    free_ffi_bytes(ffi_bytes);
    table_close(table_ptr);
    database_close(conn_ptr);
}
