//! Tests for low-level FFI utility functions (string conversion, etc.).

use arrow_array::Array;
use lancedb_ffi::ffi;
use std::ptr;

#[test]
fn test_to_string_returns_owned_string() {
    let c_str = std::ffi::CString::new("hello world").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "hello world");
    // c_str is still valid here — to_string must not take ownership
    assert_eq!(c_str.to_str().unwrap(), "hello world");
}

#[test]
fn test_to_string_utf8() {
    let c_str = std::ffi::CString::new("你好世界").unwrap();
    let result = ffi::to_string(c_str.as_ptr());
    assert_eq!(result, "你好世界");
}

#[test]
fn test_to_string_null_returns_empty() {
    assert_eq!(ffi::to_string(ptr::null()), "");
}

#[test]
fn test_parse_optional_json_map_null_returns_none() {
    assert!(ffi::parse_optional_json_map(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_json_map_valid_json() {
    let json = std::ffi::CString::new(r#"{"key1":"val1","key2":"val2"}"#).unwrap();
    let result = ffi::parse_optional_json_map(json.as_ptr()).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result.get("key1").unwrap(), "val1");
    assert_eq!(result.get("key2").unwrap(), "val2");
}

#[test]
fn test_parse_optional_json_map_empty_object() {
    let json = std::ffi::CString::new("{}").unwrap();
    let result = ffi::parse_optional_json_map(json.as_ptr()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_parse_optional_json_list_null_returns_none() {
    assert!(ffi::parse_optional_json_list(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_json_list_valid_json() {
    let json = std::ffi::CString::new(r#"["team","project"]"#).unwrap();
    let result = ffi::parse_optional_json_list(json.as_ptr()).unwrap();
    assert_eq!(result, vec!["team", "project"]);
}

#[test]
fn test_parse_optional_json_list_empty_array() {
    let json = std::ffi::CString::new("[]").unwrap();
    let result = ffi::parse_optional_json_list(json.as_ptr()).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_parse_optional_string_null_returns_none() {
    assert!(ffi::parse_optional_string(ptr::null()).is_none());
}

#[test]
fn test_parse_optional_string_valid() {
    let c_str = std::ffi::CString::new("s3://my-bucket/tables/foo").unwrap();
    let result = ffi::parse_optional_string(c_str.as_ptr()).unwrap();
    assert_eq!(result, "s3://my-bucket/tables/foo");
}

#[test]
fn test_import_record_batch_null_is_error() {
    let result = ffi::import_record_batch(ptr::null_mut(), ptr::null_mut());
    assert!(result.is_err());
}

#[test]
fn test_import_record_batch_valid() {
    use arrow_array::{Int32Array, RecordBatch, StructArray};
    use arrow_data::ffi::FFI_ArrowArray;
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let struct_array: StructArray = batch.into();
    let data = struct_array.to_data();
    let mut ffi_array = FFI_ArrowArray::new(&data);
    let mut ffi_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

    let result = ffi::import_record_batch(
        &mut ffi_array as *mut FFI_ArrowArray,
        &mut ffi_schema as *mut FFI_ArrowSchema,
    )
    .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.schema().fields().len(), 1);
    assert_eq!(result.schema().field(0).name(), "id");
}

#[test]
fn test_import_schema_null_is_error() {
    let result = ffi::import_schema(ptr::null_mut());
    assert!(result.is_err());
}

#[test]
fn test_import_schema_valid() {
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, false),
    ]);
    let mut ffi_schema =
        FFI_ArrowSchema::try_from(&DataType::Struct(schema.fields().clone())).unwrap();

    let result =
        ffi::import_schema(&mut ffi_schema as *mut FFI_ArrowSchema).unwrap();
    assert_eq!(result.fields().len(), 2);
    assert_eq!(result.field(0).name(), "name");
    assert_eq!(result.field(1).name(), "age");
}

#[test]
fn test_set_last_error_and_get_returns_message() {
    ffi::set_last_error("something went wrong");
    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    let err_str = unsafe { std::ffi::CStr::from_ptr(err_ptr) }
        .to_str()
        .unwrap();
    assert_eq!(err_str, "something went wrong");
    lancedb_ffi::free_string(err_ptr);
}

#[test]
fn test_get_last_error_returns_null_when_no_error() {
    // Clear any prior error
    let _ = ffi::ffi_get_last_error();
    let err_ptr = ffi::ffi_get_last_error();
    assert!(err_ptr.is_null());
}

#[test]
fn test_get_last_error_clears_after_read() {
    ffi::set_last_error("first error");
    let err_ptr = ffi::ffi_get_last_error();
    assert!(!err_ptr.is_null());
    lancedb_ffi::free_string(err_ptr);

    let err_ptr2 = ffi::ffi_get_last_error();
    assert!(err_ptr2.is_null());
}

#[test]
fn test_import_batches_null_is_error() {
    let result = ffi::import_batches(ptr::null_mut(), ptr::null_mut(), 0);
    assert!(result.is_err());
}

#[test]
fn test_import_batches_valid() {
    use arrow_array::{Int32Array, RecordBatch, StructArray};
    use arrow_data::ffi::FFI_ArrowArray;
    use arrow_schema::ffi::FFI_ArrowSchema;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch1 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let batch2 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3, 4, 5]))]).unwrap();

    let struct1: StructArray = batch1.into();
    let data1 = struct1.to_data();
    let struct2: StructArray = batch2.into();
    let data2 = struct2.to_data();

    let mut arrays = vec![
        FFI_ArrowArray::new(&data1),
        FFI_ArrowArray::new(&data2),
    ];
    let mut ffi_schema = FFI_ArrowSchema::try_from(data1.data_type()).unwrap();

    let (batches, schema_ref) = ffi::import_batches(
        arrays.as_mut_ptr(),
        &mut ffi_schema as *mut FFI_ArrowSchema,
        2,
    ).unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[1].num_rows(), 3);
    assert_eq!(schema_ref.fields().len(), 1);
    assert_eq!(schema_ref.field(0).name(), "id");
}
