use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{DataType, Schema, SchemaRef};
use libc::c_char;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

/// Stores an error message in thread-local storage for retrieval by the caller.
pub fn set_last_error(msg: impl std::fmt::Display) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg.to_string()).ok();
    });
}

/// Returns the last error message as a C string, or null if no error.
/// The caller must free the returned string with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn ffi_get_last_error() -> *mut c_char {
    LAST_ERROR.with(|e| {
        match e.borrow_mut().take() {
            Some(s) => s.into_raw(),
            None => std::ptr::null_mut(),
        }
    })
}

/// Converts a C string pointer to an owned Rust String.
/// The caller retains ownership of the original C string.
/// Returns an empty string if the pointer is null or the data is not valid UTF-8.
pub fn to_string(c_string: *const c_char) -> String {
    if c_string.is_null() {
        return String::new();
    }
    let c_str = unsafe { CStr::from_ptr(c_string) };
    c_str.to_str().unwrap_or_default().to_owned()
}

/// Create a minimal Arrow schema with a single "id" integer field.
/// Used for creating empty tables for testing.
pub fn minimal_schema() -> Arc<Schema> {
    use arrow_schema::Field;
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

/// Parses a distance type string into a LanceDB DistanceType enum.
pub fn parse_distance_type(s: &str) -> Result<lancedb::DistanceType, String> {
    match s.to_lowercase().as_str() {
        "l2" => Ok(lancedb::DistanceType::L2),
        "cosine" => Ok(lancedb::DistanceType::Cosine),
        "dot" => Ok(lancedb::DistanceType::Dot),
        "hamming" => Ok(lancedb::DistanceType::Hamming),
        _ => Err(format!("Unknown distance type: {}", s)),
    }
}

/// Parses an optional JSON-encoded map from a nullable C string.
/// Returns None if the pointer is null.
pub fn parse_optional_json_map(json_ptr: *const c_char) -> Option<HashMap<String, String>> {
    if json_ptr.is_null() {
        return None;
    }
    let json_str = to_string(json_ptr);
    sonic_rs::from_str(&json_str).ok()
}

/// Parses an optional JSON-encoded string array from a nullable C string.
/// Returns None if the pointer is null.
pub fn parse_optional_json_list(json_ptr: *const c_char) -> Option<Vec<String>> {
    if json_ptr.is_null() {
        return None;
    }
    let json_str = to_string(json_ptr);
    sonic_rs::from_str(&json_str).ok()
}

/// Parses an optional C string pointer into an Option<String>.
/// Returns None if the pointer is null.
pub fn parse_optional_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    Some(to_string(ptr))
}

/// Imports a RecordBatch from Arrow C Data Interface pointers.
/// Takes ownership of both the array and schema by reading from the pointers.
/// After reading, the source pointers are zeroed to prevent double-free
/// (the original structs' Drop will be a no-op with zeroed release callbacks).
/// Must be called synchronously before any async task, as the pointers
/// may become invalid after the FFI function returns.
pub fn import_record_batch(
    array_ptr: *mut arrow_data::ffi::FFI_ArrowArray,
    schema_ptr: *mut arrow_schema::ffi::FFI_ArrowSchema,
) -> Result<RecordBatch, String> {
    if array_ptr.is_null() || schema_ptr.is_null() {
        return Err("C Data array or schema pointer is null".to_string());
    }
    unsafe {
        let ffi_array = std::ptr::read(array_ptr);
        std::ptr::write_bytes(array_ptr, 0, 1);
        let ffi_schema = std::ptr::read(schema_ptr);
        std::ptr::write_bytes(schema_ptr, 0, 1);
        let data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema)
            .map_err(|e| format!("Failed to import C Data: {}", e))?;
        let struct_array = StructArray::from(data);
        Ok(RecordBatch::from(struct_array))
    }
}

/// Imports a Schema from an Arrow C Data Interface pointer.
/// Takes ownership by reading from the pointer and zeroing the source.
pub fn import_schema(
    schema_ptr: *mut arrow_schema::ffi::FFI_ArrowSchema,
) -> Result<SchemaRef, String> {
    if schema_ptr.is_null() {
        return Err("C Data schema pointer is null".to_string());
    }
    unsafe {
        let ffi_schema = std::ptr::read(schema_ptr);
        std::ptr::write_bytes(schema_ptr, 0, 1);
        let data_type = DataType::try_from(&ffi_schema)
            .map_err(|e| format!("Failed to import C Data schema: {}", e))?;
        match data_type {
            DataType::Struct(fields) => Ok(Arc::new(Schema::new(fields))),
            _ => Err("Expected Struct type for schema import".to_string()),
        }
    }
}

/// Imports multiple RecordBatches from contiguous Arrow C Data Interface arrays.
/// `arrays` points to `count` contiguous FFI_ArrowArray structs.
/// `schema` points to a single FFI_ArrowSchema shared by all batches.
/// All pointers are zeroed after reading to prevent double-free.
pub fn import_batches(
    arrays: *mut arrow_data::ffi::FFI_ArrowArray,
    schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
    count: usize,
) -> Result<(Vec<RecordBatch>, SchemaRef), String> {
    if arrays.is_null() || schema.is_null() || count == 0 {
        return Err("C Data arrays/schema pointer is null or count is 0".to_string());
    }
    unsafe {
        // Read the schema (borrowed for from_ffi, then converted to SchemaRef)
        let ffi_schema = std::ptr::read(schema);
        std::ptr::write_bytes(schema, 0, 1);

        let mut batches = Vec::with_capacity(count);
        for i in 0..count {
            let array_ptr = arrays.add(i);
            let ffi_array = std::ptr::read(array_ptr);
            std::ptr::write_bytes(array_ptr, 0, 1);
            let data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema)
                .map_err(|e| format!("Failed to import C Data batch {}: {}", i, e))?;
            batches.push(RecordBatch::from(StructArray::from(data)));
        }

        let data_type = DataType::try_from(&ffi_schema)
            .map_err(|e| format!("Failed to import C Data schema: {}", e))?;
        let schema_ref = match data_type {
            DataType::Struct(fields) => Arc::new(Schema::new(fields)),
            _ => return Err("Expected Struct type for schema import".to_string()),
        };

        Ok((batches, schema_ref))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn free_string(c_string: *mut c_char) {
    unsafe {
        if c_string.is_null() {
            return;
        }
        drop(CString::from_raw(c_string))
    };
}

/// Arrow C Data Interface struct holding exported FFI_ArrowArray and FFI_ArrowSchema.
/// Must be freed with free_ffi_cdata.
#[repr(C)]
pub struct FfiCData {
    pub array: *mut arrow_data::ffi::FFI_ArrowArray,
    pub schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
}

/// Frees an FfiCData struct and its contained FFI_ArrowArray and FFI_ArrowSchema.
#[unsafe(no_mangle)]
pub extern "C" fn free_ffi_cdata(ptr: *mut FfiCData) {
    if !ptr.is_null() {
        unsafe {
            let cdata = Box::from_raw(ptr);
            if !cdata.array.is_null() {
                drop(Box::from_raw(cdata.array));
            }
            if !cdata.schema.is_null() {
                drop(Box::from_raw(cdata.schema));
            }
        }
    }
}

/// Frees a heap-allocated FFI_ArrowSchema.
#[unsafe(no_mangle)]
pub extern "C" fn free_ffi_schema(ptr: *mut arrow_schema::ffi::FFI_ArrowSchema) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}
