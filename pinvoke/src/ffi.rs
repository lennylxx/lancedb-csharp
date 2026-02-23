use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Schema, SchemaRef};
use libc::c_char;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io::Cursor;
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
    serde_json::from_str(&json_str).ok()
}

/// Parses an optional JSON-encoded string array from a nullable C string.
/// Returns None if the pointer is null.
pub fn parse_optional_json_list(json_ptr: *const c_char) -> Option<Vec<String>> {
    if json_ptr.is_null() {
        return None;
    }
    let json_str = to_string(json_ptr);
    serde_json::from_str(&json_str).ok()
}

/// Parses an optional C string pointer into an Option<String>.
/// Returns None if the pointer is null.
pub fn parse_optional_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    Some(to_string(ptr))
}

/// Deserializes Arrow IPC file bytes into a Schema.
pub fn ipc_to_schema(ipc_data: *const u8, ipc_len: usize) -> Result<SchemaRef, String> {
    if ipc_data.is_null() || ipc_len == 0 {
        return Err("Schema IPC data is null or empty".to_string());
    }
    let bytes = unsafe { std::slice::from_raw_parts(ipc_data, ipc_len) };
    let cursor = Cursor::new(bytes);
    let reader =
        FileReader::try_new(cursor, None).map_err(|e| format!("Invalid IPC schema: {}", e))?;
    Ok(reader.schema())
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

/// Opaque byte buffer returned from FFI. Must be freed with free_ffi_bytes.
#[repr(C)]
pub struct FfiBytes {
    pub data: *const u8,
    pub len: usize,
    pub(crate) _owner: Vec<u8>,
}

/// Frees an FfiBytes struct allocated by Rust.
#[unsafe(no_mangle)]
pub extern "C" fn free_ffi_bytes(ptr: *mut FfiBytes) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

