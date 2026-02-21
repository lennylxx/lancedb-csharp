use lancedb::query::Query;
use lancedb::table::Table;
use libc::c_char;
use std::ffi::CString;
use std::sync::Arc;

use crate::FfiCallback;

/// Returns the name of the table as a C string. Caller must free with free_string().
#[no_mangle]
pub extern "C" fn table_get_name(table_ptr: *const Table) -> *mut c_char {
    let table = ffi_borrow!(table_ptr, Table);
    let name = table.name();
    let c_str_name = CString::new(name).unwrap();
    c_str_name.into_raw()
}

#[no_mangle]
pub extern "C" fn table_is_open(table_ptr: *const Table) -> bool {
    !table_ptr.is_null()
}

/// Creates a new Query from the table.
#[no_mangle]
pub extern "C" fn table_create_query(table_ptr: *const Table) -> *const Query {
    let table = ffi_borrow!(table_ptr, Table);
    let query = table.query().clone();
    Arc::into_raw(Arc::new(query))
}

/// Counts the number of rows in the table, optionally filtered by a SQL predicate.
/// Returns the count as a pointer-sized integer via the callback.
#[no_mangle]
pub extern "C" fn table_count_rows(
    table_ptr: *const Table,
    filter: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let filter = if filter.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(filter))
    };
    crate::RUNTIME.spawn(async move {
        match table.count_rows(filter).await {
            Ok(count) => {
                completion(count as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Deletes rows from the table matching the given SQL predicate.
#[no_mangle]
pub extern "C" fn table_delete(
    table_ptr: *const Table,
    predicate: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let predicate = crate::ffi::to_string(predicate);
    crate::RUNTIME.spawn(async move {
        match table.delete(&predicate).await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Updates rows in the table. columns_json is a JSON array of [name, expr] pairs.
/// filter is an optional SQL predicate (null for all rows).
#[no_mangle]
pub extern "C" fn table_update(
    table_ptr: *const Table,
    filter: *const c_char,
    columns_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let filter = if filter.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(filter))
    };
    let columns_str = crate::ffi::to_string(columns_json);

    crate::RUNTIME.spawn(async move {
        let columns: Vec<(String, String)> = match serde_json::from_str(&columns_str) {
            Ok(c) => c,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let mut builder = table.update();
        if let Some(f) = filter {
            builder = builder.only_if(f);
        }
        for (name, expr) in columns {
            builder = builder.column(name, expr);
        }

        match builder.execute().await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Returns the table's Arrow schema serialized as Arrow IPC bytes.
/// The callback receives a pointer to an FfiBytes struct (caller must free with free_ffi_bytes).
#[no_mangle]
pub extern "C" fn table_schema(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.schema().await {
            Ok(schema) => {
                let ipc_bytes = crate::ffi::schema_to_ipc(&schema);
                let ffi_bytes = Box::new(FfiBytes {
                    data: ipc_bytes.as_ptr(),
                    len: ipc_bytes.len(),
                    _owner: ipc_bytes,
                });
                let ptr = Box::into_raw(ffi_bytes);
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Opaque byte buffer returned from FFI. Must be freed with free_ffi_bytes.
#[repr(C)]
pub struct FfiBytes {
    pub data: *const u8,
    pub len: usize,
    _owner: Vec<u8>,
}

/// Frees an FfiBytes struct allocated by Rust.
#[no_mangle]
pub extern "C" fn free_ffi_bytes(ptr: *mut FfiBytes) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

/// Closes the table and frees the underlying Arc.
#[no_mangle]
pub extern "C" fn table_close(table_ptr: *const Table) {
    ffi_free!(table_ptr, Table);
}

#[no_mangle]
pub extern "C" fn free_string(c_string: *mut c_char) {
    unsafe {
        if c_string.is_null() {
            return;
        }
        drop(CString::from_raw(c_string))
    };
}
