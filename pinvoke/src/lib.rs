use lancedb::connection::Connection;
use lancedb::database::CreateTableMode;
use lazy_static::lazy_static;
use libc::c_char;
use std::ffi::CString;
use tokio::runtime::Runtime;

#[macro_use]
mod macros;
pub mod ffi;
mod query;
mod table;

// Re-export FFI functions for integration tests
pub use query::{
    query_execute, query_fast_search, query_free, query_full_text_search, query_limit,
    query_nearest_to, query_offset, query_only_if, query_postfilter, query_select,
    query_with_row_id, vector_query_bypass_vector_index, vector_query_column,
    vector_query_distance_range, vector_query_distance_type, vector_query_ef,
    vector_query_execute, vector_query_fast_search, vector_query_free,
    vector_query_full_text_search, vector_query_limit, vector_query_nprobes, vector_query_offset,
    vector_query_only_if, vector_query_postfilter, vector_query_refine_factor,
    vector_query_select, vector_query_with_row_id,
};
pub use table::{
    free_ffi_bytes, free_string, table_add, table_add_columns, table_alter_columns,
    table_checkout, table_checkout_latest, table_close, table_count_rows, table_create_index,
    table_create_query, table_delete, table_drop_columns, table_get_name, table_is_open,
    table_list_indices, table_list_versions, table_optimize, table_restore, table_schema,
    table_tags_create, table_tags_delete, table_tags_list, table_tags_update, table_update,
    table_uri, table_version, FfiBytes,
};

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().expect("Failed to create tokio runtime");
}

/// Callback type for async FFI operations.
/// On success: result is non-null, error is null.
/// On error: result is null, error is a UTF-8 C string (caller must free with free_string).
type FfiCallback = extern "C" fn(result: *const std::ffi::c_void, error: *const c_char);

/// Helper to invoke a callback with an error string.
fn callback_error(completion: FfiCallback, err: impl std::fmt::Display) {
    let msg = CString::new(err.to_string()).unwrap_or_default();
    completion(std::ptr::null(), msg.into_raw());
}

#[unsafe(no_mangle)]
pub extern "C" fn database_connect(
    uri: *const c_char,
    completion: FfiCallback,
) {
    let dataset_uri = ffi::to_string(uri);
    ffi_async!(completion, async {
        lancedb::connection::connect(&dataset_uri).execute().await
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_open_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    ffi_async!(completion, async {
        connection.open_table(table_name).execute().await
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_empty_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let schema = ffi::minimal_schema();
    let connection = ffi_clone_arc!(connection_ptr, Connection);

    ffi_async!(completion, async {
        connection
            .create_empty_table(table_name, schema)
            .execute()
            .await
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    ipc_data: *const u8,
    ipc_len: usize,
    mode: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);

    let ipc_bytes = unsafe { std::slice::from_raw_parts(ipc_data, ipc_len) }.to_vec();

    let create_mode = if mode.is_null() {
        CreateTableMode::Create
    } else {
        match ffi::to_string(mode).as_str() {
            "overwrite" => CreateTableMode::Overwrite,
            _ => CreateTableMode::Create,
        }
    };

    RUNTIME.spawn(async move {
        let reader = match lancedb::ipc::ipc_file_to_batches(ipc_bytes) {
            Ok(r) => r,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        match connection
            .create_table(table_name, reader)
            .mode(create_mode)
            .execute()
            .await
        {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_close(connection_ptr: *const Connection) {
    ffi_free!(connection_ptr, Connection);
}

#[unsafe(no_mangle)]
pub extern "C" fn database_table_names(
    connection_ptr: *const Connection,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    RUNTIME.spawn(async move {
        match connection.table_names().execute().await {
            Ok(names) => {
                let joined = names.join("\n");
                let c_str = CString::new(joined).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_drop_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    RUNTIME.spawn(async move {
        match connection.drop_table(&table_name, &[]).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_drop_all_tables(
    connection_ptr: *const Connection,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    RUNTIME.spawn(async move {
        match connection.drop_all_tables(&[]).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}
