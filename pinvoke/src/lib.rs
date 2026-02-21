use lancedb::connection::Connection;
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
pub use query::{query_free, query_nearest_to, vector_query_column, vector_query_free};
pub use table::{free_string, table_close, table_create_query, table_get_name, table_is_open};

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

#[no_mangle]
pub extern "C" fn database_connect(
    uri: *const c_char,
    completion: FfiCallback,
) {
    let dataset_uri = ffi::to_string(uri);
    ffi_async!(completion, async {
        lancedb::connection::connect(&dataset_uri).execute().await
    });
}

#[no_mangle]
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

#[no_mangle]
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

#[no_mangle]
pub extern "C" fn database_close(connection_ptr: *const Connection) {
    ffi_free!(connection_ptr, Connection);
}
