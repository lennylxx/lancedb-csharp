use lancedb::connection::Connection;
use lancedb::table::Table;
use lazy_static::lazy_static;
use libc::c_char;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub mod ffi;
mod query;
mod table;

// Re-export FFI functions for integration tests
pub use query::{query_free, query_nearest_to, vector_query_column, vector_query_free};
pub use table::{free_string, table_close, table_create_query, table_get_name, table_is_open};

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().expect("Failed to create tokio runtime");
}

#[no_mangle]
pub extern "C" fn database_connect(
    uri: *const c_char,
    completion: extern "C" fn(*const Connection),
) {
    let dataset_uri = ffi::to_string(uri);
    RUNTIME.spawn(async move {
        let connection = lancedb::connection::connect(&dataset_uri).execute().await;

        let arc_connection = Arc::new(connection.unwrap());
        let ptr = Arc::into_raw(arc_connection);
        completion(ptr);
    });
}

#[no_mangle]
pub extern "C" fn database_open_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: extern "C" fn(*const Table),
) {
    let table_name = ffi::to_string(table_name);
    // Clone the Arc so the original pointer stays valid for the caller
    unsafe {
        assert!(!connection_ptr.is_null(), "Connection pointer is null");
        Arc::increment_strong_count(connection_ptr);
    }
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    RUNTIME.spawn(async move {
        let table = connection.open_table(table_name).execute().await;
        let arc_table = Arc::new(table.unwrap());
        let ptr = Arc::into_raw(arc_table);

        completion(ptr);
    });
}

/// Create an empty table with the given name and a minimal schema (single "id" int32 column).
#[no_mangle]
pub extern "C" fn database_create_empty_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: extern "C" fn(*const Table),
) {
    let table_name = ffi::to_string(table_name);
    let schema = ffi::minimal_schema();

    // Borrow the connection without consuming it
    unsafe {
        assert!(!connection_ptr.is_null(), "Connection pointer is null");
        Arc::increment_strong_count(connection_ptr);
    }
    let connection = unsafe { Arc::from_raw(connection_ptr) };

    RUNTIME.spawn(async move {
        let table = connection
            .create_empty_table(table_name, schema)
            .execute()
            .await;
        let arc_table = Arc::new(table.unwrap());
        let ptr = Arc::into_raw(arc_table);
        completion(ptr);
    });
}

#[no_mangle]
pub extern "C" fn database_close(connection_ptr: *const Connection) {
    unsafe {
        assert!(!connection_ptr.is_null(), "Connection pointer is null");
        drop(Arc::from_raw(connection_ptr));
    }
}
