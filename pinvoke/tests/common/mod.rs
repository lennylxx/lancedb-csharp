//! Shared test helpers for creating database connections and tables.

use lancedb::connection::Connection;
use lancedb::table::Table;
use lancedb_pinvoke::ffi;
use std::sync::Arc;

/// Connects to a local database and returns a raw Connection pointer
/// matching the FFI ownership model (caller owns the Arc).
pub fn connect_sync(uri: &str) -> *const Connection {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let connection = rt.block_on(async {
        lancedb::connection::connect(uri).execute().await.unwrap()
    });
    Arc::into_raw(Arc::new(connection))
}

/// Creates an empty table and returns a raw Table pointer.
/// Borrows the connection pointer without consuming it.
pub fn create_table_sync(connection_ptr: *const Connection, name: &str) -> *const Table {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let schema = ffi::minimal_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = rt.block_on(async {
        connection
            .create_empty_table(name, schema)
            .execute()
            .await
            .unwrap()
    });
    Arc::into_raw(Arc::new(table))
}

/// Returns the list of table names for the connection.
pub fn table_names_sync(connection_ptr: *const Connection) -> Vec<String> {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        connection.table_names().execute().await.unwrap()
    })
}

/// Drops a table by name.
pub fn drop_table_sync(connection_ptr: *const Connection, name: &str) {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        connection.drop_table(name, &[]).await.unwrap()
    });
}

/// Drops all tables in the database.
pub fn drop_all_tables_sync(connection_ptr: *const Connection) {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        connection.drop_all_tables(&[]).await.unwrap()
    });
}
