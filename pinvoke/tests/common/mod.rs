//! Shared test helpers for creating database connections and tables.

use lancedb::connection::Connection;
use lancedb::table::Table;
use lancedb_ffi::ffi;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};

// Wrapper to make raw pointers Send for use in static Mutex.
struct FfiCallbackResult {
    result: *const std::ffi::c_void,
    error: *const libc::c_char,
}
unsafe impl Send for FfiCallbackResult {}

/// Global FFI callback result slot. Tests using this must be serialized.
static FFI_RESULT: Mutex<Option<FfiCallbackResult>> = Mutex::new(None);
static FFI_READY: Condvar = Condvar::new();
/// Lock to serialize FFI callback tests.
static FFI_TEST_LOCK: Mutex<()> = Mutex::new(());

/// FFI callback that stores the result/error in a global slot.
pub extern "C" fn ffi_callback(
    result: *const std::ffi::c_void,
    error: *const libc::c_char,
) {
    let mut lock = FFI_RESULT.lock().unwrap();
    *lock = Some(FfiCallbackResult { result, error });
    FFI_READY.notify_all();
}

/// Waits for the FFI callback result, returning the result pointer on success
/// or panicking with the error message on failure.
pub fn ffi_wait_success() -> *const std::ffi::c_void {
    let mut lock = FFI_RESULT.lock().unwrap();
    while lock.is_none() {
        lock = FFI_READY.wait(lock).unwrap();
    }
    let cb = lock.take().unwrap();
    if !cb.error.is_null() {
        let err = unsafe { std::ffi::CStr::from_ptr(cb.error) }
            .to_str()
            .unwrap()
            .to_string();
        lancedb_ffi::free_string(cb.error as *mut libc::c_char);
        panic!("FFI error: {}", err);
    }
    cb.result
}

/// Acquire the FFI test lock (serialize callback-based tests).
pub fn ffi_lock() -> std::sync::MutexGuard<'static, ()> {
    FFI_TEST_LOCK.lock().unwrap()
}

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

/// Counts rows in a table, optionally filtered.
pub fn count_rows_sync(table_ptr: *const Table, filter: Option<String>) -> usize {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.count_rows(filter).await.unwrap() })
}

/// Deletes rows matching the predicate.
pub fn delete_sync(table_ptr: *const Table, predicate: &str) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.delete(predicate).await.unwrap() });
}

/// Updates rows. columns is a list of (column_name, sql_expression) pairs.
pub fn update_sync(
    table_ptr: *const Table,
    filter: Option<String>,
    columns: Vec<(String, String)>,
) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut builder = table.update();
        if let Some(f) = filter {
            builder = builder.only_if(f);
        }
        for (name, expr) in columns {
            builder = builder.column(name, expr);
        }
        builder.execute().await.unwrap();
    });
}

/// Returns the table's schema as Arrow IPC bytes.
pub fn schema_ipc_sync(table_ptr: *const Table) -> Vec<u8> {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = rt.block_on(async { table.schema().await.unwrap() });
    lancedb::ipc::schema_to_ipc_file(&schema).unwrap()
}

/// Adds data from Arrow IPC bytes to the table.
pub fn add_ipc_sync(table_ptr: *const Table, ipc_bytes: Vec<u8>) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let reader = lancedb::ipc::ipc_file_to_batches(ipc_bytes).unwrap();
        table.add(reader).execute().await.unwrap();
    });
}

/// Returns the current version of the table.
pub fn version_sync(table_ptr: *const Table) -> u64 {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.version().await.unwrap() })
}

/// Checks out a specific version of the table.
pub fn checkout_sync(table_ptr: *const Table, version: u64) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.checkout(version).await.unwrap() });
}

/// Checks out the latest version of the table.
pub fn checkout_latest_sync(table_ptr: *const Table) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.checkout_latest().await.unwrap() });
}

/// Restores the table to the currently checked out version.
pub fn restore_sync(table_ptr: *const Table) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.restore().await.unwrap() });
}

/// Creates a table with initial IPC data and returns a raw Table pointer.
pub fn create_table_with_data_sync(
    connection_ptr: *const Connection,
    name: &str,
    ipc_bytes: Vec<u8>,
) -> *const Table {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = lancedb::ipc::ipc_file_to_batches(ipc_bytes).unwrap();
    let table = rt.block_on(async {
        connection
            .create_table(name, reader)
            .execute()
            .await
            .unwrap()
    });
    Arc::into_raw(Arc::new(table))
}

/// Creates an empty table with a custom schema and returns a raw Table pointer.
pub fn create_empty_table_with_schema_sync(
    connection_ptr: *const Connection,
    name: &str,
    schema: arrow_schema::SchemaRef,
) -> *const Table {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
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

/// Creates a table with exist_ok mode and returns a raw Table pointer.
pub fn create_table_exist_ok_sync(
    connection_ptr: *const Connection,
    name: &str,
    ipc_bytes: Vec<u8>,
) -> *const Table {
    use lancedb::database::CreateTableMode;

    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = lancedb::ipc::ipc_file_to_batches(ipc_bytes).unwrap();
    let table = rt.block_on(async {
        connection
            .create_table(name, reader)
            .mode(CreateTableMode::exist_ok(|req| req))
            .execute()
            .await
            .unwrap()
    });
    Arc::into_raw(Arc::new(table))
}

/// Creates a BTree index on the given column.
pub fn create_btree_index_sync(table_ptr: *const Table, column: &str) {
    use lancedb::index::scalar::BTreeIndexBuilder;
    use lancedb::index::Index;

    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table
            .create_index(&[column], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap()
    });
}

/// Lists indices on the table and returns the raw IndexConfig vec.
pub fn list_indices_sync(table_ptr: *const Table) -> Vec<lancedb::index::IndexConfig> {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.list_indices().await.unwrap() })
}

/// Adds new columns to the table using SQL expressions.
pub fn add_columns_sync(
    table_ptr: *const Table,
    transforms: Vec<(String, String)>,
) {
    use lancedb::table::NewColumnTransform;

    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table
            .add_columns(NewColumnTransform::SqlExpressions(transforms), None)
            .await
            .unwrap()
    });
}

/// Alters columns (rename, set nullable).
pub fn alter_columns_sync(
    table_ptr: *const Table,
    alterations: Vec<lancedb::table::ColumnAlteration>,
) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.alter_columns(&alterations).await.unwrap()
    });
}

/// Drops columns from the table.
pub fn drop_columns_sync(table_ptr: *const Table, columns: &[&str]) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.drop_columns(columns).await.unwrap()
    });
}

/// Runs optimize with default settings.
pub fn optimize_sync(table_ptr: *const Table) {
    use lancedb::table::OptimizeAction;

    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.optimize(OptimizeAction::All).await.unwrap()
    });
}

/// Creates a tag on the table for the given version.
pub fn create_tag_sync(table_ptr: *const Table, tag: &str, version: u64) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.tags().await.unwrap().create(tag, version).await.unwrap()
    });
}

/// Lists tags on the table.
pub fn list_tags_sync(
    table_ptr: *const Table,
) -> std::collections::HashMap<String, lancedb::table::TagContents> {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.tags().await.unwrap().list().await.unwrap()
    })
}

/// Deletes a tag from the table.
pub fn delete_tag_sync(table_ptr: *const Table, tag: &str) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.tags().await.unwrap().delete(tag).await.unwrap()
    });
}

/// Updates a tag to point to a new version.
pub fn update_tag_sync(table_ptr: *const Table, tag: &str, version: u64) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.tags().await.unwrap().update(tag, version).await.unwrap()
    });
}
