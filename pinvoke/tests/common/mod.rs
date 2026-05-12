//! Shared test helpers for creating database connections and tables.
#![allow(dead_code)]

use arrow_array::RecordBatch;
use lancedb::connection::Connection;
use lancedb::table::Table;
use lancedb_ffi::ffi;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};

// Wrapper to make raw pointers Send for use in the per-test mailbox.
struct FfiCallbackResult {
    result: *const std::ffi::c_void,
    error: *const libc::c_char,
}
unsafe impl Send for FfiCallbackResult {}

/// Per-call mailbox passed through the FFI `user_data` slot. Each test
/// allocates its own context so callback-based tests can execute in
/// parallel without any global mutex.
pub struct FfiTestContext {
    inner: Box<FfiTestContextInner>,
}

struct FfiTestContextInner {
    state: Mutex<Option<FfiCallbackResult>>,
    ready: Condvar,
}

impl FfiTestContext {
    /// Creates a new, empty mailbox.
    pub fn new() -> Self {
        Self {
            inner: Box::new(FfiTestContextInner {
                state: Mutex::new(None),
                ready: Condvar::new(),
            }),
        }
    }

    /// Returns the opaque pointer to hand to the FFI as `user_data`.
    /// The pointer is valid for the lifetime of this context.
    pub fn user_data(&self) -> *mut std::ffi::c_void {
        &*self.inner as *const FfiTestContextInner as *mut std::ffi::c_void
    }

    /// Waits for the callback to fire and returns the result pointer on
    /// success or panics with the FFI error message on failure.
    pub fn wait_success(self) -> *const std::ffi::c_void {
        let mut lock = self.inner.state.lock().unwrap();
        while lock.is_none() {
            lock = self.inner.ready.wait(lock).unwrap();
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

    /// Waits for the callback to fire and returns the raw `(result, error)`
    /// pair without panicking. Caller is responsible for freeing `error`
    /// with `lancedb_ffi::free_string` if non-null.
    pub fn wait_raw(self) -> (*const std::ffi::c_void, *const libc::c_char) {
        let mut lock = self.inner.state.lock().unwrap();
        while lock.is_none() {
            lock = self.inner.ready.wait(lock).unwrap();
        }
        let cb = lock.take().unwrap();
        (cb.result, cb.error)
    }
}

impl Default for FfiTestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// FFI callback that posts the result into the `FfiTestContext` referenced
/// by `user_data`. Each test owns its own context, so concurrent tests do
/// not interfere with one another.
pub extern "C" fn ffi_callback(
    result: *const std::ffi::c_void,
    error: *const libc::c_char,
    user_data: *mut std::ffi::c_void,
) {
    assert!(
        !user_data.is_null(),
        "ffi_callback received a null user_data; tests must pass FfiTestContext::user_data()"
    );
    let inner = unsafe { &*(user_data as *const FfiTestContextInner) };
    let mut lock = inner.state.lock().unwrap();
    *lock = Some(FfiCallbackResult { result, error });
    inner.ready.notify_all();
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

/// Counts rows in a table, optionally filtered.
pub fn count_rows_sync(table_ptr: *const Table, filter: Option<String>) -> usize {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { table.count_rows(filter).await.unwrap() })
}

/// Adds data from RecordBatches to the table.
pub fn add_sync(table_ptr: *const Table, batches: Vec<RecordBatch>) {
    unsafe { Arc::increment_strong_count(table_ptr) };
    let table = unsafe { Arc::from_raw(table_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        table.add(batches).execute().await.unwrap();
    });
}

/// Creates a table with initial RecordBatch data and returns a raw Table pointer.
pub fn create_table_with_data_sync(
    connection_ptr: *const Connection,
    name: &str,
    batches: Vec<RecordBatch>,
) -> *const Table {
    unsafe { Arc::increment_strong_count(connection_ptr) };
    let connection = unsafe { Arc::from_raw(connection_ptr) };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = rt.block_on(async {
        connection
            .create_table(name, batches)
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
