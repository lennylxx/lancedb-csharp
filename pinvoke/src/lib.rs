use lancedb::connection::Connection;
use lancedb::database::CreateTableMode;
use libc::c_char;
use std::ffi::CString;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::runtime::Runtime;

#[macro_use]
mod macros;
pub mod ffi;
mod query;
mod table;

// Re-export FFI functions for integration tests
pub use query::{
    query_analyze_plan, query_execute, query_explain_plan, query_output_schema,
    vector_query_analyze_plan, vector_query_execute,
    vector_query_explain_plan, vector_query_output_schema,
};
pub use table::{
    table_add, table_add_columns, table_alter_columns,
    table_checkout, table_checkout_latest, table_checkout_tag, table_close, table_count_rows,
    table_create_index, table_delete, table_drop_columns, table_drop_index, table_get_name,
    table_index_stats, table_is_open, table_list_indices, table_list_versions, table_merge_insert,
    table_optimize, table_prewarm_index, table_restore, table_schema, table_tags_create,
    table_tags_delete, table_tags_get_version, table_tags_list, table_tags_update, table_take_offsets,
    table_take_row_ids, table_update, table_uri, table_version, table_wait_for_index,
};
pub use ffi::{free_ffi_cdata, free_ffi_schema, free_string, FfiCData};

/// Pool of Tokio runtimes (one per CPU core, each with 1 worker thread).
/// Async FFI calls are dispatched to the least-loaded runtime to avoid
/// task queue contention while keeping isolated per-runtime scheduling.
static RUNTIME_POOL: LazyLock<Vec<Runtime>> = LazyLock::new(|| {
    let n = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);
    (0..n)
        .map(|_| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime")
        })
        .collect()
});

/// Per-runtime inflight task counters for least-loaded dispatch.
static RUNTIME_LOAD: LazyLock<Vec<std::sync::atomic::AtomicUsize>> = LazyLock::new(|| {
    (0..RUNTIME_POOL.len())
        .map(|_| std::sync::atomic::AtomicUsize::new(0))
        .collect()
});

/// Spawns a future on the least-loaded runtime from the pool.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Find runtime with fewest inflight tasks
    let mut min_idx = 0;
    let mut min_load = RUNTIME_LOAD[0].load(std::sync::atomic::Ordering::Relaxed);
    for i in 1..RUNTIME_POOL.len() {
        let load = RUNTIME_LOAD[i].load(std::sync::atomic::Ordering::Relaxed);
        if load < min_load {
            min_load = load;
            min_idx = i;
        }
    }

    RUNTIME_LOAD[min_idx].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let idx = min_idx;

    RUNTIME_POOL[idx].spawn(async move {
        let result = future.await;
        RUNTIME_LOAD[idx].fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        result
    })
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
    read_consistency_interval_secs: f64,
    storage_options_json: *const c_char,
    index_cache_size_bytes: i64,
    metadata_cache_size_bytes: i64,
    completion: FfiCallback,
) {
    let dataset_uri = ffi::to_string(uri);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let rci_secs = if read_consistency_interval_secs.is_nan() {
        None
    } else {
        Some(read_consistency_interval_secs)
    };

    let has_session = index_cache_size_bytes >= 0 || metadata_cache_size_bytes >= 0;

    crate::spawn(async move {
        let mut builder = lancedb::connection::connect(&dataset_uri);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(secs) = rci_secs {
            builder = builder.read_consistency_interval(Duration::from_secs_f64(secs));
        }
        if has_session {
            let index_size = if index_cache_size_bytes > 0 {
                index_cache_size_bytes as usize
            } else {
                6 * 1024 * 1024 * 1024 // DEFAULT_INDEX_CACHE_SIZE (6 GiB)
            };
            let metadata_size = if metadata_cache_size_bytes > 0 {
                metadata_cache_size_bytes as usize
            } else {
                1024 * 1024 * 1024 // DEFAULT_METADATA_CACHE_SIZE (1 GiB)
            };
            let session = lancedb::Session::new(
                index_size,
                metadata_size,
                std::sync::Arc::new(lancedb::ObjectStoreRegistry::default()),
            );
            builder = builder.session(std::sync::Arc::new(session));
        }
        match builder.execute().await {
            Ok(conn) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(conn));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_open_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    storage_options_json: *const c_char,
    index_cache_size: u32,
    location: *const c_char,
    namespace_json: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    crate::spawn(async move {
        let mut builder = connection.open_table(table_name);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if index_cache_size > 0 {
            builder = builder.index_cache_size(index_cache_size);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_empty_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    schema_cdata: *mut arrow_schema::ffi::FFI_ArrowSchema,
    mode: *const c_char,
    storage_options_json: *const c_char,
    location: *const c_char,
    namespace_json: *const c_char,
    exist_ok: bool,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);

    let schema = if schema_cdata.is_null() {
        ffi::minimal_schema()
    } else {
        match ffi::import_schema(schema_cdata) {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        }
    };

    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);

    let create_mode = if exist_ok {
        CreateTableMode::exist_ok(|req| req)
    } else if !mode.is_null() {
        match ffi::to_string(mode).as_str() {
            "overwrite" => CreateTableMode::Overwrite,
            _ => CreateTableMode::Create,
        }
    } else {
        CreateTableMode::Create
    };

    crate::spawn(async move {
        let mut builder = connection
            .create_empty_table(table_name, schema)
            .mode(create_mode);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    arrays: *mut arrow_data::ffi::FFI_ArrowArray,
    schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
    batch_count: usize,
    mode: *const c_char,
    storage_options_json: *const c_char,
    location: *const c_char,
    namespace_json: *const c_char,
    exist_ok: bool,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);

    let (batches, schema_ref) = match ffi::import_batches(arrays, schema, batch_count) {
        Ok(r) => r,
        Err(e) => {
            callback_error(completion, e);
            return;
        }
    };

    let create_mode = if exist_ok {
        CreateTableMode::exist_ok(|req| req)
    } else if mode.is_null() {
        CreateTableMode::Create
    } else {
        match ffi::to_string(mode).as_str() {
            "overwrite" => CreateTableMode::Overwrite,
            _ => CreateTableMode::Create,
        }
    };

    crate::spawn(async move {
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema_ref,
        );

        let mut builder = connection
            .create_table(table_name, reader)
            .mode(create_mode);

        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }

        match builder.execute().await {
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
    start_after: *const c_char,
    limit: u32,
    namespace_json: *const c_char,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    let start_after_str = ffi::parse_optional_string(start_after);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);
    crate::spawn(async move {
        let mut builder = connection.table_names();
        if let Some(sa) = start_after_str {
            builder = builder.start_after(sa);
        }
        if limit > 0 {
            builder = builder.limit(limit);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
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
    crate::spawn(async move {
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
    crate::spawn(async move {
        match connection.drop_all_tables(&[]).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}
