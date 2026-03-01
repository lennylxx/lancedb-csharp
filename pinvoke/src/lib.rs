use std::sync::LazyLock;
use tokio::runtime::Runtime;

#[macro_use]
mod macros;
pub mod ffi;
mod connection;
mod query;
mod table;

// Re-export FFI functions for integration tests
pub use connection::{
    connection_clone_table, connection_close, connection_connect, connection_connect_namespace,
    connection_create_empty_table, connection_create_namespace, connection_create_table,
    connection_describe_namespace, connection_drop_all_tables, connection_drop_namespace,
    connection_drop_table, connection_list_namespaces, connection_list_tables,
    connection_open_table, connection_rename_table, connection_table_names,
};
pub use query::{
    query_analyze_plan, query_execute, query_execute_stream, query_explain_plan,
    query_output_schema, stream_close, stream_next, vector_query_analyze_plan,
    vector_query_execute, vector_query_execute_stream, vector_query_explain_plan,
    vector_query_output_schema,
};
pub use table::{
    table_add, table_add_columns, table_add_columns_null, table_alter_columns,
    table_checkout, table_checkout_latest, table_checkout_tag, table_close, table_count_rows,
    table_create_index, table_delete, table_drop_columns, table_drop_index, table_get_name,
    table_index_stats, table_index_stats_free, table_is_open, table_list_indices,
    table_list_versions, table_merge_insert, table_optimize, table_prewarm_index, table_restore,
    table_schema, table_stats, table_stats_free, table_tags_create, table_tags_delete,
    table_tags_get_version, table_tags_list, table_tags_update, table_take_offsets,
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

