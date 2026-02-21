use lancedb::index::Index as LanceIndex;
use lancedb::query::{ExecutableQuery, Query};
use lancedb::table::{ColumnAlteration, NewColumnTransform, OptimizeAction, Table};
use libc::c_char;
use std::ffi::CString;
use std::sync::Arc;

use crate::FfiCallback;

/// Returns the name of the table as a C string. Caller must free with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_get_name(table_ptr: *const Table) -> *mut c_char {
    let table = ffi_borrow!(table_ptr, Table);
    let name = table.name();
    let c_str_name = CString::new(name).unwrap();
    c_str_name.into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn table_is_open(table_ptr: *const Table) -> bool {
    !table_ptr.is_null()
}

/// Creates a new Query from the table.
#[unsafe(no_mangle)]
pub extern "C" fn table_create_query(table_ptr: *const Table) -> *const Query {
    let table = ffi_borrow!(table_ptr, Table);
    let query = table.query().clone();
    Arc::into_raw(Arc::new(query))
}

/// Counts the number of rows in the table, optionally filtered by a SQL predicate.
/// Returns the count as a pointer-sized integer via the callback.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub extern "C" fn table_schema(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.schema().await {
            Ok(schema) => match lancedb::ipc::schema_to_ipc_file(&schema) {
                Ok(ipc_bytes) => {
                    let ffi_bytes = Box::new(FfiBytes {
                        data: ipc_bytes.as_ptr(),
                        len: ipc_bytes.len(),
                        _owner: ipc_bytes,
                    });
                    let ptr = Box::into_raw(ffi_bytes);
                    completion(ptr as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Adds data to the table from Arrow IPC file bytes.
/// mode is "append" (default) or "overwrite" (null = "append").
#[unsafe(no_mangle)]
pub extern "C" fn table_add(
    table_ptr: *const Table,
    ipc_data: *const u8,
    ipc_len: usize,
    mode: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);

    let ipc_bytes = unsafe { std::slice::from_raw_parts(ipc_data, ipc_len) }.to_vec();

    let add_mode = if mode.is_null() {
        lancedb::table::AddDataMode::Append
    } else {
        let mode_str = crate::ffi::to_string(mode);
        match mode_str.as_str() {
            "overwrite" => lancedb::table::AddDataMode::Overwrite,
            _ => lancedb::table::AddDataMode::Append,
        }
    };

    crate::RUNTIME.spawn(async move {
        let reader = match lancedb::ipc::ipc_file_to_batches(ipc_bytes) {
            Ok(r) => r,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        match table.add(reader).mode(add_mode).execute().await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Returns the current version of the table as a u64 via the callback.
#[unsafe(no_mangle)]
pub extern "C" fn table_version(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.version().await {
            Ok(version) => {
                completion(version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Returns the table versions as a JSON string.
/// Caller must free the returned string with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_list_versions(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.list_versions().await {
            Ok(versions) => {
                let json_versions: Vec<serde_json::Value> = versions
                    .iter()
                    .map(|v| {
                        serde_json::json!({
                            "version": v.version,
                            "timestamp": v.timestamp.to_rfc3339(),
                            "metadata": v.metadata,
                        })
                    })
                    .collect();
                let json = serde_json::to_string(&json_versions).unwrap_or_default();
                let c_str = CString::new(json).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Checks out a specific version of the table.
#[unsafe(no_mangle)]
pub extern "C" fn table_checkout(
    table_ptr: *const Table,
    version: u64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.checkout(version).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Checks out a specific version of the table by tag name.
#[unsafe(no_mangle)]
pub extern "C" fn table_checkout_tag(
    table_ptr: *const Table,
    tag: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let tag = crate::ffi::to_string(tag);
    crate::RUNTIME.spawn(async move {
        match table.checkout_tag(&tag).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Checks out the latest version of the table.
#[unsafe(no_mangle)]
pub extern "C" fn table_checkout_latest(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.checkout_latest().await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Restores the table to the currently checked out version.
#[unsafe(no_mangle)]
pub extern "C" fn table_restore(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.restore().await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Returns the table's storage URI as a C string.
/// Caller must free the returned string with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_uri(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.uri().await {
            Ok(uri) => {
                let c_str = CString::new(uri).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Creates an index on the table.
/// columns_json: JSON array of column names, e.g. '["vector"]'.
/// index_type: one of "BTree", "Bitmap", "LabelList", "FTS", "IvfPq", "HnswPq", "HnswSq".
/// config_json: JSON object with index-specific parameters (can be null for defaults).
/// replace: whether to replace an existing index on the same columns.
/// name: optional custom index name (null for auto-generated).
/// train: whether to train the index with existing data.
#[unsafe(no_mangle)]
pub extern "C" fn table_create_index(
    table_ptr: *const Table,
    columns_json: *const c_char,
    index_type: *const c_char,
    config_json: *const c_char,
    replace: bool,
    name: *const c_char,
    train: bool,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let columns_str = crate::ffi::to_string(columns_json);
    let index_type_str = crate::ffi::to_string(index_type);
    let config_str = if config_json.is_null() {
        "{}".to_string()
    } else {
        crate::ffi::to_string(config_json)
    };
    let index_name = if name.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(name))
    };

    crate::RUNTIME.spawn(async move {
        let columns: Vec<String> = match serde_json::from_str(&columns_str) {
            Ok(c) => c,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let config: serde_json::Value = match serde_json::from_str(&config_str) {
            Ok(c) => c,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let index = match build_index(&index_type_str, &config) {
            Ok(idx) => idx,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let mut builder = table
            .create_index(&col_refs, index)
            .replace(replace)
            .train(train);
        if let Some(n) = index_name {
            builder = builder.name(n);
        }
        match builder.execute().await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

fn build_index(index_type: &str, config: &serde_json::Value) -> Result<LanceIndex, String> {
    use lancedb::index::scalar::*;
    use lancedb::index::vector::*;

    match index_type {
        "BTree" => Ok(LanceIndex::BTree(BTreeIndexBuilder::default())),
        "Bitmap" => Ok(LanceIndex::Bitmap(BitmapIndexBuilder::default())),
        "LabelList" => Ok(LanceIndex::LabelList(LabelListIndexBuilder::default())),
        "FTS" => {
            let mut builder = FtsIndexBuilder::default();
            if let Some(v) = config.get("with_position").and_then(|v| v.as_bool()) {
                builder = builder.with_position(v);
            }
            if let Some(v) = config.get("base_tokenizer").and_then(|v| v.as_str()) {
                builder = builder.base_tokenizer(v.to_string());
            }
            if let Some(v) = config.get("language").and_then(|v| v.as_str()) {
                builder = builder.language(v).map_err(|e| e.to_string())?;
            }
            if let Some(v) = config.get("max_token_length").and_then(|v| v.as_u64()) {
                builder = builder.max_token_length(Some(v as usize));
            }
            if let Some(v) = config.get("lower_case").and_then(|v| v.as_bool()) {
                builder = builder.lower_case(v);
            }
            if let Some(v) = config.get("stem").and_then(|v| v.as_bool()) {
                builder = builder.stem(v);
            }
            if let Some(v) = config.get("remove_stop_words").and_then(|v| v.as_bool()) {
                builder = builder.remove_stop_words(v);
            }
            if let Some(v) = config.get("ascii_folding").and_then(|v| v.as_bool()) {
                builder = builder.ascii_folding(v);
            }
            if let Some(v) = config.get("ngram_min_length").and_then(|v| v.as_u64()) {
                builder = builder.ngram_min_length(v as u32);
            }
            if let Some(v) = config.get("ngram_max_length").and_then(|v| v.as_u64()) {
                builder = builder.ngram_max_length(v as u32);
            }
            if let Some(v) = config.get("prefix_only").and_then(|v| v.as_bool()) {
                builder = builder.ngram_prefix_only(v);
            }
            Ok(LanceIndex::FTS(builder))
        }
        "IvfPq" => {
            let mut builder = IvfPqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_str()) {
                builder = builder.distance_type(parse_distance_type(v)?);
            }
            if let Some(v) = config.get("num_partitions").and_then(|v| v.as_u64()) {
                builder = builder.num_partitions(v as u32);
            }
            if let Some(v) = config.get("num_sub_vectors").and_then(|v| v.as_u64()) {
                builder = builder.num_sub_vectors(v as u32);
            }
            if let Some(v) = config.get("num_bits").and_then(|v| v.as_u64()) {
                builder = builder.num_bits(v as u32);
            }
            if let Some(v) = config.get("max_iterations").and_then(|v| v.as_u64()) {
                builder = builder.max_iterations(v as u32);
            }
            if let Some(v) = config.get("sample_rate").and_then(|v| v.as_u64()) {
                builder = builder.sample_rate(v as u32);
            }
            if let Some(v) = config.get("target_partition_size").and_then(|v| v.as_u64()) {
                builder = builder.target_partition_size(v as u32);
            }
            Ok(LanceIndex::IvfPq(builder))
        }
        "HnswPq" => {
            let mut builder = IvfHnswPqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_str()) {
                builder = builder.distance_type(parse_distance_type(v)?);
            }
            if let Some(v) = config.get("num_partitions").and_then(|v| v.as_u64()) {
                builder = builder.num_partitions(v as u32);
            }
            if let Some(v) = config.get("num_sub_vectors").and_then(|v| v.as_u64()) {
                builder = builder.num_sub_vectors(v as u32);
            }
            if let Some(v) = config.get("num_bits").and_then(|v| v.as_u64()) {
                builder = builder.num_bits(v as u32);
            }
            if let Some(v) = config.get("max_iterations").and_then(|v| v.as_u64()) {
                builder = builder.max_iterations(v as u32);
            }
            if let Some(v) = config.get("sample_rate").and_then(|v| v.as_u64()) {
                builder = builder.sample_rate(v as u32);
            }
            if let Some(v) = config.get("num_edges").and_then(|v| v.as_u64()) {
                builder = builder.num_edges(v as u32);
            }
            if let Some(v) = config.get("ef_construction").and_then(|v| v.as_u64()) {
                builder = builder.ef_construction(v as u32);
            }
            if let Some(v) = config.get("target_partition_size").and_then(|v| v.as_u64()) {
                builder = builder.target_partition_size(v as u32);
            }
            Ok(LanceIndex::IvfHnswPq(builder))
        }
        "HnswSq" => {
            let mut builder = IvfHnswSqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_str()) {
                builder = builder.distance_type(parse_distance_type(v)?);
            }
            if let Some(v) = config.get("num_partitions").and_then(|v| v.as_u64()) {
                builder = builder.num_partitions(v as u32);
            }
            if let Some(v) = config.get("max_iterations").and_then(|v| v.as_u64()) {
                builder = builder.max_iterations(v as u32);
            }
            if let Some(v) = config.get("sample_rate").and_then(|v| v.as_u64()) {
                builder = builder.sample_rate(v as u32);
            }
            if let Some(v) = config.get("num_edges").and_then(|v| v.as_u64()) {
                builder = builder.num_edges(v as u32);
            }
            if let Some(v) = config.get("ef_construction").and_then(|v| v.as_u64()) {
                builder = builder.ef_construction(v as u32);
            }
            if let Some(v) = config.get("target_partition_size").and_then(|v| v.as_u64()) {
                builder = builder.target_partition_size(v as u32);
            }
            Ok(LanceIndex::IvfHnswSq(builder))
        }
        _ => Err(format!("Unknown index type: {}", index_type)),
    }
}

fn parse_distance_type(s: &str) -> Result<lancedb::DistanceType, String> {
    crate::ffi::parse_distance_type(s)
}

/// Returns the table's indices as a JSON string.
/// Caller must free the returned string with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_list_indices(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.list_indices().await {
            Ok(indices) => {
                let json_indices: Vec<serde_json::Value> = indices
                    .iter()
                    .map(|idx| {
                        serde_json::json!({
                            "name": idx.name,
                            "index_type": idx.index_type.to_string(),
                            "columns": idx.columns,
                        })
                    })
                    .collect();
                let json = serde_json::to_string(&json_indices).unwrap_or_default();
                let c_str = CString::new(json).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Add new columns to the table using SQL expressions.
/// transforms_json is a JSON array of [name, expression] pairs, e.g. [["doubled","id * 2"]].
#[unsafe(no_mangle)]
pub extern "C" fn table_add_columns(
    table_ptr: *const Table,
    transforms_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let transforms_str = crate::ffi::to_string(transforms_json);
    let pairs: Vec<(String, String)> = serde_json::from_str(&transforms_str).unwrap_or_default();

    crate::RUNTIME.spawn(async move {
        let transform = NewColumnTransform::SqlExpressions(pairs);
        match table.add_columns(transform, None).await {
            Ok(_) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Alter existing columns (rename, set nullable, cast type).
/// alterations_json is a JSON array of objects with "path", optional "rename",
/// optional "nullable", optional "data_type" (as Arrow DataType string).
#[unsafe(no_mangle)]
pub extern "C" fn table_alter_columns(
    table_ptr: *const Table,
    alterations_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let json_str = crate::ffi::to_string(alterations_json);
    let raw: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap_or_default();

    let alterations: Vec<ColumnAlteration> = raw
        .iter()
        .map(|v| {
            let path = v["path"].as_str().unwrap_or_default().to_string();
            let mut alt = ColumnAlteration::new(path);
            if let Some(name) = v.get("rename").and_then(|n| n.as_str()) {
                alt.rename = Some(name.to_string());
            }
            if let Some(nullable) = v.get("nullable").and_then(|n| n.as_bool()) {
                alt.nullable = Some(nullable);
            }
            alt
        })
        .collect();

    crate::RUNTIME.spawn(async move {
        match table.alter_columns(&alterations).await {
            Ok(_) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Drop columns from the table.
/// columns_json is a JSON array of column names, e.g. ["col1","col2"].
#[unsafe(no_mangle)]
pub extern "C" fn table_drop_columns(
    table_ptr: *const Table,
    columns_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let json_str = crate::ffi::to_string(columns_json);
    let columns: Vec<String> = serde_json::from_str(&json_str).unwrap_or_default();

    crate::RUNTIME.spawn(async move {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        match table.drop_columns(&col_refs).await {
            Ok(_) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Optimize the on-disk data and indices for better performance.
/// cleanup_older_than_ms: if >= 0, prune versions older than this many milliseconds.
///   If < 0, use default pruning behavior (keep all versions).
/// delete_unverified: whether to delete unverified files (files newer than 7 days).
///   Only meaningful when cleanup_older_than_ms >= 0.
#[unsafe(no_mangle)]
pub extern "C" fn table_optimize(
    table_ptr: *const Table,
    cleanup_older_than_ms: i64,
    delete_unverified: bool,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);

    crate::RUNTIME.spawn(async move {
        // Always run compaction first
        let compaction_stats = match table
            .optimize(OptimizeAction::Compact {
                options: Default::default(),
                remap_options: None,
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        // Run prune with optional parameters
        let older_than = if cleanup_older_than_ms >= 0 {
            Some(chrono::TimeDelta::milliseconds(cleanup_older_than_ms))
        } else {
            None
        };

        let prune_stats = match table
            .optimize(OptimizeAction::Prune {
                older_than,
                delete_unverified: Some(delete_unverified),
                error_if_tagged_old_versions: None,
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        // Run index optimization
        if let Err(e) = table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
        {
            crate::callback_error(completion, e);
            return;
        }

        let json = serde_json::json!({
            "compaction": compaction_stats.compaction.as_ref().map(|c| serde_json::json!({
                "fragments_removed": c.fragments_removed,
                "fragments_added": c.fragments_added,
                "files_removed": c.files_removed,
                "files_added": c.files_added,
            })),
            "prune": prune_stats.prune.as_ref().map(|p| serde_json::json!({
                "bytes_removed": p.bytes_removed,
                "old_versions": p.old_versions,
            })),
        });
        let c_str = CString::new(json.to_string()).unwrap_or_default();
        completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
    });
}

/// List all tags on the table. Returns a JSON object like {"tag_name": {"version": 1, "manifest_size": 100}}.
#[unsafe(no_mangle)]
pub extern "C" fn table_tags_list(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::RUNTIME.spawn(async move {
        match table.tags().await {
            Ok(tags) => match tags.list().await {
                Ok(tag_map) => {
                    let json_map: serde_json::Map<String, serde_json::Value> = tag_map
                        .into_iter()
                        .map(|(name, contents)| {
                            (name, serde_json::json!({
                                "version": contents.version,
                                "manifest_size": contents.manifest_size,
                            }))
                        })
                        .collect();
                    let json = serde_json::to_string(&json_map).unwrap_or_default();
                    let c_str = CString::new(json).unwrap_or_default();
                    completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Create a new tag for the given version.
#[unsafe(no_mangle)]
pub extern "C" fn table_tags_create(
    table_ptr: *const Table,
    tag: *const c_char,
    version: u64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let tag_name = crate::ffi::to_string(tag);
    crate::RUNTIME.spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.create(&tag_name, version).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Delete a tag from the table.
#[unsafe(no_mangle)]
pub extern "C" fn table_tags_delete(
    table_ptr: *const Table,
    tag: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let tag_name = crate::ffi::to_string(tag);
    crate::RUNTIME.spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.delete(&tag_name).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Update an existing tag to point to a new version.
#[unsafe(no_mangle)]
pub extern "C" fn table_tags_update(
    table_ptr: *const Table,
    tag: *const c_char,
    version: u64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let tag_name = crate::ffi::to_string(tag);
    crate::RUNTIME.spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.update(&tag_name, version).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Get the version number that a tag points to.
#[unsafe(no_mangle)]
pub extern "C" fn table_tags_get_version(
    table_ptr: *const Table,
    tag: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let tag_name = crate::ffi::to_string(tag);
    crate::RUNTIME.spawn(async move {
        match table.tags().await {
            Ok(tags) => match tags.get_version(&tag_name).await {
                Ok(version) => {
                    completion(version as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => crate::callback_error(completion, e),
            },
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Drop an index from the table by name.
#[unsafe(no_mangle)]
pub extern "C" fn table_drop_index(
    table_ptr: *const Table,
    name: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let index_name = crate::ffi::to_string(name);
    crate::RUNTIME.spawn(async move {
        match table.drop_index(&index_name).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Prewarm an index in the table.
#[unsafe(no_mangle)]
pub extern "C" fn table_prewarm_index(
    table_ptr: *const Table,
    name: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let index_name = crate::ffi::to_string(name);
    crate::RUNTIME.spawn(async move {
        match table.prewarm_index(&index_name).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Wait for indexing to complete for the given index names.
#[unsafe(no_mangle)]
pub extern "C" fn table_wait_for_index(
    table_ptr: *const Table,
    index_names_json: *const c_char,
    timeout_ms: i64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let json_str = crate::ffi::to_string(index_names_json);
    let names: Vec<String> = serde_json::from_str(&json_str).unwrap_or_default();
    let timeout = if timeout_ms > 0 {
        std::time::Duration::from_millis(timeout_ms as u64)
    } else {
        std::time::Duration::from_secs(300)
    };
    crate::RUNTIME.spawn(async move {
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        match table.wait_for_index(&name_refs, timeout).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Get statistics about an index. Returns a JSON string or null if the index doesn't exist.
#[unsafe(no_mangle)]
pub extern "C" fn table_index_stats(
    table_ptr: *const Table,
    index_name: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let name = crate::ffi::to_string(index_name);
    crate::RUNTIME.spawn(async move {
        match table.index_stats(&name).await {
            Ok(Some(stats)) => {
                let json = serde_json::json!({
                    "num_indexed_rows": stats.num_indexed_rows,
                    "num_unindexed_rows": stats.num_unindexed_rows,
                    "index_type": format!("{}", stats.index_type),
                    "distance_type": stats.distance_type.map(|d| format!("{}", d)),
                    "num_indices": stats.num_indices,
                });
                let json_str = serde_json::to_string(&json).unwrap_or_default();
                let c_str = CString::new(json_str).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Ok(None) => {
                completion(std::ptr::null(), std::ptr::null());
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
    pub(crate) _owner: Vec<u8>,
}

/// Frees an FfiBytes struct allocated by Rust.
#[unsafe(no_mangle)]
pub extern "C" fn free_ffi_bytes(ptr: *mut FfiBytes) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

/// Closes the table and frees the underlying Arc.
#[unsafe(no_mangle)]
pub extern "C" fn table_close(table_ptr: *const Table) {
    ffi_free!(table_ptr, Table);
}

/// Merge insert (upsert) operation on the table.
/// on_columns_json: JSON array of column names to match on, e.g. '["id"]'.
/// when_matched_update_all: if true, update matched rows.
/// when_matched_update_all_filter: optional SQL filter for matched update (null for none).
/// when_not_matched_insert_all: if true, insert non-matching source rows.
/// when_not_matched_by_source_delete: if true, delete target rows not in source.
/// when_not_matched_by_source_delete_filter: optional SQL filter for source delete (null for none).
/// ipc_data/ipc_len: Arrow IPC file bytes containing the new data.
#[unsafe(no_mangle)]
pub extern "C" fn table_merge_insert(
    table_ptr: *const Table,
    on_columns_json: *const c_char,
    when_matched_update_all: bool,
    when_matched_update_all_filter: *const c_char,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filter: *const c_char,
    ipc_data: *const u8,
    ipc_len: usize,
    use_index: bool,
    timeout_ms: i64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let on_columns_str = crate::ffi::to_string(on_columns_json);

    let matched_filter = if when_matched_update_all_filter.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(when_matched_update_all_filter))
    };

    let source_delete_filter = if when_not_matched_by_source_delete_filter.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(when_not_matched_by_source_delete_filter))
    };

    let ipc_bytes = unsafe { std::slice::from_raw_parts(ipc_data, ipc_len) }.to_vec();

    crate::RUNTIME.spawn(async move {
        let on_columns: Vec<String> = match serde_json::from_str(&on_columns_str) {
            Ok(c) => c,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let on_refs: Vec<&str> = on_columns.iter().map(|s| s.as_str()).collect();
        let mut builder = table.merge_insert(&on_refs);

        if when_matched_update_all {
            builder.when_matched_update_all(matched_filter);
        }
        if when_not_matched_insert_all {
            builder.when_not_matched_insert_all();
        }
        if when_not_matched_by_source_delete {
            builder.when_not_matched_by_source_delete(source_delete_filter);
        }

        builder.use_index(use_index);
        if timeout_ms >= 0 {
            builder.timeout(std::time::Duration::from_millis(timeout_ms as u64));
        }

        let reader = match lancedb::ipc::ipc_file_to_batches(ipc_bytes) {
            Ok(r) => r,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        match builder.execute(Box::new(reader)).await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => crate::callback_error(completion, e),
        }
    });
}

/// Takes rows by offset positions and returns Arrow IPC bytes.
/// offsets: pointer to array of u64 offset values.
/// offsets_len: number of offsets.
/// columns_json: optional JSON array of column names to select (null for all columns).
#[unsafe(no_mangle)]
pub extern "C" fn table_take_offsets(
    table_ptr: *const Table,
    offsets: *const u64,
    offsets_len: usize,
    columns_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let offset_vec: Vec<u64> = unsafe { std::slice::from_raw_parts(offsets, offsets_len) }.to_vec();
    let columns_str = if columns_json.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(columns_json))
    };

    crate::RUNTIME.spawn(async move {
        use futures::TryStreamExt;

        let mut query = table.take_offsets(offset_vec);
        if let Some(cols) = columns_str {
            let columns: Vec<String> = match serde_json::from_str(&cols) {
                Ok(c) => c,
                Err(e) => {
                    crate::callback_error(completion, e);
                    return;
                }
            };
            let col_tuples: Vec<(String, String)> =
                columns.iter().map(|c| (c.clone(), c.clone())).collect();
            query = query.select(lancedb::query::Select::dynamic(col_tuples.as_slice()));
        }

        let stream = match query.execute().await {
            Ok(s) => s,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let schema = stream.schema().clone();
        let batches: Vec<arrow_array::RecordBatch> = match stream.try_collect().await {
            Ok(b) => b,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let ipc_result = if batches.is_empty() {
            lancedb::ipc::schema_to_ipc_file(&schema)
        } else {
            lancedb::ipc::batches_to_ipc_file(&batches)
        };

        match ipc_result {
            Ok(ipc_bytes) => {
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

/// Takes rows by row IDs and returns Arrow IPC bytes.
/// row_ids: pointer to array of u64 row ID values.
/// row_ids_len: number of row IDs.
/// columns_json: optional JSON array of column names to select (null for all columns).
#[unsafe(no_mangle)]
pub extern "C" fn table_take_row_ids(
    table_ptr: *const Table,
    row_ids: *const u64,
    row_ids_len: usize,
    columns_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let id_vec: Vec<u64> = unsafe { std::slice::from_raw_parts(row_ids, row_ids_len) }.to_vec();
    let columns_str = if columns_json.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(columns_json))
    };

    crate::RUNTIME.spawn(async move {
        use futures::TryStreamExt;

        let mut query = table.take_row_ids(id_vec);
        if let Some(cols) = columns_str {
            let columns: Vec<String> = match serde_json::from_str(&cols) {
                Ok(c) => c,
                Err(e) => {
                    crate::callback_error(completion, e);
                    return;
                }
            };
            let col_tuples: Vec<(String, String)> =
                columns.iter().map(|c| (c.clone(), c.clone())).collect();
            query = query.select(lancedb::query::Select::dynamic(col_tuples.as_slice()));
        }

        let stream = match query.execute().await {
            Ok(s) => s,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let schema = stream.schema().clone();
        let batches: Vec<arrow_array::RecordBatch> = match stream.try_collect().await {
            Ok(b) => b,
            Err(e) => {
                crate::callback_error(completion, e);
                return;
            }
        };

        let ipc_result = if batches.is_empty() {
            lancedb::ipc::schema_to_ipc_file(&schema)
        } else {
            lancedb::ipc::batches_to_ipc_file(&batches)
        };

        match ipc_result {
            Ok(ipc_bytes) => {
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

#[unsafe(no_mangle)]
pub extern "C" fn free_string(c_string: *mut c_char) {
    unsafe {
        if c_string.is_null() {
            return;
        }
        drop(CString::from_raw(c_string))
    };
}
