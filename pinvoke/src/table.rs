use lancedb::index::Index as LanceIndex;
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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

/// Checks out the latest version of the table.
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn table_create_index(
    table_ptr: *const Table,
    columns_json: *const c_char,
    index_type: *const c_char,
    config_json: *const c_char,
    replace: bool,
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
        match table
            .create_index(&col_refs, index)
            .replace(replace)
            .execute()
            .await
        {
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
    match s.to_lowercase().as_str() {
        "l2" => Ok(lancedb::DistanceType::L2),
        "cosine" => Ok(lancedb::DistanceType::Cosine),
        "dot" => Ok(lancedb::DistanceType::Dot),
        _ => Err(format!("Unknown distance type: {}", s)),
    }
}

/// Returns the table's indices as a JSON string.
/// Caller must free the returned string with free_string().
#[no_mangle]
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
