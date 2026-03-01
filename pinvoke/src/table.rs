use lancedb::index::Index as LanceIndex;
use lancedb::query::ExecutableQuery;
use lancedb::table::{ColumnAlteration, NewColumnTransform, OptimizeAction, Table};
use libc::c_char;
use sonic_rs::JsonValueTrait;
use std::ffi::CString;

use crate::ffi::{callback_error, FfiCallback};
use crate::ffi;

/// C-compatible struct for update results, passed across FFI.
#[repr(C)]
pub struct FfiUpdateResult {
    pub rows_updated: u64,
    pub version: u64,
}

/// C-compatible struct for merge insert results, passed across FFI.
#[repr(C)]
pub struct FfiMergeResult {
    pub version: u64,
    pub num_inserted_rows: u64,
    pub num_updated_rows: u64,
    pub num_deleted_rows: u64,
    pub num_attempts: u32,
}

/// Returns the name of the table as a C string. Caller must free with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_get_name(table_ptr: *const Table) -> *mut c_char {
    let table = ffi_borrow!(table_ptr, Table);
    let name = table.name();
    let c_str_name = match CString::new(name) {
        Ok(s) => s,
        Err(e) => {
            ffi::set_last_error(e);
            return std::ptr::null_mut();
        }
    };
    c_str_name.into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn table_is_open(table_ptr: *const Table) -> bool {
    !table_ptr.is_null()
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
    crate::spawn(async move {
        match table.count_rows(filter).await {
            Ok(count) => {
                completion(count as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.delete(&predicate).await {
            Ok(result) => {
                completion(result.version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Updates rows in the table. column_sqlexprs_json is a JSON array of [name, expr] pairs.
/// filter is an optional SQL predicate (null for all rows).
#[unsafe(no_mangle)]
pub extern "C" fn table_update(
    table_ptr: *const Table,
    filter: *const c_char,
    column_sqlexprs_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let filter = if filter.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(filter))
    };
    let column_sqlexprs_str = crate::ffi::to_string(column_sqlexprs_json);

    crate::spawn(async move {
        let column_sqlexprs: Vec<(String, String)> = match sonic_rs::from_str(&column_sqlexprs_str) {
            Ok(c) => c,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let mut builder = table.update();
        if let Some(f) = filter {
            builder = builder.only_if(f);
        }
        for (column, expr) in column_sqlexprs {
            builder = builder.column(column, expr);
        }

        match builder.execute().await {
            Ok(result) => {
                let ffi = Box::new(FfiUpdateResult {
                    rows_updated: result.rows_updated,
                    version: result.version,
                });
                completion(Box::into_raw(ffi) as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Frees an FfiUpdateResult pointer returned by table_update.
#[unsafe(no_mangle)]
pub extern "C" fn table_update_result_free(ptr: *mut FfiUpdateResult) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

/// Returns the table's Arrow schema via the C Data Interface.
/// The callback receives a pointer to a heap-allocated FFI_ArrowSchema
/// (caller must free with free_ffi_schema).
#[unsafe(no_mangle)]
pub extern "C" fn table_schema(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::spawn(async move {
        match table.schema().await {
            Ok(schema) => {
                match arrow_schema::ffi::FFI_ArrowSchema::try_from(
                    arrow_schema::DataType::Struct(schema.fields().clone()),
                ) {
                    Ok(ffi_schema) => {
                        let ptr = Box::into_raw(Box::new(ffi_schema));
                        completion(ptr as *const std::ffi::c_void, std::ptr::null());
                    }
                    Err(e) => callback_error(completion, e),
                }
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Adds data to the table from Arrow C Data Interface arrays.
/// arrays: pointer to contiguous FFI_ArrowArray structs (one per batch).
/// schema: pointer to a single FFI_ArrowSchema shared by all batches.
/// batch_count: number of batches.
/// mode is "append" (default) or "overwrite" (null = "append").
#[unsafe(no_mangle)]
pub extern "C" fn table_add(
    table_ptr: *const Table,
    arrays: *mut arrow_data::ffi::FFI_ArrowArray,
    schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
    batch_count: usize,
    mode: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);

    let (batches, schema_ref) = match ffi::import_batches(arrays, schema, batch_count) {
        Ok(r) => r,
        Err(e) => {
            callback_error(completion, e);
            return;
        }
    };

    let add_mode = if mode.is_null() {
        lancedb::table::AddDataMode::Append
    } else {
        let mode_str = crate::ffi::to_string(mode);
        match mode_str.as_str() {
            "overwrite" => lancedb::table::AddDataMode::Overwrite,
            _ => lancedb::table::AddDataMode::Append,
        }
    };

    crate::spawn(async move {
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema_ref,
        );
        match table.add(reader).mode(add_mode).execute().await {
            Ok(result) => {
                completion(result.version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.version().await {
            Ok(version) => {
                completion(version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Returns whether the table uses V2 manifest paths (1 = true, 0 = false).
#[unsafe(no_mangle)]
pub extern "C" fn table_uses_v2_manifest_paths(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::spawn(async move {
        match table.as_native() {
            Some(native) => match native.uses_v2_manifest_paths().await {
                Ok(uses_v2) => {
                    completion(uses_v2 as usize as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => callback_error(completion, e),
            },
            None => callback_error(
                completion,
                lancedb::Error::NotSupported {
                    message: "uses_v2_manifest_paths is only supported for local tables".into(),
                },
            ),
        }
    });
}

/// Migrates the table to use V2 manifest paths.
#[unsafe(no_mangle)]
pub extern "C" fn table_migrate_manifest_paths_v2(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::spawn(async move {
        match table.as_native() {
            Some(native) => match native.migrate_manifest_paths_v2().await {
                Ok(()) => {
                    completion(1 as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => callback_error(completion, e),
            },
            None => callback_error(
                completion,
                lancedb::Error::NotSupported {
                    message: "migrate_manifest_paths_v2 is only supported for local tables".into(),
                },
            ),
        }
    });
}

/// Replaces the metadata of a field in the table schema.
/// field_name: UTF-8 field name.
/// metadata_json: JSON object with string key-value pairs.
#[unsafe(no_mangle)]
pub extern "C" fn table_replace_field_metadata(
    table_ptr: *const Table,
    field_name: *const c_char,
    metadata_json: *const c_char,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let field_name = ffi::to_string(field_name);
    let metadata: std::collections::HashMap<String, String> =
        match ffi::parse_optional_json_map(metadata_json) {
            Some(m) => m,
            None => {
                callback_error(
                    completion,
                    lancedb::Error::InvalidInput {
                        message: "metadata_json must be a valid JSON object".into(),
                    },
                );
                return;
            }
        };
    crate::spawn(async move {
        match table.as_native() {
            Some(native) => {
                let manifest = match native.manifest().await {
                    Ok(m) => m,
                    Err(e) => {
                        callback_error(completion, e);
                        return;
                    }
                };
                let field = match manifest.schema.field(&field_name) {
                    Some(f) => f,
                    None => {
                        callback_error(
                            completion,
                            lancedb::Error::InvalidInput {
                                message: format!("Field '{}' not found in schema", field_name),
                            },
                        );
                        return;
                    }
                };
                let field_id = field.id as u32;
                match native
                    .replace_field_metadata(vec![(field_id, metadata)])
                    .await
                {
                    Ok(()) => {
                        completion(1 as *const std::ffi::c_void, std::ptr::null());
                    }
                    Err(e) => callback_error(completion, e),
                }
            }
            None => callback_error(
                completion,
                lancedb::Error::NotSupported {
                    message: "replace_field_metadata is only supported for local tables".into(),
                },
            ),
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
    crate::spawn(async move {
        match table.list_versions().await {
            Ok(versions) => {
                let json_versions: Vec<sonic_rs::Value> = versions
                    .iter()
                    .map(|v| {
                        sonic_rs::json!({
                            "version": v.version,
                            "timestamp": v.timestamp.to_rfc3339(),
                            "metadata": v.metadata,
                        })
                    })
                    .collect();
                let json = sonic_rs::to_string(&json_versions).unwrap_or_default();
                let c_str = CString::new(json).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.checkout(version).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.checkout_tag(&tag).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.checkout_latest().await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.restore().await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.uri().await {
            Ok(uri) => {
                let c_str = CString::new(uri).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Creates an index on the table.
/// columns_json: JSON array of column names, e.g. '["vector"]'.
/// index_type: integer matching IndexType enum (0=IvfFlat, 1=IvfSq, ..., 9=FTS).
/// config_json: JSON object with index-specific parameters (can be null for defaults).
/// replace: whether to replace an existing index on the same columns.
/// name: optional custom index name (null for auto-generated).
/// train: whether to train the index with existing data.
#[unsafe(no_mangle)]
pub extern "C" fn table_create_index(
    table_ptr: *const Table,
    columns_json: *const c_char,
    index_type: i32,
    config_json: *const c_char,
    replace: bool,
    name: *const c_char,
    train: bool,
    wait_timeout_ms: i64,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let columns_str = crate::ffi::to_string(columns_json);
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

    crate::spawn(async move {
        let columns: Vec<String> = match sonic_rs::from_str(&columns_str) {
            Ok(c) => c,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let config: sonic_rs::Value = match sonic_rs::from_str(&config_str) {
            Ok(c) => c,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let index = match build_index(index_type, &config) {
            Ok(idx) => idx,
            Err(e) => {
                callback_error(completion, e);
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
        if wait_timeout_ms >= 0 {
            builder = builder.wait_timeout(std::time::Duration::from_millis(wait_timeout_ms as u64));
        }
        match builder.execute().await {
            Ok(_) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

fn build_index(index_type: i32, config: &sonic_rs::Value) -> Result<LanceIndex, String> {
    use lancedb::index::scalar::*;
    use lancedb::index::vector::*;
    use lancedb::index::IndexType;

    let idx_type = ffi::ffi_to_index_type(index_type)?;

    match idx_type {
        IndexType::BTree => Ok(LanceIndex::BTree(BTreeIndexBuilder::default())),
        IndexType::Bitmap => Ok(LanceIndex::Bitmap(BitmapIndexBuilder::default())),
        IndexType::LabelList => Ok(LanceIndex::LabelList(LabelListIndexBuilder::default())),
        IndexType::FTS => {
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
        IndexType::IvfPq => {
            let mut builder = IvfPqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
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
        IndexType::IvfHnswPq => {
            let mut builder = IvfHnswPqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
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
        IndexType::IvfHnswSq => {
            let mut builder = IvfHnswSqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
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
        IndexType::IvfFlat => {
            let mut builder = IvfFlatIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
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
            if let Some(v) = config.get("target_partition_size").and_then(|v| v.as_u64()) {
                builder = builder.target_partition_size(v as u32);
            }
            Ok(LanceIndex::IvfFlat(builder))
        }
        IndexType::IvfSq => {
            let mut builder = IvfSqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
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
            if let Some(v) = config.get("target_partition_size").and_then(|v| v.as_u64()) {
                builder = builder.target_partition_size(v as u32);
            }
            Ok(LanceIndex::IvfSq(builder))
        }
        IndexType::IvfRq => {
            let mut builder = IvfRqIndexBuilder::default();
            if let Some(v) = config.get("distance_type").and_then(|v| v.as_i64()) {
                builder = builder.distance_type(ffi::ffi_to_distance_type(v as i32)?);
            }
            if let Some(v) = config.get("num_partitions").and_then(|v| v.as_u64()) {
                builder = builder.num_partitions(v as u32);
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
            Ok(LanceIndex::IvfRq(builder))
        }
    }
}

/// Returns the table's indices as a JSON string.
/// Caller must free the returned string with free_string().
#[unsafe(no_mangle)]
pub extern "C" fn table_list_indices(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::spawn(async move {
        match table.list_indices().await {
            Ok(indices) => {
                let json_indices: Vec<sonic_rs::Value> = indices
                    .iter()
                    .map(|idx| {
                        sonic_rs::json!({
                            "name": idx.name,
                            "index_type": ffi::index_type_to_ffi(&idx.index_type),
                            "columns": idx.columns,
                        })
                    })
                    .collect();
                let json = sonic_rs::to_string(&json_indices).unwrap_or_default();
                let c_str = CString::new(json).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    let pairs: Vec<(String, String)> = sonic_rs::from_str(&transforms_str).unwrap_or_default();

    crate::spawn(async move {
        let transform = NewColumnTransform::SqlExpressions(pairs);
        match table.add_columns(transform, None).await {
            Ok(result) => {
                completion(result.version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    let raw: Vec<sonic_rs::Value> = sonic_rs::from_str(&json_str).unwrap_or_default();

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

    crate::spawn(async move {
        match table.alter_columns(&alterations).await {
            Ok(result) => {
                completion(result.version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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
    let columns: Vec<String> = sonic_rs::from_str(&json_str).unwrap_or_default();

    crate::spawn(async move {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        match table.drop_columns(&col_refs).await {
            Ok(result) => {
                completion(result.version as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
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

    crate::spawn(async move {
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
                callback_error(completion, e);
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
                callback_error(completion, e);
                return;
            }
        };

        // Run index optimization
        if let Err(e) = table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
        {
            callback_error(completion, e);
            return;
        }

        let json = sonic_rs::json!({
            "compaction": compaction_stats.compaction.as_ref().map(|c| sonic_rs::json!({
                "fragments_removed": c.fragments_removed,
                "fragments_added": c.fragments_added,
                "files_removed": c.files_removed,
                "files_added": c.files_added,
            })),
            "prune": prune_stats.prune.as_ref().map(|p| sonic_rs::json!({
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
    crate::spawn(async move {
        match table.tags().await {
            Ok(tags) => match tags.list().await {
                Ok(tag_map) => {
                    let mut json_map = sonic_rs::Object::new();
                    for (name, contents) in tag_map {
                        json_map.insert(&name, sonic_rs::json!({
                            "version": contents.version,
                            "manifest_size": contents.manifest_size,
                        }));
                    }
                    let json = sonic_rs::to_string(&json_map).unwrap_or_default();
                    let c_str = CString::new(json).unwrap_or_default();
                    completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => callback_error(completion, e),
            },
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.create(&tag_name, version).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => callback_error(completion, e),
            },
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.delete(&tag_name).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => callback_error(completion, e),
            },
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.tags().await {
            Ok(mut tags) => match tags.update(&tag_name, version).await {
                Ok(()) => completion(std::ptr::null(), std::ptr::null()),
                Err(e) => callback_error(completion, e),
            },
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.tags().await {
            Ok(tags) => match tags.get_version(&tag_name).await {
                Ok(version) => {
                    completion(version as *const std::ffi::c_void, std::ptr::null());
                }
                Err(e) => callback_error(completion, e),
            },
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.drop_index(&index_name).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => callback_error(completion, e),
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
    crate::spawn(async move {
        match table.prewarm_index(&index_name).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => callback_error(completion, e),
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
    let names: Vec<String> = sonic_rs::from_str(&json_str).unwrap_or_default();
    let timeout = if timeout_ms > 0 {
        std::time::Duration::from_millis(timeout_ms as u64)
    } else {
        std::time::Duration::from_secs(300)
    };
    crate::spawn(async move {
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        match table.wait_for_index(&name_refs, timeout).await {
            Ok(()) => completion(std::ptr::null(), std::ptr::null()),
            Err(e) => callback_error(completion, e),
        }
    });
}

/// C-compatible struct for index statistics, passed across FFI without JSON.
#[repr(C)]
pub struct FfiIndexStats {
    pub num_indexed_rows: u64,
    pub num_unindexed_rows: u64,
    /// Maps to IndexType enum: 0=IvfFlat, 1=IvfSq, 2=IvfPq, 3=IvfRq,
    /// 4=IvfHnswPq, 5=IvfHnswSq, 6=BTree, 7=Bitmap, 8=LabelList, 9=FTS
    pub index_type: i32,
    /// Maps to DistanceType enum: 0=L2, 1=Cosine, 2=Dot, 3=Hamming, -1=None
    pub distance_type: i32,
    /// Number of index parts. 0 if not available.
    pub num_indices: u32,
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
    crate::spawn(async move {
        match table.index_stats(&name).await {
            Ok(Some(stats)) => {
                let ffi_stats = Box::new(FfiIndexStats {
                    num_indexed_rows: stats.num_indexed_rows as u64,
                    num_unindexed_rows: stats.num_unindexed_rows as u64,
                    index_type: ffi::index_type_to_ffi(&stats.index_type),
                    distance_type: ffi::distance_type_to_ffi(stats.distance_type),
                    num_indices: stats.num_indices.unwrap_or(0),
                });
                let ptr = Box::into_raw(ffi_stats);
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Ok(None) => {
                completion(std::ptr::null(), std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Frees an FfiIndexStats pointer returned by table_index_stats.
#[unsafe(no_mangle)]
pub extern "C" fn table_index_stats_free(ptr: *mut FfiIndexStats) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

/// Closes the table and frees the underlying Arc.
#[unsafe(no_mangle)]
pub extern "C" fn table_close(table_ptr: *const Table) {
    ffi_free!(table_ptr, Table);
}

/// C-compatible struct for fragment row count summary statistics.
#[repr(C)]
pub struct FfiFragmentSummaryStats {
    pub min: u64,
    pub max: u64,
    pub mean: u64,
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p99: u64,
}

/// C-compatible struct for fragment-level statistics.
#[repr(C)]
pub struct FfiFragmentStats {
    pub num_fragments: u64,
    pub num_small_fragments: u64,
    pub lengths: FfiFragmentSummaryStats,
}

/// C-compatible struct for table statistics, passed across FFI without JSON overhead.
#[repr(C)]
pub struct FfiTableStats {
    pub total_bytes: u64,
    pub num_rows: u64,
    pub num_indices: u64,
    pub fragment_stats: FfiFragmentStats,
}

/// Returns table statistics as a heap-allocated FfiTableStats struct.
/// Caller must free the returned pointer with table_stats_free.
#[unsafe(no_mangle)]
pub extern "C" fn table_stats(
    table_ptr: *const Table,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    crate::spawn(async move {
        match table.stats().await {
            Ok(stats) => {
                let ffi_stats = Box::new(FfiTableStats {
                    total_bytes: stats.total_bytes as u64,
                    num_rows: stats.num_rows as u64,
                    num_indices: stats.num_indices as u64,
                    fragment_stats: FfiFragmentStats {
                        num_fragments: stats.fragment_stats.num_fragments as u64,
                        num_small_fragments: stats.fragment_stats.num_small_fragments as u64,
                        lengths: FfiFragmentSummaryStats {
                            min: stats.fragment_stats.lengths.min as u64,
                            max: stats.fragment_stats.lengths.max as u64,
                            mean: stats.fragment_stats.lengths.mean as u64,
                            p25: stats.fragment_stats.lengths.p25 as u64,
                            p50: stats.fragment_stats.lengths.p50 as u64,
                            p75: stats.fragment_stats.lengths.p75 as u64,
                            p99: stats.fragment_stats.lengths.p99 as u64,
                        },
                    },
                });
                let ptr = Box::into_raw(ffi_stats);
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Frees an FfiTableStats pointer returned by table_stats.
#[unsafe(no_mangle)]
pub extern "C" fn table_stats_free(ptr: *mut FfiTableStats) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

/// Merge insert (upsert) operation on the table.
/// on_columns_json: JSON array of column names to match on, e.g. '["id"]'.
/// when_matched_update_all: if true, update matched rows.
/// when_matched_update_all_filter: optional SQL filter for matched update (null for none).
/// when_not_matched_insert_all: if true, insert non-matching source rows.
/// when_not_matched_by_source_delete: if true, delete target rows not in source.
/// when_not_matched_by_source_delete_filter: optional SQL filter for source delete (null for none).
/// arrays/schema/batch_count: Arrow C Data Interface arrays containing the new data.
#[unsafe(no_mangle)]
pub extern "C" fn table_merge_insert(
    table_ptr: *const Table,
    on_columns_json: *const c_char,
    when_matched_update_all: bool,
    when_matched_update_all_filter: *const c_char,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filter: *const c_char,
    arrays: *mut arrow_data::ffi::FFI_ArrowArray,
    schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
    batch_count: usize,
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

    let (batches, schema_ref) = match ffi::import_batches(arrays, schema, batch_count) {
        Ok(r) => r,
        Err(e) => {
            callback_error(completion, e);
            return;
        }
    };

    crate::spawn(async move {
        let on_columns: Vec<String> = match sonic_rs::from_str(&on_columns_str) {
            Ok(c) => c,
            Err(e) => {
                callback_error(completion, e);
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

        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema_ref,
        );

        match builder.execute(Box::new(reader)).await {
            Ok(result) => {
                let ffi = Box::new(FfiMergeResult {
                    version: result.version,
                    num_inserted_rows: result.num_inserted_rows,
                    num_updated_rows: result.num_updated_rows,
                    num_deleted_rows: result.num_deleted_rows,
                    num_attempts: result.num_attempts,
                });
                completion(Box::into_raw(ffi) as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

/// Frees an FfiMergeResult pointer returned by table_merge_insert.
#[unsafe(no_mangle)]
pub extern "C" fn table_merge_result_free(ptr: *mut FfiMergeResult) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

/// Takes rows by offset positions and returns results via Arrow C Data Interface.
/// offsets: pointer to array of u64 offset values.
/// offsets_len: number of offsets.
/// columns_json: optional JSON array of column names to select (null for all columns).
/// with_row_id: whether to include the _rowid column in results.
#[unsafe(no_mangle)]
pub extern "C" fn table_take_offsets(
    table_ptr: *const Table,
    offsets: *const u64,
    offsets_len: usize,
    columns_json: *const c_char,
    with_row_id: bool,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let offset_vec: Vec<u64> = unsafe { std::slice::from_raw_parts(offsets, offsets_len) }.to_vec();
    let columns_str = if columns_json.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(columns_json))
    };

    crate::spawn(async move {
        use arrow_array::Array;
        use arrow_array::StructArray;
        use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        use futures::TryStreamExt;

        let mut query = table.take_offsets(offset_vec);
        if let Some(cols) = columns_str {
            let columns: Vec<String> = match sonic_rs::from_str(&cols) {
                Ok(c) => c,
                Err(e) => {
                    callback_error(completion, e);
                    return;
                }
            };
            let col_tuples: Vec<(String, String)> =
                columns.iter().map(|c| (c.clone(), c.clone())).collect();
            query = query.select(lancedb::query::Select::dynamic(col_tuples.as_slice()));
        }
        if with_row_id {
            query = query.with_row_id();
        }

        let stream = match query.execute().await {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let schema = stream.schema().clone();
        let batches: Vec<arrow_array::RecordBatch> = match stream.try_collect().await {
            Ok(b) => b,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let batch = if batches.is_empty() {
            arrow_array::RecordBatch::new_empty(schema)
        } else if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            match arrow_select::concat::concat_batches(&schema, &batches) {
                Ok(b) => b,
                Err(e) => {
                    callback_error(completion, e);
                    return;
                }
            }
        };

        let struct_array: StructArray = batch.into();
        let data = struct_array.to_data();

        let ffi_array = FFI_ArrowArray::new(&data);
        let ffi_schema = match FFI_ArrowSchema::try_from(data.data_type()) {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let cdata = Box::new(crate::ffi::FfiCData {
            array: Box::into_raw(Box::new(ffi_array)),
            schema: Box::into_raw(Box::new(ffi_schema)),
        });
        let ptr = Box::into_raw(cdata);
        completion(ptr as *const std::ffi::c_void, std::ptr::null());
    });
}

/// Takes rows by row IDs and returns results via Arrow C Data Interface.
/// row_ids: pointer to array of u64 row ID values.
/// row_ids_len: number of row IDs.
/// columns_json: optional JSON array of column names to select (null for all columns).
/// with_row_id: whether to include the _rowid column in results.
#[unsafe(no_mangle)]
pub extern "C" fn table_take_row_ids(
    table_ptr: *const Table,
    row_ids: *const u64,
    row_ids_len: usize,
    columns_json: *const c_char,
    with_row_id: bool,
    completion: FfiCallback,
) {
    let table = ffi_clone_arc!(table_ptr, Table);
    let id_vec: Vec<u64> = unsafe { std::slice::from_raw_parts(row_ids, row_ids_len) }.to_vec();
    let columns_str = if columns_json.is_null() {
        None
    } else {
        Some(crate::ffi::to_string(columns_json))
    };

    crate::spawn(async move {
        use arrow_array::Array;
        use arrow_array::StructArray;
        use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        use futures::TryStreamExt;

        let mut query = table.take_row_ids(id_vec);
        if let Some(cols) = columns_str {
            let columns: Vec<String> = match sonic_rs::from_str(&cols) {
                Ok(c) => c,
                Err(e) => {
                    callback_error(completion, e);
                    return;
                }
            };
            let col_tuples: Vec<(String, String)> =
                columns.iter().map(|c| (c.clone(), c.clone())).collect();
            query = query.select(lancedb::query::Select::dynamic(col_tuples.as_slice()));
        }
        if with_row_id {
            query = query.with_row_id();
        }

        let stream = match query.execute().await {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let schema = stream.schema().clone();
        let batches: Vec<arrow_array::RecordBatch> = match stream.try_collect().await {
            Ok(b) => b,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let batch = if batches.is_empty() {
            arrow_array::RecordBatch::new_empty(schema)
        } else if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            match arrow_select::concat::concat_batches(&schema, &batches) {
                Ok(b) => b,
                Err(e) => {
                    callback_error(completion, e);
                    return;
                }
            }
        };

        let struct_array: StructArray = batch.into();
        let data = struct_array.to_data();

        let ffi_array = FFI_ArrowArray::new(&data);
        let ffi_schema = match FFI_ArrowSchema::try_from(data.data_type()) {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        };

        let cdata = Box::new(crate::ffi::FfiCData {
            array: Box::into_raw(Box::new(ffi_array)),
            schema: Box::into_raw(Box::new(ffi_schema)),
        });
        let ptr = Box::into_raw(cdata);
        completion(ptr as *const std::ffi::c_void, std::ptr::null());
    });
}
