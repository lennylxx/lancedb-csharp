use lancedb::query::{ExecutableQuery, Query, QueryBase, Select, VectorQuery};
use libc::{c_char, c_double, size_t};
use std::slice;
use std::sync::Arc;

use crate::ffi;
use crate::table::FfiBytes;
use crate::FfiCallback;

/// Parses a JSON string into a Select enum.
/// JSON array of strings → Select::Columns, JSON object → Select::Dynamic.
fn parse_select(json: &str) -> Result<Select, String> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("Invalid select JSON: {}", e))?;
    match value {
        serde_json::Value::Array(arr) => {
            let columns: Vec<String> = arr
                .into_iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| "Select array elements must be strings".to_string())
                        .map(|s| s.to_owned())
                })
                .collect::<Result<_, _>>()?;
            Ok(Select::Columns(columns))
        }
        serde_json::Value::Object(obj) => {
            let pairs: Vec<(String, String)> = obj
                .into_iter()
                .map(|(k, v)| {
                    let expr = v
                        .as_str()
                        .ok_or_else(|| "Select object values must be strings".to_string())?
                        .to_owned();
                    Ok((k, expr))
                })
                .collect::<Result<_, String>>()?;
            Ok(Select::Dynamic(pairs))
        }
        _ => Err("Select must be a JSON array or object".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Query builder FFI
// ---------------------------------------------------------------------------

/// Applies a column selection to a Query. columns_json is a JSON array of
/// column names or a JSON object of {"alias": "expression"} pairs.
/// Returns a new Query pointer; caller must free the old one.
#[unsafe(no_mangle)]
pub extern "C" fn query_select(
    query_ptr: *const Query,
    columns_json: *const c_char,
) -> *const Query {
    let query = ffi_borrow!(query_ptr, Query);
    let json = ffi::to_string(columns_json);
    let select = parse_select(&json).expect("Invalid select JSON");
    let new_query = query.clone().select(select);
    Arc::into_raw(Arc::new(new_query))
}

/// Applies a WHERE filter to a Query. Returns a new Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_only_if(
    query_ptr: *const Query,
    predicate: *const c_char,
) -> *const Query {
    let query = ffi_borrow!(query_ptr, Query);
    let predicate = ffi::to_string(predicate);
    let new_query = query.clone().only_if(predicate);
    Arc::into_raw(Arc::new(new_query))
}

/// Sets a row limit on a Query. Returns a new Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_limit(query_ptr: *const Query, limit: u64) -> *const Query {
    let query = ffi_borrow!(query_ptr, Query);
    let new_query = query.clone().limit(limit as usize);
    Arc::into_raw(Arc::new(new_query))
}

/// Sets a row offset on a Query. Returns a new Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_offset(query_ptr: *const Query, offset: u64) -> *const Query {
    let query = ffi_borrow!(query_ptr, Query);
    let new_query = query.clone().offset(offset as usize);
    Arc::into_raw(Arc::new(new_query))
}

/// Includes the internal row ID column in results. Returns a new Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_with_row_id(query_ptr: *const Query) -> *const Query {
    let query = ffi_borrow!(query_ptr, Query);
    let new_query = query.clone().with_row_id();
    Arc::into_raw(Arc::new(new_query))
}

/// Creates a VectorQuery from a Query by finding nearest vectors.
#[unsafe(no_mangle)]
pub extern "C" fn query_nearest_to(
    query_ptr: *const Query,
    vector_ptr: *const c_double,
    len: size_t,
) -> *const VectorQuery {
    let query = ffi_borrow!(query_ptr, Query);

    let vector = unsafe {
        assert!(!vector_ptr.is_null());
        slice::from_raw_parts(vector_ptr, len as usize)
    };

    let vector_query = query.clone().nearest_to(vector).unwrap().clone();
    Arc::into_raw(Arc::new(vector_query))
}

/// Executes a Query and returns results as Arrow IPC bytes via FfiBytes.
/// The callback receives a pointer to an FfiBytes struct (caller must free with free_ffi_bytes).
#[unsafe(no_mangle)]
pub extern "C" fn query_execute(query_ptr: *const Query, completion: FfiCallback) {
    let query = ffi_clone_arc!(query_ptr, Query);
    execute_to_ipc(query, completion);
}

/// Frees a Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_free(query_ptr: *const Query) {
    ffi_free!(query_ptr, Query);
}

// ---------------------------------------------------------------------------
// VectorQuery builder FFI
// ---------------------------------------------------------------------------

/// Applies a column selection to a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_select(
    vq_ptr: *const VectorQuery,
    columns_json: *const c_char,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let json = ffi::to_string(columns_json);
    let select = parse_select(&json).expect("Invalid select JSON");
    let new_vq = vq.clone().select(select);
    Arc::into_raw(Arc::new(new_vq))
}

/// Applies a WHERE filter to a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_only_if(
    vq_ptr: *const VectorQuery,
    predicate: *const c_char,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let predicate = ffi::to_string(predicate);
    let new_vq = vq.clone().only_if(predicate);
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets a row limit on a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_limit(vq_ptr: *const VectorQuery, limit: u64) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().limit(limit as usize);
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets a row offset on a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_offset(
    vq_ptr: *const VectorQuery,
    offset: u64,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().offset(offset as usize);
    Arc::into_raw(Arc::new(new_vq))
}

/// Includes the internal row ID column in VectorQuery results.
/// Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_with_row_id(vq_ptr: *const VectorQuery) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().with_row_id();
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets the vector column to query. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_column(
    vq_ptr: *const VectorQuery,
    column_name: *const c_char,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let column_name = ffi::to_string(column_name);
    let new_vq = vq.clone().column(&column_name);
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets the distance type for a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_distance_type(
    vq_ptr: *const VectorQuery,
    distance_type: *const c_char,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let dt_str = ffi::to_string(distance_type);
    let dt = ffi::parse_distance_type(&dt_str).expect("Invalid distance type");
    let new_vq = vq.clone().distance_type(dt);
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets nprobes for a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_nprobes(
    vq_ptr: *const VectorQuery,
    nprobes: u64,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().nprobes(nprobes as usize);
    Arc::into_raw(Arc::new(new_vq))
}

/// Sets refine_factor for a VectorQuery. Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_refine_factor(
    vq_ptr: *const VectorQuery,
    refine_factor: u32,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().refine_factor(refine_factor);
    Arc::into_raw(Arc::new(new_vq))
}

/// Bypasses the vector index for a VectorQuery (brute-force search).
/// Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_bypass_vector_index(
    vq_ptr: *const VectorQuery,
) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().bypass_vector_index();
    Arc::into_raw(Arc::new(new_vq))
}

/// Applies filters after the vector search instead of before.
/// Returns a new VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_postfilter(vq_ptr: *const VectorQuery) -> *const VectorQuery {
    let vq = ffi_borrow!(vq_ptr, VectorQuery);
    let new_vq = vq.clone().postfilter();
    Arc::into_raw(Arc::new(new_vq))
}

/// Executes a VectorQuery and returns results as Arrow IPC bytes via FfiBytes.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_execute(vq_ptr: *const VectorQuery, completion: FfiCallback) {
    let vq = ffi_clone_arc!(vq_ptr, VectorQuery);
    execute_to_ipc(vq, completion);
}

/// Frees a VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_free(vector_query_ptr: *const VectorQuery) {
    ffi_free!(vector_query_ptr, VectorQuery);
}

// ---------------------------------------------------------------------------
// Shared execution helper
// ---------------------------------------------------------------------------

/// Executes any query that implements ExecutableQuery and returns Arrow IPC bytes.
fn execute_to_ipc<Q>(query: Arc<Q>, completion: FfiCallback)
where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::RUNTIME.spawn(async move {
        use futures::TryStreamExt;

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