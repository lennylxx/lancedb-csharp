use lancedb::index::scalar::{
    BooleanQuery, BoostQuery, FtsQuery, FullTextSearchQuery, MatchQuery, MultiMatchQuery, Occur,
    Operator, PhraseQuery,
};
use lancedb::query::{ExecutableQuery, Query, QueryBase, QueryExecutionOptions, Select, VectorQuery};
use lancedb::table::Table;
use libc::{c_char, c_float, size_t};
use serde::Deserialize;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::slice;
use std::sync::Arc;

use crate::ffi;
use crate::ffi::{callback_error, FfiCallback, UserData};

/// Parses a JSON string into a Select enum.
/// JSON array of strings → Select::Columns, JSON object → Select::Dynamic.
fn parse_select(json: &str) -> Result<Select, String> {
    let value: sonic_rs::Value =
        sonic_rs::from_str(json).map_err(|e| format!("Invalid select JSON: {}", e))?;
    if let Some(arr) = value.as_array() {
        let mut columns = Vec::new();
        for v in arr.iter() {
            let s = v.as_str()
                .ok_or_else(|| "Select array elements must be strings".to_string())?;
            columns.push(s.to_owned());
        }
        Ok(Select::Columns(columns))
    } else if let Some(obj) = value.as_object() {
        let mut pairs = Vec::new();
        for (k, v) in obj.iter() {
            let expr = v.as_str()
                .ok_or_else(|| "Select object values must be strings".to_string())?
                .to_owned();
            pairs.push((k.to_owned(), expr));
        }
        Ok(Select::Dynamic(pairs))
    } else {
        Err("Select must be a JSON array or object".to_string())
    }
}

// ---------------------------------------------------------------------------
// Query parameters (for build+execute)
// ---------------------------------------------------------------------------

/// All parameters for building a query, deserialized from JSON.
/// Used by the query_* and vector_query_* FFI functions.
#[derive(Deserialize, Default)]
pub struct QueryParams {
    pub select: Option<sonic_rs::Value>,
    #[serde(rename = "where")]
    pub predicate: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub with_row_id: Option<bool>,
    pub full_text_search: Option<String>,
    pub full_text_search_columns: Option<Vec<String>>,
    pub full_text_query: Option<String>,
    pub fast_search: Option<bool>,
    pub postfilter: Option<bool>,
    // Vector-specific
    pub column: Option<String>,
    pub distance_type: Option<i32>,
    pub nprobes: Option<u64>,
    pub refine_factor: Option<u32>,
    pub bypass_vector_index: Option<bool>,
    pub ef: Option<u64>,
    pub distance_range_lower: Option<f32>,
    pub distance_range_upper: Option<f32>,
    pub minimum_nprobes: Option<u32>,
    pub maximum_nprobes: Option<u32>,
    pub additional_vectors: Option<Vec<Vec<f32>>>,
}

/// Parses a JSON string into QueryParams.
pub(crate) fn parse_query_params(json: *const c_char) -> Result<QueryParams, String> {
    if json.is_null() {
        return Ok(QueryParams::default());
    }
    let json_str = ffi::to_string(json);
    if json_str.is_empty() {
        return Ok(QueryParams::default());
    }
    sonic_rs::from_str(&json_str).map_err(|e| format!("Invalid query params JSON: {}", e))
}

/// Structured full-text query JSON, mirroring the externally-tagged shape
/// emitted by the C# `FullTextQuery` types.
///
/// This is deserialized and converted to a `FtsQuery` natively (see
/// [`parse_fts_query_json`]) rather than via lance-index's `from_json`,
/// because lance-index's hand-written `MultiMatchQuery` serde does not
/// round-trip the per-query `operator`. Building the query natively preserves
/// `operator` at any nesting depth.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FtsQueryJson {
    Match(MatchJson),
    Phrase(PhraseJson),
    Boost(BoostJson),
    MultiMatch(MultiMatchJson),
    Boolean(BooleanJson),
}

#[derive(Deserialize)]
struct MatchJson {
    column: Option<String>,
    terms: String,
    boost: Option<f32>,
    fuzziness: Option<u32>,
    max_expansions: Option<usize>,
    operator: Option<String>,
    prefix_length: Option<u32>,
}

#[derive(Deserialize)]
struct PhraseJson {
    column: Option<String>,
    terms: String,
    slop: Option<u32>,
}

#[derive(Deserialize)]
struct BoostJson {
    positive: Box<FtsQueryJson>,
    negative: Box<FtsQueryJson>,
    negative_boost: Option<f32>,
}

#[derive(Deserialize)]
struct MultiMatchJson {
    query: String,
    columns: Vec<String>,
    boost: Option<Vec<f32>>,
    operator: Option<String>,
}

#[derive(Deserialize)]
struct BooleanJson {
    #[serde(default)]
    should: Vec<FtsQueryJson>,
    #[serde(default)]
    must: Vec<FtsQueryJson>,
    #[serde(default)]
    must_not: Vec<FtsQueryJson>,
}

fn parse_operator(value: &str) -> Result<Operator, String> {
    Operator::try_from(value).map_err(|e| e.to_string())
}

impl FtsQueryJson {
    fn into_fts(self) -> Result<FtsQuery, String> {
        match self {
            FtsQueryJson::Match(m) => {
                let mut q = MatchQuery::new(m.terms).with_column(m.column);
                if let Some(boost) = m.boost {
                    q = q.with_boost(boost);
                }
                q = q.with_fuzziness(m.fuzziness);
                if let Some(max_expansions) = m.max_expansions {
                    q = q.with_max_expansions(max_expansions);
                }
                if let Some(ref op) = m.operator {
                    q = q.with_operator(parse_operator(op)?);
                }
                if let Some(prefix_length) = m.prefix_length {
                    q = q.with_prefix_length(prefix_length);
                }
                Ok(q.into())
            }
            FtsQueryJson::Phrase(p) => {
                let mut q = PhraseQuery::new(p.terms).with_column(p.column);
                if let Some(slop) = p.slop {
                    q = q.with_slop(slop);
                }
                Ok(q.into())
            }
            FtsQueryJson::Boost(b) => {
                let positive = (*b.positive).into_fts()?;
                let negative = (*b.negative).into_fts()?;
                Ok(BoostQuery::new(positive, negative, b.negative_boost).into())
            }
            FtsQueryJson::MultiMatch(mm) => {
                let mut q =
                    MultiMatchQuery::try_new(mm.query, mm.columns).map_err(|e| e.to_string())?;
                if let Some(boosts) = mm.boost {
                    q = q.try_with_boosts(boosts).map_err(|e| e.to_string())?;
                }
                if let Some(ref op) = mm.operator {
                    q = q.with_operator(parse_operator(op)?);
                }
                Ok(q.into())
            }
            FtsQueryJson::Boolean(b) => {
                let mut pairs: Vec<(Occur, FtsQuery)> = Vec::new();
                for q in b.should {
                    pairs.push((Occur::Should, q.into_fts()?));
                }
                for q in b.must {
                    pairs.push((Occur::Must, q.into_fts()?));
                }
                for q in b.must_not {
                    pairs.push((Occur::MustNot, q.into_fts()?));
                }
                Ok(BooleanQuery::new(pairs).into())
            }
        }
    }
}

/// Parses the structured `full_text_query` JSON (as emitted by the C#
/// `FullTextQuery` types) into a native `FtsQuery`.
pub fn parse_fts_query_json(json: &str) -> Result<FtsQuery, String> {
    let parsed: FtsQueryJson =
        sonic_rs::from_str(json).map_err(|e| format!("Invalid full_text_query JSON: {}", e))?;
    parsed.into_fts()
}

/// Builds a FullTextSearchQuery from the FTS-related query params.
///
/// `full_text_query` (a structured FtsQuery JSON) and `full_text_search` (a raw
/// query string) are mutually exclusive. When a structured query is supplied the
/// columns are carried by the query nodes themselves, so
/// `full_text_search_columns` only applies to the raw-string path.
pub fn build_full_text_search(params: &QueryParams) -> Result<Option<FullTextSearchQuery>, String> {
    match (&params.full_text_query, &params.full_text_search) {
        (Some(_), Some(_)) => {
            Err("Cannot set both full_text_query and full_text_search".to_string())
        }
        (Some(json), None) => {
            let fts = parse_fts_query_json(json)?;
            Ok(Some(FullTextSearchQuery::new_query(fts)))
        }
        (None, Some(text)) => {
            let mut fts = FullTextSearchQuery::new(text.clone());
            if let Some(ref cols) = params.full_text_search_columns {
                fts = fts.with_columns(cols).map_err(|e| e.to_string())?;
            }
            Ok(Some(fts))
        }
        (None, None) => Ok(None),
    }
}

/// Applies base query parameters (shared between Query and VectorQuery).
pub(crate) fn apply_base_params(mut query: Query, params: &QueryParams) -> Result<Query, String> {
    if let Some(ref select) = params.select {
        let sel = parse_select(&select.to_string())?;
        query = query.select(sel);
    }
    if let Some(ref pred) = params.predicate {
        query = query.only_if(pred);
    }
    if let Some(limit) = params.limit {
        query = query.limit(limit as usize);
    }
    if let Some(offset) = params.offset {
        query = query.offset(offset as usize);
    }
    if params.with_row_id == Some(true) {
        query = query.with_row_id();
    }
    if let Some(fts) = build_full_text_search(params)? {
        query = query.full_text_search(fts);
    }
    if params.fast_search == Some(true) {
        query = query.fast_search();
    }
    if params.postfilter == Some(true) {
        query = query.postfilter();
    }
    Ok(query)
}

/// Applies vector-specific parameters to a VectorQuery.
pub(crate) fn apply_vector_params(
    mut vq: VectorQuery,
    params: &QueryParams,
) -> Result<VectorQuery, String> {
    if let Some(ref select) = params.select {
        let sel = parse_select(&select.to_string())?;
        vq = vq.select(sel);
    }
    if let Some(ref pred) = params.predicate {
        vq = vq.only_if(pred);
    }
    if let Some(limit) = params.limit {
        vq = vq.limit(limit as usize);
    }
    if let Some(offset) = params.offset {
        vq = vq.offset(offset as usize);
    }
    if params.with_row_id == Some(true) {
        vq = vq.with_row_id();
    }
    if let Some(fts) = build_full_text_search(params)? {
        vq = vq.full_text_search(fts);
    }
    if params.fast_search == Some(true) {
        vq = vq.fast_search();
    }
    if params.postfilter == Some(true) {
        vq = vq.postfilter();
    }
    if let Some(ref col) = params.column {
        vq = vq.column(col);
    }
    if let Some(dt) = params.distance_type {
        let dt = ffi::ffi_to_distance_type(dt)?;
        vq = vq.distance_type(dt);
    }
    if let Some(nprobes) = params.nprobes {
        vq = vq.nprobes(nprobes as usize);
    }
    if let Some(rf) = params.refine_factor {
        vq = vq.refine_factor(rf);
    }
    if params.bypass_vector_index == Some(true) {
        vq = vq.bypass_vector_index();
    }
    if let Some(ef) = params.ef {
        vq = vq.ef(ef as usize);
    }
    let lower = params.distance_range_lower;
    let upper = params.distance_range_upper;
    if lower.is_some() || upper.is_some() {
        vq = vq.distance_range(lower, upper);
    }
    if let Some(min_np) = params.minimum_nprobes {
        vq = vq
            .minimum_nprobes(min_np as usize)
            .map_err(|e| format!("Invalid minimum_nprobes: {}", e))?;
    }
    if let Some(max_np) = params.maximum_nprobes {
        let max = if max_np == 0 { None } else { Some(max_np as usize) };
        vq = vq
            .maximum_nprobes(max)
            .map_err(|e| format!("Invalid maximum_nprobes: {}", e))?;
    }
    if let Some(ref vectors) = params.additional_vectors {
        for v in vectors {
            vq = vq
                .add_query_vector(v.clone())
                .map_err(|e| format!("Failed to add query vector: {}", e))?;
        }
    }
    Ok(vq)
}

/// Builds a Query from a table and applies all base params.
fn build_query(table: &Table, params: &QueryParams) -> Result<Query, String> {
    let query = table.query().clone();
    apply_base_params(query, params)
}

/// Builds a VectorQuery from a table, query vector, and all params.
fn build_vector_query(
    table: &Table,
    vector: &[f32],
    params: &QueryParams,
) -> Result<VectorQuery, String> {
    let query = table.query().clone();
    let vq = query
        .nearest_to(vector)
        .map_err(|e| format!("Failed to create vector query: {}", e))?;
    apply_vector_params(vq, params)
}

// ---------------------------------------------------------------------------
// Shared execution helpers
// ---------------------------------------------------------------------------

/// Builds QueryExecutionOptions from FFI parameters.
fn build_execution_options(timeout_ms: i64, max_batch_length: u32) -> QueryExecutionOptions {
    let mut options = QueryExecutionOptions::default();
    if timeout_ms >= 0 {
        options.timeout = Some(std::time::Duration::from_millis(timeout_ms as u64));
    }
    if max_batch_length > 0 {
        options.max_batch_length = max_batch_length;
    }
    options
}

/// Executes any query and returns results via Arrow C Data Interface.
/// Collects all batches, concatenates into one RecordBatch, then exports
/// as FFI_ArrowArray + FFI_ArrowSchema (zero-copy on the consumer side).
fn execute_to_cdata_with_options<Q>(
    query: Arc<Q>,
    options: QueryExecutionOptions,
    completion: FfiCallback,
    user_data: UserData,
) where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::spawn(async move {
        use arrow_array::Array;
        use arrow_array::RecordBatch;
        use arrow_array::StructArray;
        use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        use futures::TryStreamExt;

        let stream = match query.execute_with_options(options).await {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, user_data, e);
                return;
            }
        };

        let schema = stream.schema().clone();
        let batches: Vec<RecordBatch> = match stream.try_collect().await {
            Ok(b) => b,
            Err(e) => {
                callback_error(completion, user_data, e);
                return;
            }
        };

        // Concatenate all batches into a single RecordBatch
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            match arrow_select::concat::concat_batches(&schema, &batches) {
                Ok(b) => b,
                Err(e) => {
                    callback_error(completion, user_data, e);
                    return;
                }
            }
        };

        // Convert RecordBatch → StructArray → FFI_ArrowArray + FFI_ArrowSchema
        let struct_array: StructArray = batch.into();
        let data = struct_array.to_data();

        let ffi_array = FFI_ArrowArray::new(&data);
        let ffi_schema = match FFI_ArrowSchema::try_from(data.data_type()) {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, user_data, e);
                return;
            }
        };

        let cdata = Box::new(ffi::FfiCData {
            array: Box::into_raw(Box::new(ffi_array)),
            schema: Box::into_raw(Box::new(ffi_schema)),
        });
        let ptr = Box::into_raw(cdata);
        completion(ptr as *const std::ffi::c_void, std::ptr::null(), user_data.as_ptr());
    });
}

/// Shared implementation for explain_plan across query types.
fn explain_plan_impl<Q>(
    query: Arc<Q>,
    verbose: bool,
    completion: FfiCallback,
    user_data: UserData,
) where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::spawn(async move {
        match query.explain_plan(verbose).await {
            Ok(plan) => {
                let c_str =
                    std::ffi::CString::new(plan).unwrap_or_default();
                completion(
                    c_str.into_raw() as *const std::ffi::c_void,
                    std::ptr::null(),
                    user_data.as_ptr(),
                );
            }
            Err(e) => callback_error(completion, user_data, e),
        }
    });
}

/// Shared implementation for analyze_plan across query types.
fn analyze_plan_impl<Q>(query: Arc<Q>, completion: FfiCallback, user_data: UserData)
where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::spawn(async move {
        match query.analyze_plan().await {
            Ok(plan) => {
                let c_str =
                    std::ffi::CString::new(plan).unwrap_or_default();
                completion(
                    c_str.into_raw() as *const std::ffi::c_void,
                    std::ptr::null(),
                    user_data.as_ptr(),
                );
            }
            Err(e) => callback_error(completion, user_data, e),
        }
    });
}

/// Shared implementation for output_schema across query types.
/// Returns a heap-allocated FFI_ArrowSchema (caller must free with free_ffi_schema).
fn output_schema_impl<Q>(query: Arc<Q>, completion: FfiCallback, user_data: UserData)
where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::spawn(async move {
        match query.output_schema().await {
            Ok(schema) => {
                match arrow_schema::ffi::FFI_ArrowSchema::try_from(
                    arrow_schema::DataType::Struct(schema.fields().clone()),
                ) {
                    Ok(ffi_schema) => {
                        let ptr = Box::into_raw(Box::new(ffi_schema));
                        completion(ptr as *const std::ffi::c_void, std::ptr::null(), user_data.as_ptr());
                    }
                    Err(e) => callback_error(completion, user_data, e),
                }
            }
            Err(e) => callback_error(completion, user_data, e),
        }
    });
}

// ---------------------------------------------------------------------------
// Query FFI
// ---------------------------------------------------------------------------

/// Builds a Query from table + JSON params and executes it.
#[unsafe(no_mangle)]
pub extern "C" fn query_execute(
    table_ptr: *const Table,
    params_json: *const c_char,
    timeout_ms: i64,
    max_batch_length: u32,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let query = match build_query(table, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let options = build_execution_options(timeout_ms, max_batch_length);
    execute_to_cdata_with_options(Arc::new(query), options, completion, user_data);
}

/// Builds a Query from table + JSON params and returns the explain plan.
#[unsafe(no_mangle)]
pub extern "C" fn query_explain_plan(
    table_ptr: *const Table,
    params_json: *const c_char,
    verbose: bool,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let query = match build_query(table, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    explain_plan_impl(Arc::new(query), verbose, completion, user_data);
}

/// Builds a Query from table + JSON params and returns the analyze plan.
#[unsafe(no_mangle)]
pub extern "C" fn query_analyze_plan(
    table_ptr: *const Table,
    params_json: *const c_char,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let query = match build_query(table, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    analyze_plan_impl(Arc::new(query), completion, user_data);
}

/// Builds a Query from table + JSON params and returns the output schema.
#[unsafe(no_mangle)]
pub extern "C" fn query_output_schema(
    table_ptr: *const Table,
    params_json: *const c_char,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let query = match build_query(table, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    output_schema_impl(Arc::new(query), completion, user_data);
}

// ---------------------------------------------------------------------------
// VectorQuery FFI
// ---------------------------------------------------------------------------

/// Builds a VectorQuery from table + vector + JSON params and executes it.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_execute(
    table_ptr: *const Table,
    vector_ptr: *const c_float,
    vector_len: size_t,
    params_json: *const c_char,
    timeout_ms: i64,
    max_batch_length: u32,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let vector = unsafe {
        if vector_ptr.is_null() {
            callback_error(completion, user_data, "Vector pointer is null");
            return;
        }
        slice::from_raw_parts(vector_ptr, vector_len as usize)
    };
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let vq = match build_vector_query(table, vector, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let options = build_execution_options(timeout_ms, max_batch_length);
    execute_to_cdata_with_options(Arc::new(vq), options, completion, user_data);
}

/// Builds a VectorQuery from table + vector + JSON params and returns explain plan.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_explain_plan(
    table_ptr: *const Table,
    vector_ptr: *const c_float,
    vector_len: size_t,
    params_json: *const c_char,
    verbose: bool,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let vector = unsafe {
        if vector_ptr.is_null() {
            callback_error(completion, user_data, "Vector pointer is null");
            return;
        }
        slice::from_raw_parts(vector_ptr, vector_len as usize)
    };
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let vq = match build_vector_query(table, vector, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    explain_plan_impl(Arc::new(vq), verbose, completion, user_data);
}

/// Builds a VectorQuery from table + vector + JSON params and returns analyze plan.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_analyze_plan(
    table_ptr: *const Table,
    vector_ptr: *const c_float,
    vector_len: size_t,
    params_json: *const c_char,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let vector = unsafe {
        if vector_ptr.is_null() {
            callback_error(completion, user_data, "Vector pointer is null");
            return;
        }
        slice::from_raw_parts(vector_ptr, vector_len as usize)
    };
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let vq = match build_vector_query(table, vector, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    analyze_plan_impl(Arc::new(vq), completion, user_data);
}

/// Builds a VectorQuery from table + vector + JSON params and returns output schema.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_output_schema(
    table_ptr: *const Table,
    vector_ptr: *const c_float,
    vector_len: size_t,
    params_json: *const c_char,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let vector = unsafe {
        if vector_ptr.is_null() {
            callback_error(completion, user_data, "Vector pointer is null");
            return;
        }
        slice::from_raw_parts(vector_ptr, vector_len as usize)
    };
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let vq = match build_vector_query(table, vector, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    output_schema_impl(Arc::new(vq), completion, user_data);
}

// ---------------------------------------------------------------------------
// Record Batch Stream FFI
// ---------------------------------------------------------------------------

type StreamHandle = tokio::sync::Mutex<lancedb::arrow::SendableRecordBatchStream>;

/// Shared helper: execute any query and return a stream handle via callback.
fn execute_stream_impl<Q>(
    query: Arc<Q>,
    options: QueryExecutionOptions,
    completion: FfiCallback,
    user_data: UserData,
) where
    Q: ExecutableQuery + Send + Sync + 'static,
{
    crate::spawn(async move {
        match query.execute_with_options(options).await {
            Ok(stream) => {
                let handle = Arc::new(tokio::sync::Mutex::new(stream));
                let ptr = Arc::into_raw(handle);
                completion(ptr as *const std::ffi::c_void, std::ptr::null(), user_data.as_ptr());
            }
            Err(e) => callback_error(completion, user_data, e),
        }
    });
}

/// Execute a Query and return a stream handle for incremental batch retrieval.
#[unsafe(no_mangle)]
pub extern "C" fn query_execute_stream(
    table_ptr: *const Table,
    params_json: *const c_char,
    timeout_ms: i64,
    max_batch_length: u32,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let query = match build_query(table, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let options = build_execution_options(timeout_ms, max_batch_length);
    execute_stream_impl(Arc::new(query), options, completion, user_data);
}

/// Execute a VectorQuery and return a stream handle for incremental batch retrieval.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_execute_stream(
    table_ptr: *const Table,
    vector_ptr: *const c_float,
    vector_len: size_t,
    params_json: *const c_char,
    timeout_ms: i64,
    max_batch_length: u32,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let table = ffi_borrow!(table_ptr, Table);
    let vector = unsafe {
        if vector_ptr.is_null() {
            callback_error(completion, user_data, "Vector pointer is null");
            return;
        }
        slice::from_raw_parts(vector_ptr, vector_len as usize)
    };
    let params = match parse_query_params(params_json) {
        Ok(p) => p,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let vq = match build_vector_query(table, vector, &params) {
        Ok(q) => q,
        Err(e) => {
            callback_error(completion, user_data, e);
            return;
        }
    };
    let options = build_execution_options(timeout_ms, max_batch_length);
    execute_stream_impl(Arc::new(vq), options, completion, user_data);
}

/// Get the next batch from a stream. Returns FfiCData via callback, or null
/// pointer (with no error) when the stream is exhausted.
#[unsafe(no_mangle)]
pub extern "C" fn stream_next(
    stream_ptr: *const StreamHandle,
    completion: FfiCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = UserData(user_data);
    let stream = ffi_clone_arc!(stream_ptr, StreamHandle);
    crate::spawn(async move {
        use arrow_array::Array;
        use arrow_array::StructArray;
        use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        use futures::StreamExt;

        let mut guard = stream.lock().await;
        match guard.next().await {
            Some(Ok(batch)) => {
                let struct_array: StructArray = batch.into();
                let data = struct_array.to_data();
                let ffi_array = FFI_ArrowArray::new(&data);
                let ffi_schema = match FFI_ArrowSchema::try_from(data.data_type()) {
                    Ok(s) => s,
                    Err(e) => {
                        callback_error(completion, user_data, e);
                        return;
                    }
                };
                let cdata = Box::new(ffi::FfiCData {
                    array: Box::into_raw(Box::new(ffi_array)),
                    schema: Box::into_raw(Box::new(ffi_schema)),
                });
                completion(Box::into_raw(cdata) as *const std::ffi::c_void, std::ptr::null(), user_data.as_ptr());
            }
            Some(Err(e)) => callback_error(completion, user_data, e),
            None => {
                // Stream exhausted — signal with null pointer and no error
                completion(std::ptr::null(), std::ptr::null(), user_data.as_ptr());
            }
        }
    });
}

/// Close and free a stream handle.
#[unsafe(no_mangle)]
pub extern "C" fn stream_close(stream_ptr: *const StreamHandle) {
    if !stream_ptr.is_null() {
        unsafe { drop(Arc::from_raw(stream_ptr)); }
    }
}
