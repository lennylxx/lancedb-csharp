use lancedb::query::{Query, VectorQuery};
use libc::{c_char, c_double, size_t};
use std::slice;
use std::sync::Arc;

use crate::ffi;

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

    let vector_query = <Query as Clone>::clone(query).nearest_to(vector).unwrap().clone();
    Arc::into_raw(Arc::new(vector_query))
}

/// Sets the column for a VectorQuery.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_column(
    vector_query_ptr: *const VectorQuery,
    column_name: *const c_char,
) -> *const VectorQuery {
    let vector_query = ffi_borrow!(vector_query_ptr, VectorQuery);

    let column_name = ffi::to_string(column_name);
    <VectorQuery as Clone>::clone(vector_query).column(&column_name);

    vector_query_ptr
}

/// Frees a Query pointer.
#[unsafe(no_mangle)]
pub extern "C" fn query_free(query_ptr: *const Query) {
    ffi_free!(query_ptr, Query);
}

/// Frees a VectorQuery pointer.
#[unsafe(no_mangle)]
pub extern "C" fn vector_query_free(vector_query_ptr: *const VectorQuery) {
    ffi_free!(vector_query_ptr, VectorQuery);
}