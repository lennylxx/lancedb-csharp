use lancedb::query::{Query, VectorQuery};
use libc::{c_char, c_double, size_t};
use std::slice;
use std::sync::Arc;

use crate::ffi;

/// Creates a VectorQuery from a Query by finding nearest vectors.
/// Borrows the query pointer without consuming it.
#[no_mangle]
pub extern "C" fn query_nearest_to(
    query_ptr: *const Query,
    vector_ptr: *const c_double,
    len: size_t,
) -> *const VectorQuery {
    assert!(!query_ptr.is_null(), "Query pointer is null");
    let query = unsafe { &*query_ptr };

    let vector = unsafe {
        assert!(!vector_ptr.is_null());
        slice::from_raw_parts(vector_ptr, len as usize)
    };

    let vector_query = <Query as Clone>::clone(query).nearest_to(vector).unwrap().clone();
    let arc_vector_query = Arc::new(vector_query);
    Arc::into_raw(arc_vector_query)
}

/// Sets the column for a VectorQuery. Borrows the pointer without consuming it.
#[no_mangle]
pub extern "C" fn vector_query_column(
    vector_query_ptr: *const VectorQuery,
    column_name: *const c_char,
) -> *const VectorQuery {
    assert!(!vector_query_ptr.is_null(), "VectorQuery pointer is null");
    let vector_query = unsafe { &*vector_query_ptr };

    let column_name = ffi::get_static_str(column_name);
    <VectorQuery as Clone>::clone(vector_query).column(column_name);

    vector_query_ptr
}

/// Frees a Query pointer.
#[no_mangle]
pub extern "C" fn query_free(query_ptr: *const Query) {
    if !query_ptr.is_null() {
        unsafe { drop(Arc::from_raw(query_ptr)) };
    }
}

/// Frees a VectorQuery pointer.
#[no_mangle]
pub extern "C" fn vector_query_free(vector_query_ptr: *const VectorQuery) {
    if !vector_query_ptr.is_null() {
        unsafe { drop(Arc::from_raw(vector_query_ptr)) };
    }
}