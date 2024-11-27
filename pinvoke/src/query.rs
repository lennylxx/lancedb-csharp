use lancedb::query::{Query, VectorQuery};
use libc::{c_char, c_double, size_t};
use std::slice;
use std::sync::Arc;

use crate::ffi;

#[no_mangle]
pub extern "C" fn query_nearest_to(
    query_ptr: *const Query,
    vector_ptr: *const c_double,
    len: size_t,
) -> *const VectorQuery {
    let query = unsafe {
        if !query_ptr.is_null() {
            Arc::from_raw(query_ptr)
        } else {
            panic!("Query pointer is null");
        }
    };

    let vector = unsafe {
        assert!(!vector_ptr.is_null());
        slice::from_raw_parts(vector_ptr, len as usize)
    };

    let vector_query = <Query as Clone>::clone(&query).nearest_to(vector).unwrap().clone();
    let arc_vector_query = Arc::new(vector_query);
    let ptr = Arc::into_raw(arc_vector_query);

    ptr
}

#[no_mangle]
pub extern "C" fn vector_query_column(
    vector_query_ptr: *const VectorQuery,
    column_name: *const c_char,
) -> *const VectorQuery {
    let vector_query = unsafe {
        if !vector_query_ptr.is_null() {
            Arc::from_raw(vector_query_ptr)
        } else {
            panic!("VectorQuery pointer is null");
        }
    };

    let column_name = ffi::get_static_str(column_name);
    <VectorQuery as Clone>::clone(&vector_query).column(column_name);

    vector_query_ptr
}