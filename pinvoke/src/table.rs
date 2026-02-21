use lancedb::query::Query;
use lancedb::table::Table;
use libc::c_char;
use std::ffi::CString;
use std::sync::Arc;

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
