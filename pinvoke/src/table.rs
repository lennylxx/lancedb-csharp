use lancedb::query::Query;
use lancedb::table::Table;
use libc::c_char;
use std::ffi::CString;
use std::sync::Arc;

/// Returns the name of the table as a C string. Caller must free with free_string().
/// Borrows the table pointer without consuming it.
#[no_mangle]
pub extern "C" fn table_get_name(table_ptr: *const Table) -> *mut c_char {
    assert!(!table_ptr.is_null(), "Table pointer is null");
    let table = unsafe { &*table_ptr };

    let name = table.name();
    let c_str_name = CString::new(name).unwrap();
    c_str_name.into_raw()
}

#[no_mangle]
pub extern "C" fn table_is_open(table_ptr: *const Table) -> bool {
    !table_ptr.is_null()
}

/// Creates a new Query from the table. Borrows the table pointer without consuming it.
#[no_mangle]
pub extern "C" fn table_create_query(table_ptr: *const Table) -> *const Query {
    assert!(!table_ptr.is_null(), "Table pointer is null");
    let table = unsafe { &*table_ptr };

    let query = table.query().clone();
    let arc_query = Arc::new(query);
    Arc::into_raw(arc_query)
}

/// Closes the table and frees the underlying Arc.
#[no_mangle]
pub extern "C" fn table_close(table_ptr: *const Table) {
    if !table_ptr.is_null() {
        unsafe { drop(Arc::from_raw(table_ptr)) };
    }
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
