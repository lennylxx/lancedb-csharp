use lancedb::connection::Connection;
use lancedb::table::Table;
use lazy_static::lazy_static;
use libc::c_char;
use std::sync::Arc;
use tokio::runtime::Runtime;

mod ffi;
mod query;
mod table;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().expect("Failed to create tokio runtime");
}

#[no_mangle]
pub extern "C" fn database_connect(
    uri: *const c_char,
    completion: extern "C" fn(*const Connection),
) {
    let dataset_uri: &str = ffi::get_static_str(uri);
    RUNTIME.spawn(async move {
        let connection = lancedb::connection::connect(dataset_uri).execute().await;

        // allocate it on the heap to avoid it being released
        let arc_connection = Arc::new(connection.unwrap());
        let ptr = Arc::into_raw(arc_connection);
        completion(ptr);
    });
}

#[no_mangle]
pub extern "C" fn database_open_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: extern "C" fn(*const Table),
) {
    let table_name = ffi::get_static_str(table_name);
    let connection = unsafe {
        if !connection_ptr.is_null() {
            Arc::from_raw(connection_ptr)
        } else {
            panic!("Connection pointer is null");
        }
    };
    RUNTIME.spawn(async move {
        let table = connection.open_table(table_name).execute().await;
        let arc_table = Arc::new(table.unwrap());
        let ptr = Arc::into_raw(arc_table);

        completion(ptr);
    });
}

#[no_mangle]
pub extern "C" fn database_close(connection_ptr: *const Connection) {
    let connection = unsafe {
        if !connection_ptr.is_null() {
            Arc::from_raw(connection_ptr)
        } else {
            panic!("Connection pointer is null");
        }
    };

    drop(connection);
}
