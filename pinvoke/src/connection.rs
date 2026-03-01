use lance_namespace::models::ListTablesRequest;
use lancedb::connection::Connection;
use lancedb::database::CreateTableMode;
use libc::c_char;
use std::ffi::CString;

use crate::ffi;
use crate::{callback_error, FfiCallback};

#[unsafe(no_mangle)]
pub extern "C" fn database_connect(
    uri: *const c_char,
    read_consistency_interval_secs: f64,
    storage_options_json: *const c_char,
    index_cache_size_bytes: i64,
    metadata_cache_size_bytes: i64,
    completion: FfiCallback,
) {
    let dataset_uri = ffi::to_string(uri);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let rci_secs = if read_consistency_interval_secs.is_nan() {
        None
    } else {
        Some(read_consistency_interval_secs)
    };

    let has_session = index_cache_size_bytes >= 0 || metadata_cache_size_bytes >= 0;

    crate::spawn(async move {
        let mut builder = lancedb::connection::connect(&dataset_uri);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(secs) = rci_secs {
            builder = builder.read_consistency_interval(std::time::Duration::from_secs_f64(secs));
        }
        if has_session {
            let index_size = if index_cache_size_bytes > 0 {
                index_cache_size_bytes as usize
            } else {
                6 * 1024 * 1024 * 1024 // DEFAULT_INDEX_CACHE_SIZE (6 GiB)
            };
            let metadata_size = if metadata_cache_size_bytes > 0 {
                metadata_cache_size_bytes as usize
            } else {
                1024 * 1024 * 1024 // DEFAULT_METADATA_CACHE_SIZE (1 GiB)
            };
            let session = lancedb::Session::new(
                index_size,
                metadata_size,
                std::sync::Arc::new(lancedb::ObjectStoreRegistry::default()),
            );
            builder = builder.session(std::sync::Arc::new(session));
        }
        match builder.execute().await {
            Ok(conn) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(conn));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_open_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    storage_options_json: *const c_char,
    index_cache_size: u32,
    location: *const c_char,
    namespace_json: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    crate::spawn(async move {
        let mut builder = connection.open_table(table_name);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if index_cache_size > 0 {
            builder = builder.index_cache_size(index_cache_size);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_empty_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    schema_cdata: *mut arrow_schema::ffi::FFI_ArrowSchema,
    mode: *const c_char,
    storage_options_json: *const c_char,
    location: *const c_char,
    namespace_json: *const c_char,
    exist_ok: bool,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);

    let schema = if schema_cdata.is_null() {
        ffi::minimal_schema()
    } else {
        match ffi::import_schema(schema_cdata) {
            Ok(s) => s,
            Err(e) => {
                callback_error(completion, e);
                return;
            }
        }
    };

    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);

    let create_mode = if exist_ok {
        CreateTableMode::exist_ok(|req| req)
    } else if !mode.is_null() {
        match ffi::to_string(mode).as_str() {
            "overwrite" => CreateTableMode::Overwrite,
            _ => CreateTableMode::Create,
        }
    } else {
        CreateTableMode::Create
    };

    crate::spawn(async move {
        let mut builder = connection
            .create_empty_table(table_name, schema)
            .mode(create_mode);
        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_create_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    arrays: *mut arrow_data::ffi::FFI_ArrowArray,
    schema: *mut arrow_schema::ffi::FFI_ArrowSchema,
    batch_count: usize,
    mode: *const c_char,
    storage_options_json: *const c_char,
    location: *const c_char,
    namespace_json: *const c_char,
    exist_ok: bool,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    let storage_opts = ffi::parse_optional_json_map(storage_options_json);
    let location_str = ffi::parse_optional_string(location);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);

    let (batches, schema_ref) = match ffi::import_batches(arrays, schema, batch_count) {
        Ok(r) => r,
        Err(e) => {
            callback_error(completion, e);
            return;
        }
    };

    let create_mode = if exist_ok {
        CreateTableMode::exist_ok(|req| req)
    } else if mode.is_null() {
        CreateTableMode::Create
    } else {
        match ffi::to_string(mode).as_str() {
            "overwrite" => CreateTableMode::Overwrite,
            _ => CreateTableMode::Create,
        }
    };

    crate::spawn(async move {
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema_ref,
        );

        let mut builder = connection
            .create_table(table_name, reader)
            .mode(create_mode);

        if let Some(opts) = storage_opts {
            builder = builder.storage_options(opts);
        }
        if let Some(loc) = location_str {
            builder = builder.location(loc);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }

        match builder.execute().await {
            Ok(table) => {
                let ptr = std::sync::Arc::into_raw(std::sync::Arc::new(table));
                completion(ptr as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_close(connection_ptr: *const Connection) {
    ffi_free!(connection_ptr, Connection);
}

#[unsafe(no_mangle)]
pub extern "C" fn database_table_names(
    connection_ptr: *const Connection,
    start_after: *const c_char,
    limit: u32,
    namespace_json: *const c_char,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    let start_after_str = ffi::parse_optional_string(start_after);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);
    crate::spawn(async move {
        let mut builder = connection.table_names();
        if let Some(sa) = start_after_str {
            builder = builder.start_after(sa);
        }
        if limit > 0 {
            builder = builder.limit(limit);
        }
        if let Some(ns) = namespace_list {
            builder = builder.namespace(ns);
        }
        match builder.execute().await {
            Ok(names) => {
                let joined = names.join("\n");
                let c_str = CString::new(joined).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_list_tables(
    connection_ptr: *const Connection,
    page_token: *const c_char,
    limit: u32,
    namespace_json: *const c_char,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    let page_token_str = ffi::parse_optional_string(page_token);
    let namespace_list = ffi::parse_optional_json_list(namespace_json);
    crate::spawn(async move {
        let mut request = ListTablesRequest::new();
        request.page_token = page_token_str;
        if limit > 0 {
            request.limit = Some(limit as i32);
        }
        if let Some(ns) = namespace_list {
            request.id = Some(ns);
        }
        match connection.list_tables(request).await {
            Ok(response) => {
                let json = sonic_rs::json!({
                    "tables": response.tables,
                    "page_token": response.page_token,
                });
                let json_str = sonic_rs::to_string(&json).unwrap_or_default();
                let c_str = CString::new(json_str).unwrap_or_default();
                completion(c_str.into_raw() as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_drop_table(
    connection_ptr: *const Connection,
    table_name: *const c_char,
    completion: FfiCallback,
) {
    let table_name = ffi::to_string(table_name);
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    crate::spawn(async move {
        match connection.drop_table(&table_name, &[]).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn database_drop_all_tables(
    connection_ptr: *const Connection,
    completion: FfiCallback,
) {
    let connection = ffi_clone_arc!(connection_ptr, Connection);
    crate::spawn(async move {
        match connection.drop_all_tables(&[]).await {
            Ok(()) => {
                completion(1 as *const std::ffi::c_void, std::ptr::null());
            }
            Err(e) => callback_error(completion, e),
        }
    });
}
