use arrow_schema::{DataType, Schema};
use libc::c_char;
use std::ffi::CStr;
use std::sync::Arc;

/// Converts a C string pointer to an owned Rust String.
/// The caller retains ownership of the original C string.
pub fn to_string(c_string: *const c_char) -> String {
    assert!(!c_string.is_null(), "Received null pointer");
    let c_str = unsafe { CStr::from_ptr(c_string) };
    c_str.to_str().expect("Invalid UTF-8 data").to_owned()
}

/// Create a minimal Arrow schema with a single "id" integer field.
/// Used for creating empty tables for testing.
pub fn minimal_schema() -> Arc<Schema> {
    use arrow_schema::Field;
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

/// Serializes an Arrow Schema to Arrow IPC format bytes.
pub fn schema_to_ipc(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(&mut buf, schema).expect("IPC writer init");
        writer.finish().expect("IPC writer finish");
    }
    buf
}
