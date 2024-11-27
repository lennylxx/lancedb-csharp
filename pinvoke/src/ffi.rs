use libc::c_char;
use std::ffi::CStr;

pub fn get_static_str(c_string: *const c_char) -> &'static str {
    assert!(!c_string.is_null(), "Received null pointer");
    let c_str = unsafe { CStr::from_ptr(c_string) };
    c_str.to_str().expect("Invalid UTF-8 data")
}
