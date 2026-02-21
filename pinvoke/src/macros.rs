/// Borrows an opaque pointer without consuming it.
///
/// Asserts the pointer is non-null, then returns a reference with the given lifetime.
///
/// # Usage
/// ```ignore
/// let table = ffi_borrow!(table_ptr, Table);
/// ```
macro_rules! ffi_borrow {
    ($ptr:expr, $ty:ty) => {{
        assert!(!$ptr.is_null(), concat!(stringify!($ty), " pointer is null"));
        unsafe { &*$ptr }
    }};
}

/// Clones an Arc from a raw pointer without consuming it.
///
/// Increments the strong count first, then creates an owned Arc.
/// Use this when you need an owned Arc for async operations.
///
/// # Usage
/// ```ignore
/// let connection = ffi_clone_arc!(connection_ptr, Connection);
/// ```
macro_rules! ffi_clone_arc {
    ($ptr:expr, $ty:ty) => {{
        assert!(!$ptr.is_null(), concat!(stringify!($ty), " pointer is null"));
        unsafe {
            std::sync::Arc::increment_strong_count($ptr);
            std::sync::Arc::from_raw($ptr)
        }
    }};
}

/// Frees an opaque pointer by dropping its Arc.
///
/// No-op if the pointer is null.
///
/// # Usage
/// ```ignore
/// ffi_free!(ptr, Table);
/// ```
macro_rules! ffi_free {
    ($ptr:expr, $ty:ty) => {
        if !$ptr.is_null() {
            unsafe { drop(std::sync::Arc::from_raw($ptr)) };
        }
    };
}
