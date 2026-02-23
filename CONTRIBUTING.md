# Contributing to this project

## Architecture

```
lancedb-csharp/
├── src/           C# class library (net10.0, namespace: lancedb)
├── pinvoke/       Rust cdylib (lancedb-ffi) exposing C-compatible FFI
└── tests/         C# xUnit tests
```

The SDK has two layers:

- **`pinvoke/`** — A Rust `cdylib` that exposes C-compatible FFI functions. Uses a pool of Tokio runtimes (one per CPU core) with least-loaded dispatch to bridge Rust async operations into synchronous FFI calls with C function pointer callbacks.
- **`src/`** — A C# class library that declares `[DllImport]` extern methods matching the Rust FFI surface, and wraps them in idiomatic async APIs using `TaskCompletionSource`.

### Ownership model

Rust objects (`Connection`, `Table`) are heap-allocated via `Arc::into_raw` on the Rust side and passed as `IntPtr` pointers to C#. C# classes use `SafeHandle` subclasses to ensure proper cleanup via the corresponding Rust free/drop functions. `RustStringHandle` extends `SafeHandle` to automatically free Rust-allocated strings via `free_string`.

Query builders (`Query`, `VectorQuery`) are pure C# objects with no native pointers. They store all builder parameters locally and make a single FFI call at execution time (e.g., `ToArrow()`, `ExplainPlan()`). Parameters are serialized to JSON and passed to a consolidated Rust FFI function (`query_execute` or `vector_query_execute`) that builds and executes the query in one shot. This matches the Python SDK's lazy builder pattern.

Data crosses the FFI boundary via the Arrow C Data Interface (zero-copy for Rust→C#, clone-and-pin for C#→Rust).

## Adding a new FFI function

1. Add the `#[unsafe(no_mangle)] pub extern "C" fn` in the appropriate Rust file under `pinvoke/src/` (edition 2024 requires `unsafe(...)` for `no_mangle`).
2. Add a matching `[DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]` declaration in the corresponding C# class.
3. For async Rust operations: use `crate::spawn` with a completion callback on the Rust side, and `TaskCompletionSource<IntPtr>` with a pinned `GCHandle` delegate on the C# side. The `spawn` function dispatches to the least-loaded runtime in the pool.

## Conventions

- All C# types are in the `lancedb` namespace (no sub-namespaces).
- Options/config classes go in `src/Options/`.
- Strings crossing the FFI boundary are UTF-8 encoded byte arrays passed as `IntPtr`. On the Rust side, `ffi::to_string` converts `*const c_char` to an owned `String`. On the C# side, `Encoding.UTF8.GetBytes` is used before passing via `fixed` pointer.
- C# uses `unsafe` blocks with `fixed` for pinning byte arrays during FFI calls.
- Rust FFI functions that return heap objects use `Arc::into_raw`; the caller is responsible for eventually calling the matching free function to avoid leaks.

## Test Projects

- **C# tests** live in `tests/` as an xUnit project (`lancedb.tests.csproj`).
  - Test naming convention: `MethodName_Scenario_ExpectedResult` (e.g., `Connect_ValidUri_ReturnsConnection`).
- **Rust tests** live in `pinvoke/tests/` as Cargo integration tests.
  - Test the FFI functions directly using raw pointers to verify ownership and memory safety.
  - Async FFI functions are tested by calling the underlying lancedb API directly (extern "C" callbacks can't capture state).