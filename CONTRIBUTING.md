# Contributing to this project

## Architecture

```
lancedb-csharp/
├── src/           C# class library (netstandard2.0 + net8.0, namespace: lancedb)
├── pinvoke/       Rust cdylib (lancedb-ffi) exposing C-compatible FFI
└── tests/         C# xUnit tests
```

The SDK has two layers:

- **`pinvoke/`** — A Rust `cdylib` that exposes C-compatible FFI functions. Uses a pool of Tokio runtimes (one per CPU core) with least-loaded dispatch to bridge Rust async operations into synchronous FFI calls with C function pointer callbacks.
- **`src/`** — A C# class library that declares `[DllImport]` extern methods matching the Rust FFI surface, and wraps them in idiomatic async APIs using `TaskCompletionSource`.

### Ownership model

Rust objects (`Connection`, `Table`) are heap-allocated via `Arc::into_raw` on the Rust side and passed as `IntPtr` pointers to C#. C# wraps them in `SafeHandle` subclasses (`ConnectionHandle`, `TableHandle`) to ensure proper cleanup via the corresponding Rust free/drop functions. Rust-allocated strings returned across the FFI boundary are read and released by `NativeCall.ReadStringAndFree`, which copies the UTF-8 bytes and then calls `free_string`.

Query builders (`Query`, `VectorQuery`, `FTSQuery`, `HybridQuery`) own no native objects — they hold only a *borrowed* `IntPtr` to the parent `Table` (which must outlive the query) and store all builder parameters locally in managed fields. They make a single FFI call at execution time (e.g., `ToArrow()`, `ExplainPlan()`). Parameters are serialized to JSON and passed to a consolidated Rust FFI function (`query_execute` or `vector_query_execute`) that builds and executes the query in one shot. This matches the Python SDK's lazy builder pattern.

Streaming execution (`ToBatches()`) instead returns a native stream handle from `query_execute_stream` / `vector_query_execute_stream`. C# wraps it in an `AsyncRecordBatchReader` (`IAsyncEnumerable<RecordBatch>`) that pulls one batch at a time across the FFI boundary and must be disposed after use.

Data crosses the FFI boundary via the Arrow C Data Interface (zero-copy for Rust→C#, clone-and-pin for C#→Rust). Schemas and materialized results use `ArrowCDataHelper` for marshalling.

## Adding a new FFI function

1. Add the `#[unsafe(no_mangle)] pub extern "C" fn` in the appropriate Rust file under `pinvoke/src/` (edition 2024 requires `unsafe(...)` for `no_mangle`).
2. Add a matching `[DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]` declaration in the corresponding C# class.
3. For async Rust operations: the FFI fn takes a `FfiCallback` (3-arg: `result`, `error`, `user_data`) and an opaque `user_data: *mut c_void`. Use `crate::spawn` to dispatch the async work to the least-loaded runtime in the pool, and forward `user_data` unchanged to the callback. On the C# side, allocate a per-call `GCHandle` over a `TaskCompletionSource<IntPtr>` and pass `GCHandle.ToIntPtr(handle)` as `userData`. A single static `[UnmanagedFunctionPointer]` dispatcher (`NativeCall.Dispatch`) recovers the `TaskCompletionSource` from `user_data` and completes it. This eliminates per-call delegate allocation and avoids the callback-vs-completion race.

## Conventions

- All C# types are in the `lancedb` namespace (no sub-namespaces).
- Options/config classes go in `src/Options/`; operation result types in `src/Results/`; namespace operation responses in `src/Responses/`.
- Search builders cover plain (`Query`), vector (`VectorQuery`), full-text (`FTSQuery`), and hybrid (`HybridQuery`) search. Hybrid results are combined with rerankers in `src/Rerankers/` (`RRFReranker`, `MRRReranker`, `LinearCombinationReranker`).
- Mutating helpers such as `MergeInsertBuilder` follow the same lazy-builder-then-single-FFI-call pattern as the query builders.
- Strings crossing the FFI boundary are UTF-8 encoded byte arrays passed as `IntPtr`. On the Rust side, `ffi::to_string` converts `*const c_char` to an owned `String`. On the C# side, `Encoding.UTF8.GetBytes` is used before passing via `fixed` pointer.
- C# uses `unsafe` blocks with `fixed` for pinning byte arrays during FFI calls.
- Rust FFI functions that return heap objects use `Arc::into_raw`; the caller is responsible for eventually calling the matching free function to avoid leaks.

## Test Projects

- **C# tests** live in `tests/` as an xUnit project (`tests.csproj`).
  - Test naming convention: `MethodName_Scenario_ExpectedResult` (e.g., `Connect_ValidUri_ReturnsConnection`).
- **Rust tests** live in `pinvoke/tests/` as Cargo integration tests.
  - Drive the FFI surface directly using raw pointers and the `FfiCallback` ABI to verify ownership, memory safety, and the `user_data` dispatch path.
  - Async FFI functions are exercised through real callbacks: each test allocates an `FfiTestContext` (per-call mailbox) and passes `ctx.user_data()` as the `user_data` slot. `ctx.wait_success()` blocks on a `Condvar` until the callback fires. Tests run in parallel with no global lock.
  - Sync helpers in `tests/common/mod.rs` (e.g., `connect_sync`, `create_table_sync`) call upstream `lancedb` directly and exist only for test setup — never use them to assert behavior of the FFI under test.