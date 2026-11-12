---
name: add-ffi-api
description: Use when adding or changing a public C# API that crosses the Rust FFI boundary in lancedb-csharp (new extern "C" fn in pinvoke/src, DllImport + wrapper in src/, tests on both sides, Python-mirrored XML docs). Also use when changing an existing FFI signature.
---

# Add or change an FFI-backed API

If the Python feature reduces to APIs that already exist (e.g. `Expr`, `Search`, reranker
logic), implement it in C# only and skip the Rust steps.

## Steps

1. **Classify the API.**
   - Read the Python source at the pinned tag:
     `curl -sL https://raw.githubusercontent.com/lancedb/lancedb/<tag>/python/python/lancedb/<table|query|db|merge>.py`.
   - Read the Rust source at the version locked in `pinvoke/Cargo.lock` (`~/.cargo/registry/src/index.crates.io-*/lancedb-<ver>/src/`).
   - Note the exact signature, defaults, validation, and when errors are raised (build vs execute).
   - Skip remote/Cloud-only, deprecated, and embeddings APIs; tell the user why.
2. **Write failing tests first and observe them fail.**
   - Rust, in `pinvoke/tests/<area>_tests.rs`: call the new `extern "C"` symbol; for async use `common::FfiTestContext` (`ctx.user_data()`, `ctx.wait_success()` / `wait_raw()`). Cover the happy path, every error path, and `*_free_null_is_safe` for each new free fn. Use `common` sync helpers only for setup.
   - C#, in `tests/<Area>Tests.cs` (`Method_Scenario_Expected`): cover the happy path, `LanceDbException` for each reachable Rust error, and builder chaining/promotion. No reflection or `InternalsVisibleTo`.
3. **Implement in Rust** (`connection.rs`, `table.rs`, or `query.rs`).
   - `#[unsafe(no_mangle)] pub extern "C" fn`.
   - Async: take `completion: FfiCallback` and trailing `user_data: *mut c_void`; first line `let user_data = UserData(user_data);`; get an owned handle with `ffi_clone_arc!`; copy every input into owned data (`ffi::to_string`, parsed JSON) before `crate::spawn`; use `user_data.as_ptr()` inside the future.
   - Sync: on failure call `ffi::set_last_error(..)` and return null.
   - Report every error (including JSON parsing) through `callback_error` / `set_last_error`; no `unwrap`, `expect`, or `unwrap_or_default` on inputs.
   - Optional values use sentinels (NaN, u32 `0`, i64 `<0`, null string, tri-state `-1`); maps/lists/structs travel as JSON (`sonic-rs`).
   - Multi-field results: `#[repr(C)]` struct via `Box::into_raw` with a null-safe `*_free`.
   - Add the fn and any struct to the `pub use` lists in `pinvoke/src/lib.rs`.
4. **Implement in C#.**
   - `[DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]` with snake_case names and descriptive parameter names; `[MarshalAs(UnmanagedType.U1)]` on every bool.
   - Async: `await NativeCall.Async((cb, ud) => extern_fn(..., cb, ud)).ConfigureAwait(false)`. Sync: `NativeCall.ThrowIfNullWithError`.
   - Strings via `NativeCall.ReadStringAndFree`; structs via `Marshal.PtrToStructure`, freed in `finally`, with `[StructLayout(Sequential)]` field order identical to Rust.
   - Options in `src/Options/`, results in `src/Results/`, one type per file. Prefer enums; append new enum ints, never renumber.
   - New query-builder fields: carry them through every copy/promotion path (`HybridQuery` constructors, `CopyVectorParams`) and serialize them into the params JSON with the exact serde shape.
   - XML docs mirror the Python docstring; `<list type="bullet">` for lists; braces on every `if`.
5. **When a signature changes,** grep `src/` and `pinvoke/tests/` and update every DllImport, caller, and Rust test together.
6. **Validate on freshly rebuilt binaries:** test counts rose by the expected amount, zero warnings, no `error CS` for the .NET Standard target.
7. **Update docs:** README.md API reference, and CONTRIBUTING.md when FFI conventions change.
8. **Report and stop.** Commit only when asked: on `main`, single-line subject, no trailer.

## Guidelines

- Keep the .NET Standard target compiling: no `Array.Fill`, `GetValueOrDefault`, `Marshal.PtrToStringUTF8`, `ConvertAll`, covariant returns, or `UnmanagedCallersOnly`.
- Export user batches through `ArrowCDataHelper.CloneBatchForExport`; Rust `from_ffi` consumes the array and only borrows the schema; free each structure exactly one time.
- Keep `IndexType` ints in sync in all six places: the C# enum, `IndexTypeJsonConverter`, `ffi_to_index_type`, `index_type_to_ffi`, `build_index`, `table_tests.rs`.
- Assert upstream's real error text.
- Add only protections that C# can trigger.
- Rebuild the Release configuration before running anything in Release.
