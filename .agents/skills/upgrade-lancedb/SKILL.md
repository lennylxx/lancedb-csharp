---
name: upgrade-lancedb
description: Use when bumping the upstream Rust lancedb crate (or lance-namespace / arrow) in pinvoke/Cargo.toml for lancedb-csharp, including adapting to API changes, keeping Cargo.lock churn minimal, surfacing new upstream APIs/fixes, and re-baselining the benchmark.
---

# Upgrade the upstream lancedb crate

## Steps

1. **Research the target.**
   - Latest version: `curl -s -H "User-Agent: x" https://crates.io/api/v1/crates/lancedb` (crates.io requires a User-Agent).
   - Release notes: `gh release view vX.Y.Z --repo lancedb/lancedb` (Rust tags `vX.Y.Z`, Python tags `python-vX.Y.Z`). List new APIs, relevant fixes, deprecations, and new enum variants.
   - Read lancedb's workspace `Cargo.toml` at that tag for its `arrow` major and `lance-namespace` version.
2. **Edit only `pinvoke/Cargo.toml`.**
   - Bump `lancedb`, keeping the existing feature list (no `remote`).
   - Set every `arrow-*` dependency to lancedb's major.
   - Pin `lance-namespace` with `=` to lancedb's workspace version.
   - Use lancedb's re-exports instead of adding `lance` or `lance-io`.
   - Use crates.io releases on `main`; keep any beta or git dependency on a branch.
3. **Build.**
   - Let cargo resolve from the `Cargo.toml` change; don't run a bare `cargo update`.
   - Fix a transitive version with `cargo update -p <crate> --precise <ver>` after checking what the dependent crate needs.
   - Review `git --no-pager diff --stat pinvoke/Cargo.lock` and keep churn minimal.
4. **Adapt to API changes,** reading the source at the *locked* version (`~/.cargo/registry/src/index.crates.io-*/lancedb-<ver>/`):
   - Data input traits (e.g. `Scannable`): pass `Vec<RecordBatch>`; `merge_insert` takes `Box<dyn RecordBatchReader + Send>`.
   - New result fields: expose them in the `#[repr(C)]` struct and the C# result type together.
   - New enum variants: update all six `IndexType` sync points (C# enum, `IndexTypeJsonConverter`, `ffi_to_index_type`, `index_type_to_ffi`, `build_index`, `pinvoke/tests/table_tests.rs`), appending new ints.
   - Deprecations: `#[allow(deprecated)]` to keep zero warnings, and tell the user about the follow-up.
5. **Run the full Rust and C# suites on fresh binaries**; all pass with zero warnings.
6. **Adopt upstream behaviour changes** that affect results (e.g. reranker formulas, `max_batch_length`) with a regression test each; a test whose expectations change ships with its code change.
7. **Surface new APIs** with the `add-ffi-api` skill. Commit order: upgrade first, then features and fixes, each separately.
8. **Re-baseline the benchmark** (see AGENTS.md §3.8): regenerate the datasets, use fresh Release natives, update only the numbers in `benchmark/README.md` ("Update benchmark results for lancedb X.Y.Z").
9. **Update docs:** README.md feature list and anything in CONTRIBUTING.md the upgrade affects.
10. **Report and wait for "commit".** Subject like `Upgrade to lancedb X.Y.Z ...`, with `Cargo.lock` included. The `version.txt` bump is a separate commit, with a number the user chooses.

## Guidelines

- Same Arrow major as lancedb everywhere; mismatches create duplicate Arrow types and can break C Data.
- Exact `=` pin on `lance-namespace`.
- Read the locked upstream version, not the latest source.
- Rust tests call our FFI symbols, never lancedb directly; no `#[cfg(test)]` in `pinvoke/src`.
- Compare benchmark numbers only on regenerated data with Release natives.
