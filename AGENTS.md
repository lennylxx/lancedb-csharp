# AGENTS.md — lancedb-csharp

Guidance for AI coding agents working on this repo. Read **§3 Engineering guidelines** before you
touch FFI code, packaging, dependencies, or CI. Build and usage instructions are in
[README.md](README.md); contributor-level architecture is in [CONTRIBUTING.md](CONTRIBUTING.md).
Workflow skills live in `.agents/skills/`: `add-ffi-api`, `upgrade-lancedb`, `python-parity`.

---

## 1. Project at a glance

A C# SDK for LanceDB that wraps the upstream Rust `lancedb` crate via P/Invoke.

| Path | What it is |
|---|---|
| `pinvoke/` | Rust crate `lancedb-ffi` (cdylib + rlib, edition 2024, toolchain pinned in `rust-toolchain.toml`): `lib.rs`, `ffi.rs`, `macros.rs`, `connection.rs`, `table.rs`, `query.rs`. |
| `pinvoke/tests/` | Cargo integration tests for the FFI symbols; shared helpers in `common/mod.rs`. |
| `src/` | C# library, multi-targeting a .NET Standard target and a modern .NET target. Namespace `lancedb`, no sub-namespaces. Subfolders: `Options/`, `Results/`, `Responses/`, `Rerankers/`, `build/LanceDB.targets`. |
| `tests/` | xUnit project `tests/tests.csproj`. Test names follow `Method_Scenario_Expected`. |
| `benchmark/` | C# vector-search benchmark; usage and baseline numbers in `benchmark/README.md`. |
| `samples/ConsoleSample`, `samples/CloudSample` | Target the *minimum* supported .NET on purpose; keep them there. CI reuses ConsoleSample as the nupkg smoke test. |
| `lancedb-csharp.slnx` | The only solution file; a second solution file makes `dotnet build` discovery ambiguous. |
| `version.txt` | Single source of the package version. `Cargo.toml` keeps a dummy version. |

**Architecture essentials:**

- **Runtime pool:** `RUNTIME_POOL` in `pinvoke/src/lib.rs` holds `num_cpus::get_physical()` single-worker multi-thread Tokio runtimes. `crate::spawn` sends each task to the least-loaded runtime (`RUNTIME_LOAD` atomics). The comment above `RUNTIME_POOL` records the measured alternatives; keep it current.
- **Async FFI:** Rust signature `fn(..., completion: FfiCallback, user_data: *mut c_void)`; first statement `let user_data = UserData(user_data);`; errors via `ffi::callback_error`. C# calls `NativeCall.Async((cb, ud) => extern_fn(..., cb, ud))`; a static dispatcher maps the `GCHandle` in `user_data` back to its `TaskCompletionSource` (`RunContinuationsAsynchronously`).
- **Sync errors:** Rust calls `ffi::set_last_error` and returns null; C# calls `NativeCall.ThrowIfNullWithError`, which takes the message from `ffi_get_last_error`.
- **Pointers:** `ffi_borrow!` to borrow, `ffi_clone_arc!` for an owned Arc in async work, `ffi_free!` as the only drop. Connection and Table are `Arc::into_raw` pointers in `ConnectionHandle` / `TableHandle` SafeHandles. Strings are read with `NativeCall.ReadStringAndFree`.
- **Lazy query builders:** all builder state lives in C# and is serialized to one params JSON, executed by a single call (`query_execute` / `vector_query_execute` or their `_stream` variants). Streaming returns `AsyncRecordBatchReader`.
- **Data transfer:** Arrow C Data Interface (`ArrowCDataHelper`, `free_ffi_cdata`, `free_ffi_schema`). C#→Rust exports a deep clone (`CloneBatchForExport` + `EnableManagedMemoryExport`). Vectors are `f32` (`*const c_float` ↔ `float[]`).
- **Multi-field results:** `Box::into_raw` `#[repr(C)]` structs with a null-safe `*_free` (e.g. `FfiDeleteResult`, `FfiMergeResult`, `FfiIndexStats`).
- **Optional-value sentinels:** f64/f32 NaN, u32 `0`, i64 `<0` (so `0` is a real value), string null pointer, tri-state bool `-1/0/1`. Maps, lists, and structs cross as JSON via `sonic-rs`.
- **Exports:** modules are private; every `extern "C"` fn or struct must be listed in the `pub use` block of `lib.rs`.
- **C#-side features:** `HybridQuery`, the rerankers (RRF, MRR, LinearCombination), `Expr` (emits SQL strings), and `Search`/`VectorSearch` are implemented in C#.
- **Build orchestration:** the repo's build scripts invoke cargo and then dotnet; MSBuild does not build the Rust crate.
- **Platforms:** linux-x64, win-x64, osx-arm64. .NET Framework x64 consumers get the native library through `src/build/LanceDB.targets`.
- **CI and publishing:** the reusable CI build workflow (`configuration`, `publish` inputs) builds and tests on Linux, Windows, and macOS-arm64, packs, verifies package contents, smoke-tests the nupkg from a copy of ConsoleSample plus a .NET Framework job, and uploads the artifact when publishing. The publish workflow runs on a published GitHub release (or manual dispatch), builds in Release, and pushes to NuGet using `version.txt` at the tag.

## 2. Working rules

**Git**
- Commit, push, or branch only when explicitly asked; "commit", "push", and "push and watch" are separate requests.
- Commit directly on `main`; don't create feature branches unprompted.
- "Commit the staged changes" means exactly what is staged; write the message from `git diff --cached`.
- Keep every line of the message, including subject, body, and bullets, at most 72 characters; measure it.
- Scale length to the change: a small or mechanical change gets one tight subject line; a real feature gets an imperative subject, a blank line, then a body covering the key behaviors and the why, with bullets for supporting changes.
- No trailers of any kind (no `Co-authored-by`, no session or tool attribution), even if the runtime adds one by default; the body ends with its last content line.
- Show the exact message as a `git commit -F - <<'EOF'` heredoc and commit only after the user approves that exact text; show any revision again.
- Describe the user-facing benefit; leave out verification narratives, Python versions or PR numbers, the Rust benchmark, numbered stage labels, and bug numbers.
- One concern per commit: upgrades before fixes; a test whose expectations change ships with the code that changes them.
- Leave untracked local files (notes, plans, release-note drafts) alone and uncommitted. Change `version.txt` only in a bump commit.
- CI workflow files are normal tracked code. Everything else in the GitHub config folder is private: never stage it, `git add -f` it, or reference it anywhere.
- Use `git --no-pager`.

**Code**
- TDD: write the failing test first and observe it fail.
- Build and test through the repo's scripts, which sequence the Rust and .NET builds correctly.
- Mirror the upstream Python **source** at the matching tag (not the docs site), with C# naming (PascalCase, `async Task`, properties instead of getters) so Python users can migrate directly.
- Every public API has XML docs mirroring the Python docstring (`<summary>/<param>/<returns>/<remarks>`), with C# terms (`list` → `IEnumerable`, `dict` → `Dictionary`, `None` → `null`). VS Code hovers don't render HTML lists: use `<list type="bullet">` or plain dashes, never `<ul>`/`<li>`.
- Braces on every `if`/`else`.
- Zero warnings in Rust and C#, including xUnit analyzers (e.g. `Assert.Single` over `Assert.Equal(1, x.Count)`).
- House style: `_camelCase` private fields, no `this.`, block-scoped namespace with `using`s inside, no `GlobalUsings.cs`, no license headers; member order DllImports, fields, ctors, properties, public, private.
- Prefer enums and typed `#[repr(C)]` structs over magic strings or encoded values. One result type per file in `src/Results/`; options in `src/Options/`. Descriptive FFI parameter names (e.g. `column_sqlexprs_json`).
- Add new classes, enums, config knobs, or files only with a proven need.
- Change formatting only on lines you are already editing: no `dotnet format` sweeps, EOL/charset resaves, or new analyzers.
- No narrative comments in CI YAML or config files; never name a script in a code comment; no "Phase X" / "Group X" labels in code, comments, or commits.
- Comments and docs must describe the code's actual behavior: update XML docs, README.md, and CONTRIBUTING.md in the same change.
- Avoid breaking changes against the last released NuGet unless approved; check signatures at the released tag.

**Scope and evidence**
- In scope: local and object-store usage. Out of scope: LanceDB Cloud and remote-only APIs (the `remote` feature, `PrewarmData`, `rename_table`, `namespace_client`), deprecated Python APIs, embeddings, API-based rerankers (external SDKs) and ML rerankers (ML runtime). State the reason when omitting an API.
- Claim "matches Python" only after running the pinned Python release; call something a defect or "transient" only after reproducing it on fresh binaries; treat subagent reports as hypotheses until verified.
- For large parity work, present a plan and get approval first. When the user says "stop", stop immediately.

## 3. Engineering guidelines

### 3.1 Building and testing

- After any change, rebuild the same configuration before running tests; the test runner uses `--no-build` and otherwise loads the previous binaries.
- Check that the test count changed by the expected amount, and scan the build output for `error CS` — a compile error in only one target framework leaves the old DLL in place.
- Before claiming done or committing, run the full Rust and C# suites and never commit with failing tests; `cargo build` does not compile `pinvoke/tests`.
- Before running anything in Release (benchmark, pack), rebuild the Release configuration; the default build produces Debug only. If a native crash follows an FFI change, check the timestamp of the native library build output first.
- After renaming or relinking the native library, clear `src/bin src/obj tests/bin tests/obj`; use `cargo clean -p lancedb-ffi` when cargo reports success without relinking.
- Tests that start native background work (e.g. LSM/MemWAL writers) must close it before cleanup; the fixture deletes directories with retry (`tests/TestFixture.cs`).
- Tests against cloud URIs must not wait on `object_store` retries; measure slow tests with the TRX logger before optimizing.

### 3.2 FFI

- Never panic across `extern "C"` (the release profile uses `panic=abort`): no `unwrap`/`expect`/`assert!`/`unwrap_or_default` on inputs. Report sync errors with `set_last_error` + null, async errors with `callback_error`, including every JSON parse.
- Copy all inputs into owned Rust data before `crate::spawn`; C# buffers are pinned only for the duration of the call.
- Pass `user_data.as_ptr()` into spawned futures, never `user_data.0` (disjoint capture makes the future non-`Send`).
- Only `*_free` functions consume a pointer; borrow paths use `ffi_borrow!`, never `Arc::from_raw`.
- Return multi-field results as `Box::into_raw` `#[repr(C)]` structs with a null-safe `*_free`; never pass stack memory through a callback. Keep the Rust struct and its C# `[StructLayout(Sequential)]` twin in identical field order.
- Keep callbacks and tests free of global state; correlate each call through its own `user_data`/`GCHandle`.
- The native→managed dispatcher always completes the TCS and frees the handle (try/catch/finally), so an await can never hang.
- Use `.ConfigureAwait(false)` on every library `await`.
- Put `[MarshalAs(UnmanagedType.U1)]` on every DllImport bool; Rust `bool` is one byte.
- Apply sentinels exactly (i64 `<0` = None) and test the boundary value (e.g. a zero timeout).
- Arrow C Data: `from_ffi` consumes the array but only borrows the schema; take ownership with `ptr::read` and zero the source; free each structure exactly one time. Keep all marshalling in `ArrowCDataHelper`.
- Export user batches only through `CloneBatchForExport`; the exporter transfers ownership of native-owned buffers.
- Reattach top-level schema metadata after `RecordBatch::from(StructArray)` and test the round-trip.
- Emit JSON that matches the exact Rust serde type (an `Option<String>` is a string, not an object). Build upstream types such as `FtsQuery` through their builder APIs, and test that every field round-trips.
- When an FFI signature changes, update every DllImport, caller, and Rust test in the same change — grep both `src/` and `pinvoke/tests/`; a mismatched P/Invoke compiles but corrupts memory.
- Add only protections C# can actually trigger; C# never passes a null handle.

### 3.3 Tests

- Each new or changed `extern "C"` fn gets a `pinvoke/tests/` test that calls that symbol (async via `common::FfiTestContext`: `ctx.user_data()`, `ctx.wait_success()`). Sync helpers in `common/mod.rs` are for setup only; no `#[cfg(test)]` in `pinvoke/src`.
- Every Rust error path gets a Rust test, plus a C# test asserting `LanceDbException` when C# can reach it.
- Assert the error at the moment upstream raises it (build vs execute) and with upstream's real message; read the source to find both.
- No reflection or `InternalsVisibleTo` in tests.
- Parity fixtures must have no ties (distinct scores and distances); C# `Array.Sort` is unstable while numpy `argsort` is stable.
- Take expected values from an actual run of the pinned Python release; compare floats with `Math.Abs(d) < 1e-3f`, not `Assert.Equal(double, double, precision)`, which rounds both values.
- Adjust a fixture rather than production code when the fixture is ambiguous.
- Verify every index parameter and default against the locked Rust crate and the Python source (e.g. `minimum_nprobes` defaults to 20, so a lower `MaximumNprobes` errors).
- Mirror upstream paging semantics exactly (e.g. `list_tables` skips the token item) and test with enough tables to cross a page.

### 3.4 C# API and query builders

- When adding a builder field, carry it through every copy/promotion path (`CopyVectorParams`, the `HybridQuery` constructors) and add a promotion test.
- `HybridQuery` reimplements Rust's `LanceHybridQuery` by hand; match it explicitly: 1-based RRF rank, offset before limit, sub-queries fetch `limit + offset`, normalize scores before reranking, strip `_rowid` unless requested, union result columns.
- Implement reranker formulas exactly as the pinned Python release does; they change between releases.
- `IndexType` ints are kept in sync in six places: the C# enum, `IndexTypeJsonConverter` (`src/IndexType.cs`), `ffi_to_index_type`, `index_type_to_ffi`, `build_index`, and `pinvoke/tests/table_tests.rs`. Append new values; never renumber.
- `Expr` emits SQL strings; probe what Lance's parser accepts (cast type names such as `BOOLEAN`, `INT`, `BIGINT`, `STRING`, `DATE`; struct field access) with a throwaway test first. Don't overload `==` on `Expr`.
- Check how Python's binding layer composes Rust calls before wiring one directly (e.g. Python `restore(version)` = Rust `checkout(version)` + `restore()`; `ReplaceFieldMetadata` takes Lance field IDs, not Arrow positions).
- Apache.Arrow C#: use `FloatType.Default`, `FixedSizeListArray.Builder(Field|IArrowType, int)`, `System.Array.Empty` (fully qualified), `Convert.ToInt32` for boxed values; alias `Apache.Arrow.Table` vs `lancedb.Table`. Copy builder code from existing tests.
- Keep the .NET Standard target compiling: no `Array.Fill`, `GetValueOrDefault`, `Marshal.PtrToStringUTF8`, `ConvertAll`, covariant returns, or `UnmanagedCallersOnly`.
- Write README examples against real public signatures and compile or run them before publishing.

### 3.5 Dependencies and upgrades

- Keep every `arrow-*` dependency on the same major as lancedb, and pin `lance-namespace` exactly (`=`) to lancedb's workspace version; mismatches produce duplicate Arrow types.
- Depend on lancedb's re-exports (e.g. `lancedb::Session`) rather than adding `lance`/`lance-io` directly; `lance-namespace` is the only extra direct dependency.
- Never run a bare `cargo update`. Edit `Cargo.toml`, build, and review `git diff --stat pinvoke/Cargo.lock`; keep churn minimal. Fix a transitive version with `cargo update -p <crate> --precise <ver>`.
- Read upstream source at the version locked in `pinvoke/Cargo.lock` (`~/.cargo/registry/src/index.crates.io-*/<crate>-<ver>/`).
- Silence upstream deprecations with `#[allow(deprecated)]` plus a noted follow-up, to keep zero warnings.
- `main` uses crates.io dependencies only; keep git or beta dependencies on a branch until the release lands, then land the upgrade and the feature as separate commits.

### 3.6 Packaging and NuGet

- Native libraries are `Content` items, which are `Exists()`-conditional. Always pass `-c` to `dotnet pack` and inspect the nupkg with `unzip -l`: expect `lib/<each target framework>/lancedb.dll`, `build/LanceDB.targets`, and `runtimes/{linux-x64,win-x64,osx-arm64}/native/*`.
- Use `/` and full file names in `PackagePath` (e.g. `build/LanceDB.targets`).
- Read `version.txt` in a csproj property group, not inside a target.
- Never add a global NuGet source; run `dotnet nuget locals all --clear` before re-testing a re-packed version.
- Smoke tests consume the nupkg from a *copy* of ConsoleSample, using a shared nuget.config with package-source mapping (`LanceDB` → `%LANCEDB_LOCAL_FEED%`) so they never resolve the published package.
- Fix .NET Framework smoke issues in the source or sample (e.g. `LangVersion latest`, `Substring` instead of ranges), not in CI.
- Ship only Release natives (strip, LTO, `codegen-units=1`, `panic=abort`, tokio `rt-multi-thread` only); don't use `opt-level="z"`.

### 3.7 CI and GitHub

- On a failed run, start from the first failing job (`gh run view <id> --job <job-id> --log-failed`; a failing matrix job cancels the others), reproduce locally on fresh binaries, and fix at the source (code, test, or sample) before touching workflow YAML.
- Confirm run outcomes with `gh run view <id> --json conclusion,jobs`; `gh run watch` can exit 0 for a failed run.
- Every Rust-building job needs protoc via `taiki-e/install-action` (`tool: protoc`).
- For Node runtime deprecations, read `runs.using` in each candidate tag's `action.yml` and pick the lowest major on the newest supported runtime.
- Windows LNK1102 (linker out of memory) is intermittent: re-run first; if it persists, propose `rust-lld` or `[profile.dev] debug = "line-tables-only"`.
- Leave the Windows cargo cache configuration as is unless asked.
- Run `git check-ignore -v <path>` before relying on a new file in the GitHub config folder; only workflows are un-ignored. Ask instead of `git add -f`.
- The agent's gh token cannot create releases; write release notes to an untracked file for the user to publish in the web UI.

### 3.8 Releases and benchmarks

- The user picks the version number; a bump commit changes only `version.txt` (and the README `PackageReference` if shown), subject `Bump version to X.Y.Z`. Removals or renames in `src/` since the last tag need a major.
- Release notes follow the previous release's format (`gh release view vPREV`): `### New Features` / `### Bug Fixes` / `### Improvements`, `#### <Area>` subheadings, each bullet ending in the bare short commit hash in parentheses. Describe user-facing benefits only; no emoji or test details.
- Benchmark only on fresh Release natives, an idle machine, and datasets regenerated after any lancedb or `Cargo.lock` change. Take several runs per variant and compare QPS together with p50/p99.
- Compare the C# SDK against the Rust `lancedb` crate, not raw `lance`. Never claim C# beats native; round ratios within noise.
- After an upgrade, re-baseline by updating only the numbers in `benchmark/README.md` (`Update benchmark results for lancedb X.Y.Z`).
- Record measured alternatives in the code comment next to the choice (e.g. above `RUNTIME_POOL`).

### 3.9 Local git and tooling

- Revert with `git restore --source=HEAD --staged --worktree <f>`; `git checkout -- <f>` restores from the index.
- Use `git stash apply` or a backup branch rather than `stash pop`; back up with commits or branches, never ad-hoc copies.
- After large edits, scan `git diff` for unintended `-///` lines; anchor edits on unique names.
- Check `git config user.name` before committing in another repository.
- For upstream source, use the local cargo registry, `curl https://raw.githubusercontent.com/lancedb/lancedb/<tag>/python/...`, and `curl -H "User-Agent: x" https://crates.io/api/v1/crates/lancedb` (crates.io requires a User-Agent). Rust tags are `vX.Y.Z`; Python tags are `python-vX.Y.Z`.
- Verify Python behaviour in a throwaway venv inside the repo or session folder with the pinned lancedb release, not system Python.

## 4. Design decisions

| Decision | Why |
|---|---|
| Pool of single-worker runtimes, one per physical core, least-loaded dispatch | Best throughput and tail latency measured. A single shared runtime contends on its injection queue; round-robin ignores load (high p99); current-thread runtimes deadlock because nothing polls them; more workers or logical-core sizing adds context switching; P2C, affinity, and sticky hashing gave no gain. |
| Dispatch policy as plain in-source code | No `DispatchStrategy` enum or runtime config; nothing needs to switch it. |
| System allocator | jemalloc gave no measurable gain and required a TLS workaround. |
| Arrow C Data Interface across FFI | Zero-copy, no IPC serialization. |
| Lazy JSON-param query builders with one execute call | Simpler FFI surface than per-method native builders. |
| Hand-written async FFI functions | Each async fn returns a different payload; a generic `ffi_async!` macro is too rigid. |
| Copy-and-free strings via `NativeCall.ReadStringAndFree` | A string SafeHandle is the wrong lifetime model for copied strings. |
| Delegate callbacks, not `[UnmanagedCallersOnly]` | Not available on the .NET Standard target; a dual path would add ~400 LOC. |
| Treat perf deltas within ±2–5% as noise | Run-to-run variance; sub-1% regressions are accepted and re-baselined rather than bisected. |
| `version.txt` + csproj properties as the version source | No `Directory.Build.props`, nuspec, or Cargo version coupling. |
| Smoke test the real nupkg via a ConsoleSample copy and package-source mapping | Exercises the shipped package without sentinel versions or duplicated projects. |
| No checked-in .NET Framework test project, no `global.json` | Can't build on Linux; `global.json` adds a burden on every SDK bump. |
| No StyleCop or repo-wide `dotnet format` | Conflicts with house style. |
| No `ILanceQuery` interface; keep `IReranker` | No covariant returns on the .NET Standard target; replacing `IReranker` needs a major version. |
| Keep released names and parameter order | Renames (e.g. `StandardQuery`) and reordering (`ToBatches`) break consumers. |
| Enums cross FFI as ints | Consistent across `IndexType`, `DistanceType`, `ApproxMode`. |
| osx-x64, embeddings, Cloud-only APIs not shipped | Out of scope with no proven need. |

## 5. Where to look

- [README.md](README.md): build instructions and the public API reference.
- [CONTRIBUTING.md](CONTRIBUTING.md): architecture, ownership model, adding an FFI function.
- `benchmark/README.md`: running the benchmark and baseline numbers.
- The comment above `RUNTIME_POOL` in `pinvoke/src/lib.rs`: measured dispatch alternatives.
