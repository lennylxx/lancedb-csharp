---
name: python-parity
description: Use when auditing or improving lancedb-csharp parity with the upstream LanceDB Python SDK — gap analysis of the public API surface, verifying behaviour/expected values by running Python, or reviewing C#-implemented features (HybridQuery, rerankers, Expr).
---

# Python parity: gap analysis and behaviour verification

## A. Gap analysis

1. **Pin the target release:** the Python release that ships the same Rust `lancedb` version as `pinvoke/Cargo.toml`. Fetch source at that tag, not the docs site:
   `curl -sL https://raw.githubusercontent.com/lancedb/lancedb/python-vX.Y.Z/python/python/lancedb/{db,table,query,merge,index}.py` (plus `rerankers/`, `_lancedb.pyi`).
2. **List the C# surface:** `grep -rhE "public .*\(" src/*.cs src/Options src/Results src/Rerankers`, and compare with the `pub use` exports in `pinvoke/src/lib.rs`.
3. **Classify each Python async API:** exists / missing, C#-only / missing, needs FFI / won't implement (remote/Cloud-only, deprecated, embeddings, ML runtime, external HTTP SDK — with a reason).
4. **Write a plan** in a local untracked file with P0/P1/P2 tables and get approval before implementing.
5. **Implement one item at a time** with the `add-ffi-api` skill; check signatures against the last released tag (`git show vPREV:src/Table.cs`) and avoid renames and parameter reorders.
6. **Track progress** in the local plan.

## B. Verify behaviour by running Python

1. Use a throwaway venv inside the repo or session folder with the pinned lancedb release.
2. Build the same fixture in Python and in the C# test, with distinct scores and distances so ordering is deterministic (C# `Array.Sort` is unstable; numpy `argsort` is stable).
3. Take expected values from the Python output; put the snippet and values in the C# test's doc comment.
4. Compare with `Assert.True(Math.Abs(expected - actual) < 1e-3f)`.
5. Treat a difference as a C# defect only when the pinned Python release behaves differently; formulas such as LinearCombination change between releases. Write the failing test first, then fix the code; adjust ambiguous fixtures rather than production code.
6. Remove your throwaway venv and probe tests when done; ask before deleting anything you didn't create.

## C. Review checklist for C#-only reimplementations

Python gets these from Rust's `LanceHybridQuery`; C# has them in `src/HybridQuery.cs` and `src/Rerankers/`:
- RRF uses a 1-based rank: `1/(rank+k)`.
- Offset, then limit; sub-queries fetch `limit + offset`.
- Min-max normalize `_distance` and `_score` before reranking, then restore originals; support `normalize="rank"`.
- Strip `_rowid` unless `WithRowId()`; merged schemas are the union of columns.
- `Select`, `DistanceRange`, and every builder field survive `VectorQuery`→`HybridQuery` promotion (`CopyVectorParams`).
- Reranker output contains `_relevance_score`.
- `Expr` emits SQL strings; probe Lance's parser (cast names, struct field access) with a throwaway test.

## Guidelines

- Source at the matching `python-vX.Y.Z` tag via raw.githubusercontent.com.
- Defaults, validation, and error timing come from the locked Rust crate source.
- Every skipped API has a stated reason.
- Cite Python in a test only after running it.
- Keep Python versions and PR numbers out of commits and comments.
