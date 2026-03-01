# LanceDB C# SDK

A C# SDK for [LanceDB](https://lancedb.github.io/lancedb/) — the developer-friendly embedded vector database. This SDK wraps the official Rust [`lancedb`](https://crates.io/crates/lancedb) crate via P/Invoke, providing idiomatic C# async APIs with full feature parity to the [Python SDK](https://github.com/lancedb/lancedb/tree/main/python).

## Features

- **Connection management** — connect to local or cloud databases with configurable options
- **Table CRUD** — create, open, rename, clone, drop tables; add, update, delete, merge-insert rows
- **Vector search** — nearest-neighbor queries with distance metrics, nprobes, refine factor, multi-vector search
- **Full-text search** — FTS indexing with configurable tokenization, stemming, and stop words
- **Hybrid search** — combine vector and full-text search with reranking (RRF, Linear Combination, MRR)
- **Streaming results** — `ToBatches()` returns `AsyncRecordBatchReader` for memory-efficient iteration
- **Indexing** — BTree, Bitmap, LabelList, FTS, IVF-PQ, IVF-Flat, IVF-SQ, IVF-RQ, HNSW-PQ, HNSW-SQ
- **Schema management** — add, alter, drop columns; add null-filled columns from Arrow Schema
- **Versioning** — checkout, restore, list versions; tag management
- **Namespace management** — create, list, drop, describe namespaces for multi-tenant organization
- **Query introspection** — explain plans, analyze plans, output schemas
- **Direct row access** — `TakeQuery` builder with select and row ID support

## Performance

The native Rust layer uses a pool of Tokio runtimes (one per CPU core) with least-loaded dispatch to maximize async throughput across the FFI boundary.

Under concurrent workloads, the C# SDK achieves ~94% of native Rust throughput (if using `lancedb` crate), ~82% of native Rust throughput (if using `lance` crate). The difference comes from DataFusion query planning overhead in `lancedb`.

## Prerequisites

- [.NET 8.0 SDK](https://dotnet.microsoft.com) or later
- [Rust toolchain](https://rustup.rs/) 1.93.1

## Build

```bash
./build.sh
```

This builds the Rust native library (`lancedb_ffi`) first, then the C# project. The C# build copies the pre-built native library to the output directory.

## Test

```bash
./test.sh
```

Runs both Rust integration tests and C# xUnit tests. The Rust native library must be built first.

## Quick Start

```csharp
using lancedb;
using Apache.Arrow;
using Apache.Arrow.Types;

// Connect to a local database
var connection = new Connection();
await connection.Connect("/tmp/my_lancedb");

// Create a table with vector data
var vectorField = new Field("item", FloatType.Default, nullable: false);
var vectorType = new FixedSizeListType(vectorField, 128);
var schema = new Schema.Builder()
    .Field(new Field("id", Int32Type.Default, nullable: false))
    .Field(new Field("text", StringType.Default, nullable: false))
    .Field(new Field("vector", vectorType, nullable: false))
    .Build();

var table = await connection.CreateTable("documents",
    new CreateTableOptions { Schema = schema });

// Add data (as Apache Arrow RecordBatch)
await table.Add(batch);

// Create a vector index
await table.CreateIndex(new[] { "vector" }, new HnswSqIndex
{
    DistanceType = DistanceType.Cosine,
    NumPartitions = 4,
});

// Search (NearestTo accepts double[])
var results = await table.Query()
    .NearestTo(queryVector)
    .Limit(10)
    .Where("id > 5")
    .ToList();

// Full-text search
await table.CreateIndex(new[] { "text" }, new FtsIndex());
var ftsResults = await table.Query()
    .NearestToText("search terms")
    .Limit(10)
    .ToList();

// Hybrid search (vector + FTS)
var hybridResults = await table.Query()
    .NearestTo(queryVector)
    .NearestToText("search terms")
    .Rerank(new RRFReranker())
    .Limit(10)
    .ToArrow();

// Streaming results (memory-efficient)
using var reader = await table.Query()
    .NearestTo(queryVector)
    .Limit(100)
    .ToBatches();
await foreach (var batch in reader)
{
    // Process each batch incrementally
}

// Direct row access with TakeQuery builder
var rows = await table.TakeOffsets(new ulong[] { 0, 1, 2 })
    .Select(new[] { "id", "text" })
    .WithRowId()
    .ToArrow();

// Cleanup
table.Dispose();
connection.Dispose();
```

## API Reference

### Connection

```csharp
var connection = new Connection();
await connection.Connect(uri, options);          // Connect to a local/cloud database
await connection.ConnectNamespace(nsImpl, props); // Connect to a LanceDB namespace
bool open = connection.IsOpen();                 // Check connection state

// Table management
var table = await connection.OpenTable("name");
var table = await connection.CreateTable("name", recordBatch);
var table = await connection.CreateEmptyTable("name", options);
var response = await connection.ListTables();    // Paginated table listing
var names = await connection.TableNames();       // List table names
await connection.RenameTable("old", "new");      // Rename a table
await connection.DropTable("name");              // Drop a table
await connection.DropAllTables();                // Drop all tables
var table = await connection.CloneTable("target", sourceUri); // Clone a table

// Namespace management
await connection.CreateNamespace(new[] { "ns" });
var ns = await connection.ListNamespaces();
var info = await connection.DescribeNamespace(new[] { "ns" });
await connection.DropNamespace(new[] { "ns" });
```

### Table — Data Operations

```csharp
string name = table.Name;                        // Table name
var head = await table.Head(10);                 // First N rows
var all = await table.ToArrow();                 // All rows as RecordBatch
await table.Add(recordBatch);                    // Append data
await table.Add(recordBatch, "overwrite");       // Overwrite data
await table.Update(values, where);               // Update rows (SQL expressions)
await table.Delete("id > 10");                   // Delete rows
long count = await table.CountRows("id < 5");    // Count with optional filter
var schema = await table.Schema();               // Get Arrow schema
var stats = await table.Stats();                 // Get table statistics
string uri = await table.Uri();                  // Get table URI

// Merge insert (upsert)
await table.MergeInsert("id")
    .WhenMatchedUpdateAll()
    .WhenNotMatchedInsertAll()
    .WhenNotMatchedBySourceDelete()
    .Execute(newData);

// Direct row access (TakeQuery builder)
var batch = await table.TakeOffsets(offsets)
    .Select(new[] { "id", "text" })
    .WithRowId()
    .ToArrow();
var batch = await table.TakeRowIds(rowIds).ToArrow();

// Schema management
await table.AddColumns(new Dictionary<string, string>
{
    { "doubled", "id * 2" }                      // SQL expression
});
await table.AddColumns(arrowSchema);             // Add null-filled columns
await table.AlterColumns(alterations);
await table.DropColumns(new[] { "old_column" });
await table.ReplaceFieldMetadata("field", metadata);
bool usesV2 = await table.UsesV2ManifestPaths(); // Check manifest path version
await table.MigrateManifestPathsV2();            // Migrate to V2 manifest paths
var stats = await table.Optimize(cleanupOlderThan: TimeSpan.FromDays(7));
```

### Querying

```csharp
// Flat scan
var results = await table.Query()
    .Select(new[] { "id", "text" })
    .Where("id > 5")
    .Limit(10)
    .Offset(5)
    .WithRowId()
    .ToArrow();                                  // Returns RecordBatch

// Vector search
var results = await table.Query()
    .NearestTo(vector)
    .DistanceType(DistanceType.Cosine)
    .Nprobes(20)
    .RefineFactor(10)
    .DistanceRange(lowerBound: 0.0f, upperBound: 1.0f)
    .Limit(10)
    .ToList();                                   // Returns List<Dictionary>

// Multi-vector search
var results = await table.Query()
    .NearestTo(vector1)
    .AddQueryVector(vector2)
    .Limit(10)
    .ToArrow();

// Full-text search
var results = await table.Query()
    .NearestToText("search query")
    .Limit(10)
    .ToList();

// Full-text search on specific columns
var results = await table.Query()
    .NearestToText("search query", new[] { "title", "body" })
    .Limit(10)
    .ToList();

// Hybrid search (vector + FTS with reranking)
var results = await table.Query()
    .NearestTo(vector)
    .NearestToText("search query")
    .Rerank(new RRFReranker())                   // or LinearCombinationReranker, MRRReranker
    .Limit(10)
    .ToArrow();

// Hybrid search (direct)
var results = await table.HybridSearch("search query", vector)
    .Rerank(new LinearCombinationReranker(weight: 0.7f))
    .Limit(10)
    .ToArrow();

// Streaming results
using var reader = await table.Query()
    .Limit(1000)
    .ToBatches(maxBatchLength: 100);
await foreach (var batch in reader)
{
    // Process incrementally
}

// Query introspection
string plan = await query.ExplainPlan(verbose: true);
string analysis = await query.AnalyzePlan();
var outputSchema = await query.OutputSchema();
```

### Indexing

```csharp
// Scalar indexes
await table.CreateIndex(new[] { "id" }, new BTreeIndex());
await table.CreateIndex(new[] { "category" }, new BitmapIndex());
await table.CreateIndex(new[] { "tags" }, new LabelListIndex());

// Full-text index
await table.CreateIndex(new[] { "text" }, new FtsIndex
{
    WithPosition = true,
    Language = "English",
});

// Vector indexes
await table.CreateIndex(new[] { "vector" }, new IvfPqIndex { DistanceType = DistanceType.Cosine });
await table.CreateIndex(new[] { "vector" }, new IvfFlatIndex());
await table.CreateIndex(new[] { "vector" }, new IvfSqIndex());
await table.CreateIndex(new[] { "vector" }, new IvfRqIndex());
await table.CreateIndex(new[] { "vector" }, new HnswPqIndex());
await table.CreateIndex(new[] { "vector" }, new HnswSqIndex());

// Wait for index to finish building
await table.CreateIndex(new[] { "vector" }, new IvfPqIndex(),
    waitTimeout: TimeSpan.FromSeconds(60));

// Index management
var indices = await table.ListIndices();
var stats = await table.IndexStats("my_index");
await table.DropIndex("my_index");
await table.PrewarmIndex("my_index");
await table.WaitForIndex(new[] { "my_index" }, TimeSpan.FromSeconds(30));
```

### Versioning & Tags

```csharp
ulong version = await table.Version();
var versions = await table.ListVersions();
await table.Checkout(version);                   // Checkout by version
await table.Checkout("v1.0");                    // Checkout by tag
await table.CheckoutLatest();
await table.Restore();                           // Restore checked-out version
await table.Restore(version);                    // Restore specific version
await table.Restore("v1.0");                     // Restore by tag

// Tags
await table.CreateTag("v1.0", version);
await table.UpdateTag("v1.0", newVersion);
await table.DeleteTag("v1.0");
var tags = await table.ListTags();
ulong v = await table.GetTagVersion("v1.0");
```
