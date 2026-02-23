namespace lancedb.benchmark;

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Table = lancedb.Table;

public static class Program
{
    // ==================== CONFIGURATION ====================

    private static int NumDatasets = 3;
    private static int RowsPerDataset = 1_000_000;
    private static int VectorDim = 768;
    private static int BatchSize = 100_000;

    private static int NumPartitions = 256;
    private static int NumSubVectors = 48;

    private static int NumQueries = 2_000;
    private static int NumWorkers = 16;
    private static int ConcurrentQueriesPerWorker = 4;
    private static int QueryK = 50;
    private static int QueryNprobes = 1;
    private static int QueryRefineFactor = 10;

    private static List<string> DatasetPaths = new();

    // ==================== ENTRY POINT ====================

    public static async Task Main(string[] args)
    {
        ParseArgs(args);

        if (DatasetPaths.Count == 0)
        {
            for (int i = 1; i <= NumDatasets; i++)
            {
                DatasetPaths.Add(Path.Combine(Path.GetTempPath(), $"lancedb_bench_{i}"));
            }
        }
        NumDatasets = DatasetPaths.Count;

        PrintHeader();

        // Step 1 & 2: Create datasets + indices
        var tables = new List<Table>();
        var connections = new List<Connection>();

        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 1: Loading/Creating Datasets");
        Console.WriteLine("=".PadRight(60, '='));

        for (int i = 0; i < NumDatasets; i++)
        {
            string path = DatasetPaths[i];
            Console.WriteLine($"\nDataset {i + 1}/{NumDatasets}: {path}");

            var conn = new Connection();
            await conn.Connect(path);
            connections.Add(conn);

            var names = await conn.TableNames();
            Table table;
            if (names.Contains("vectors"))
            {
                table = await conn.OpenTable("vectors");
                long count = await table.CountRows();
                if (count == RowsPerDataset)
                {
                    Console.WriteLine($"  Dataset exists with {count} rows - loading");
                }
                else
                {
                    Console.WriteLine($"  Dataset has {count} rows (expected {RowsPerDataset}) - recreating");
                    table.Dispose();
                    await conn.DropTable("vectors");
                    table = await GenerateDataset(conn, RowsPerDataset, VectorDim, BatchSize);
                }
            }
            else
            {
                Console.WriteLine("  Dataset not found - creating");
                table = await GenerateDataset(conn, RowsPerDataset, VectorDim, BatchSize);
            }

            tables.Add(table);
        }

        // Step 2: Create indices
        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 2: Loading/Creating Indices");
        Console.WriteLine("=".PadRight(60, '='));

        for (int i = 0; i < NumDatasets; i++)
        {
            Console.WriteLine($"\nIndex {i + 1}/{NumDatasets}...");
            var indices = await tables[i].ListIndices();
            if (indices.Count > 0)
            {
                Console.WriteLine("  Vector index already exists - skipping");
            }
            else
            {
                Console.WriteLine("  Creating vector index...");
                var sw = Stopwatch.StartNew();
                await tables[i].CreateIndex(
                    new[] { "vector" },
                    new IvfPqIndex
                    {
                        DistanceType = "l2",
                        NumPartitions = NumPartitions,
                        NumSubVectors = NumSubVectors,
                    });
                sw.Stop();
                Console.WriteLine($"  Done in {sw.Elapsed.TotalSeconds:F1}s");
            }
        }

        // Step 3: Generate queries
        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 3: Generating Queries");
        Console.WriteLine("=".PadRight(60, '='));

        var queries = GenerateQueries(NumQueries, VectorDim);

        // Step 4: Warmup
        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 4: Warmup Phase");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine($"\nExecuting {NumQueries} queries...");

        await RunQueries(tables, queries, warmup: true);

        // Step 5: Drop cache
        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 5: Dropping Page Cache");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine("\nDropping dataset files from kernel page cache...");

        for (int i = 0; i < NumDatasets; i++)
        {
            Console.WriteLine($"\n  Dataset {i + 1}/{NumDatasets}: {DatasetPaths[i]}");
            DropDatasetCache(DatasetPaths[i]);
        }

        // Step 6: Timed phase
        Console.WriteLine($"\n{"=".PadRight(60, '=')}");
        Console.WriteLine("Step 6: Timed Phase");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine($"\nExecuting {NumQueries} queries...");

        var overallSw = Stopwatch.StartNew();
        var latencies = await RunQueries(tables, queries, warmup: false);
        overallSw.Stop();

        // Step 7: Results
        Console.WriteLine();
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine("BENCHMARK RESULTS");
        Console.WriteLine("=".PadRight(60, '='));

        var stats = ComputeStatistics(latencies);
        double throughput = NumQueries / overallSw.Elapsed.TotalSeconds;

        Console.WriteLine();
        Console.WriteLine("Latency Statistics (seconds):");
        Console.WriteLine($"  Mean:   {stats.Mean:F6}");
        Console.WriteLine($"  Std:    {stats.Std:F6}");
        Console.WriteLine($"  Min:    {stats.Min:F6}");
        Console.WriteLine($"  Max:    {stats.Max:F6}");
        Console.WriteLine($"  p50:    {stats.P50:F6}");
        Console.WriteLine($"  p95:    {stats.P95:F6}");
        Console.WriteLine($"  p99:    {stats.P99:F6}");
        Console.WriteLine();
        Console.WriteLine($"Throughput: {throughput:F2} queries/sec");
        Console.WriteLine();
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine("Benchmark Complete!");
        Console.WriteLine("=".PadRight(60, '='));

        // Cleanup
        foreach (var t in tables) { t.Dispose(); }
        foreach (var c in connections) { c.Dispose(); }
    }

    // ==================== ARGUMENT PARSING ====================

    private static void ParseArgs(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-d":
                case "--dataset":
                    if (++i < args.Length)
                    {
                        foreach (var p in args[i].Split(',', StringSplitOptions.RemoveEmptyEntries))
                        {
                            DatasetPaths.Add(p);
                        }
                    }
                    break;
                case "--num-datasets":
                    if (++i < args.Length) { NumDatasets = int.Parse(args[i]); }
                    break;
                case "--rows-per-dataset":
                    if (++i < args.Length) { RowsPerDataset = int.Parse(args[i]); }
                    break;
                case "--vector-dim":
                    if (++i < args.Length) { VectorDim = int.Parse(args[i]); }
                    break;
                case "--batch-size":
                    if (++i < args.Length) { BatchSize = int.Parse(args[i]); }
                    break;
                case "--num-partitions":
                    if (++i < args.Length) { NumPartitions = int.Parse(args[i]); }
                    break;
                case "--num-sub-vectors":
                    if (++i < args.Length) { NumSubVectors = int.Parse(args[i]); }
                    break;
                case "--num-queries":
                    if (++i < args.Length) { NumQueries = int.Parse(args[i]); }
                    break;
                case "--num-workers":
                    if (++i < args.Length) { NumWorkers = int.Parse(args[i]); }
                    break;
                case "--concurrent-queries":
                    if (++i < args.Length) { ConcurrentQueriesPerWorker = int.Parse(args[i]); }
                    break;
                case "-k":
                case "--query-k":
                    if (++i < args.Length) { QueryK = int.Parse(args[i]); }
                    break;
                case "--nprobes":
                    if (++i < args.Length) { QueryNprobes = int.Parse(args[i]); }
                    break;
                case "--refine-factor":
                    if (++i < args.Length) { QueryRefineFactor = int.Parse(args[i]); }
                    break;
            }
        }
    }

    // ==================== DATASET GENERATION ====================

    private static async Task<Table> GenerateDataset(
        Connection conn, int numRows, int dim, int batchSize)
    {
        Console.WriteLine($"\n  Generating dataset: {numRows} rows, {dim} dimensions");

        int numBatches = numRows / batchSize;
        if (numBatches == 0) { numBatches = 1; batchSize = numRows; }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("vector",
                new FixedSizeListType(new Field("item", FloatType.Default, true), dim), true))
            .Build();

        Table? table = null;
        var rng = new Random();

        for (int b = 0; b < numBatches; b++)
        {
            int rowsThisBatch = (b == numBatches - 1) ? numRows - b * batchSize : batchSize;
            var batch = CreateVectorBatch(schema, rowsThisBatch, dim, rng);

            if (b == 0)
            {
                table = await conn.CreateTable("vectors", batch);
            }
            else
            {
                await table!.Add(batch);
            }

            Console.Write($"\r  Writing batches [{ProgressBar(b + 1, numBatches)}] {b + 1}/{numBatches}");
        }

        Console.WriteLine();
        return table!;
    }

    private static RecordBatch CreateVectorBatch(
        Apache.Arrow.Schema schema, int numRows, int dim, Random rng)
    {
        var valuesBuilder = new FloatArray.Builder();
        for (int r = 0; r < numRows; r++)
        {
            for (int d = 0; d < dim; d++)
            {
                valuesBuilder.Append((float)NextGaussian(rng));
            }
        }
        var valuesArray = valuesBuilder.Build();
        var listArray = new FixedSizeListArray(
            new FixedSizeListType(new Field("item", FloatType.Default, true), dim),
            numRows,
            valuesArray,
            ArrowBuffer.Empty,
            0);

        return new RecordBatch(schema, new IArrowArray[] { listArray }, numRows);
    }

    private static double NextGaussian(Random rng)
    {
        double u1 = 1.0 - rng.NextDouble();
        double u2 = 1.0 - rng.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Sin(2.0 * Math.PI * u2);
    }

    // ==================== QUERY GENERATION ====================

    private static double[][] GenerateQueries(int numQueries, int dim)
    {
        Console.WriteLine($"\nGenerating {numQueries} query vectors...");
        var sw = Stopwatch.StartNew();
        var rng = new Random();

        var queries = new double[numQueries][];
        for (int i = 0; i < numQueries; i++)
        {
            var q = new double[dim];
            for (int d = 0; d < dim; d++)
            {
                q[d] = NextGaussian(rng);
            }
            queries[i] = q;
        }

        sw.Stop();
        Console.WriteLine($"  Done in {sw.Elapsed.TotalSeconds:F2}s");
        return queries;
    }

    // ==================== QUERY EXECUTION ====================

    private static async Task<List<double>> RunQueries(
        List<Table> tables, double[][] queries, bool warmup)
    {
        string desc = warmup ? "Warmup queries" : "Timed queries";
        int total = queries.Length;
        int completed = 0;
        var latencies = new System.Collections.Concurrent.ConcurrentBag<double>();

        // Channel for distributing work items to workers
        var channel = System.Threading.Channels.Channel.CreateBounded<(int datasetIdx, double[] vector)>(
            new System.Threading.Channels.BoundedChannelOptions(NumWorkers * ConcurrentQueriesPerWorker)
            {
                SingleWriter = true,
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
            });

        // Start worker tasks — each worker pulls from the channel with limited concurrency
        var workers = new Task[NumWorkers];
        for (int w = 0; w < NumWorkers; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                var semaphore = new SemaphoreSlim(ConcurrentQueriesPerWorker);
                var inflight = new List<Task>();

                await foreach (var item in channel.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    await semaphore.WaitAsync().ConfigureAwait(false);
                    var captured = item;
                    inflight.Add(Task.Run(async () =>
                    {
                        try
                        {
                            double latency = await ExecuteQuery(
                                tables[captured.datasetIdx], captured.vector).ConfigureAwait(false);

                            if (!warmup)
                            {
                                latencies.Add(latency);
                            }

                            int done = Interlocked.Increment(ref completed);
                            if (done % 100 == 0 || done == total)
                            {
                                Console.Write(
                                    $"\r  {desc} [{ProgressBar(done, total)}] {done}/{total}");
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));
                }

                await Task.WhenAll(inflight).ConfigureAwait(false);
            });
        }

        // Feed work items into the channel
        for (int i = 0; i < total; i++)
        {
            await channel.Writer.WriteAsync((i % tables.Count, queries[i])).ConfigureAwait(false);
        }
        channel.Writer.Complete();

        await Task.WhenAll(workers).ConfigureAwait(false);
        Console.WriteLine();

        return latencies.ToList();
    }

    private static async Task<double> ExecuteQuery(Table table, double[] queryVector)
    {
        var sw = Stopwatch.StartNew();

        using var vq = table.Query()
            .NearestTo(queryVector)
            .Limit(QueryK)
            .Nprobes(QueryNprobes)
            .RefineFactor(QueryRefineFactor);

        var batch = await vq.ToArrow();
        sw.Stop();

        return sw.Elapsed.TotalSeconds;
    }

    // ==================== CACHE MANAGEMENT ====================

    private static void DropDatasetCache(string datasetPath)
    {
        if (!OperatingSystem.IsLinux())
        {
            Console.WriteLine("    Cache drop is only supported on Linux - skipping");
            return;
        }

        if (!Directory.Exists(datasetPath))
        {
            Console.WriteLine($"    Warning: Dataset path does not exist: {datasetPath}");
            return;
        }

        int fileCount = 0;
        long totalSize = 0;

        foreach (var filePath in Directory.EnumerateFiles(datasetPath, "*", SearchOption.AllDirectories))
        {
            try
            {
                var info = new FileInfo(filePath);
                totalSize += info.Length;
                DropFileCache(filePath, info.Length);
                fileCount++;
            }
            catch
            {
                // Ignore files we can't access
            }
        }

        Console.WriteLine(
            $"    Dropped {fileCount} files ({totalSize / 1024.0 / 1024.0 / 1024.0:F2} GB) from cache");
    }

    private static void DropFileCache(string filePath, long fileSize)
    {
        if (!OperatingSystem.IsLinux()) { return; }

        try
        {
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            int fd = (int)fs.SafeFileHandle.DangerousGetHandle();
            PosixFadvise(fd, 0, fileSize, 4); // POSIX_FADV_DONTNEED = 4
        }
        catch
        {
            // Best effort
        }
    }

    [System.Runtime.InteropServices.DllImport("libc", EntryPoint = "posix_fadvise")]
    private static extern int PosixFadvise(int fd, long offset, long len, int advice);

    // ==================== STATISTICS ====================

    private record struct Statistics(
        double Mean, double Std, double Min, double Max,
        double P50, double P95, double P99);

    private static Statistics ComputeStatistics(List<double> latencies)
    {
        if (latencies.Count == 0)
        {
            return new Statistics(0, 0, 0, 0, 0, 0, 0);
        }

        double n = latencies.Count;
        double mean = latencies.Sum() / n;
        double variance = latencies.Sum(x => (x - mean) * (x - mean)) / n;
        double std = Math.Sqrt(variance);

        latencies.Sort();
        double min = latencies[0];
        double max = latencies[^1];
        double p50 = latencies[(int)(n * 0.50)];
        double p95 = latencies[(int)(n * 0.95)];
        double p99 = latencies[(int)(n * 0.99)];

        return new Statistics(mean, std, min, max, p50, p95, p99);
    }

    // ==================== HELPERS ====================

    private static void PrintHeader()
    {
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine("Lance Vector Search Benchmark (C#)");
        Console.WriteLine("=".PadRight(60, '='));
        Console.WriteLine();
        Console.WriteLine("Configuration:");
        Console.WriteLine($"  Datasets: {NumDatasets}");
        Console.WriteLine($"  Rows per dataset: {RowsPerDataset}");
        Console.WriteLine($"  Vector dimensions: {VectorDim}");
        Console.WriteLine($"  Index: IVF_PQ (partitions={NumPartitions}, subvectors={NumSubVectors})");
        Console.WriteLine($"  Num queries: {NumQueries}");
        Console.WriteLine($"  Query parameters: k={QueryK}, nprobes={QueryNprobes}, refine_factor={QueryRefineFactor}");
        Console.WriteLine($"  Number of workers: {NumWorkers}");
        Console.WriteLine($"  Concurrent queries per worker: {ConcurrentQueriesPerWorker}");
    }

    private static string ProgressBar(int current, int total)
    {
        int width = 40;
        int filled = (int)((double)current / total * width);
        return new string('#', filled) + new string('-', width - filled);
    }
}
