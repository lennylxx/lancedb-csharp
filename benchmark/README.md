# Lance Vector Search Benchmark (C#)

This is a C# port of the [Rust vector search benchmark](https://github.com/westonpace/one-million-iops/tree/main/vector_search_rs).

## Requirements

- .NET 10.0 or higher
- Linux (for page cache management)

## Usage

Run the benchmark with default settings:

```bash
dotnet run --project benchmark
```

### Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-d`, `--dataset` | auto-generated temp paths | Dataset paths (comma-separated or multiple `-d` flags) |
| `--num-datasets` | 3 | Number of datasets to create/use |
| `--rows-per-dataset` | 1,000,000 | Rows per dataset |
| `--vector-dim` | 768 | Vector dimensions |
| `--batch-size` | 100,000 | Batch size for writing |
| `--num-partitions` | 256 | Number of IVF partitions |
| `--num-sub-vectors` | 48 | Number of PQ sub-vectors |
| `--num-queries` | 2,000 | Number of queries to run |
| `--num-workers` | 16 | Number of worker threads |
| `--concurrent-queries` | 4 | Concurrent queries per worker |
| `-k`, `--query-k` | 50 | Top K results to return |
| `--nprobes` | 1 | Number of probes for IVF search |
| `--refine-factor` | 10 | Refine factor for PQ search |

### Examples

```bash
# Quick smoke test with small parameters
dotnet run --project benchmark -- \
  --num-datasets 1 --rows-per-dataset 10000 --vector-dim 32 \
  --num-partitions 4 --num-sub-vectors 4 --num-queries 20

# Full benchmark with custom dataset paths
dotnet run --project benchmark -- \
  -d /var/data/dataset1,/var/data/dataset2,/var/data/dataset3

# Tune concurrency
dotnet run --project benchmark -- --num-workers 32 --concurrent-queries 8
```

## Output

* CPU: Intel(R) Xeon(R) w5-2455X, 12 cores
* RAM: 64 GB

```bash
dotnet run --project benchmark --configuration Release -- --num-queries 10000 --num-workers 12
```

```
============================================================
BENCHMARK RESULTS
============================================================

Latency Statistics (seconds):
  Mean:   0.058186
  Std:    0.010549
  Min:    0.013553
  Max:    0.115520
  p50:    0.057748
  p95:    0.075706
  p99:    0.086672

Throughput: 821.20 queries/sec

============================================================
Benchmark Complete!
============================================================
```