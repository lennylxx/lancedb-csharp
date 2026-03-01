namespace lancedb
{
    using System.Runtime.InteropServices;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Native FFI struct matching Rust FfiFragmentSummaryStats layout.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct FfiFragmentSummaryStats
    {
        public ulong Min;
        public ulong Max;
        public ulong Mean;
        public ulong P25;
        public ulong P50;
        public ulong P75;
        public ulong P99;
    }

    /// <summary>
    /// Native FFI struct matching Rust FfiFragmentStats layout.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct FfiFragmentStats
    {
        public ulong NumFragments;
        public ulong NumSmallFragments;
        public FfiFragmentSummaryStats Lengths;
    }

    /// <summary>
    /// Native FFI struct matching Rust FfiTableStats layout.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct FfiTableStats
    {
        public ulong TotalBytes;
        public ulong NumRows;
        public ulong NumIndices;
        public FfiFragmentStats FragmentStats;
    }

    /// <summary>
    /// Statistics about a LanceDB table.
    /// Returned by <see cref="Table.Stats"/>.
    /// </summary>
    public class TableStatistics
    {
        /// <summary>
        /// The total number of bytes in the table.
        /// </summary>
        [JsonPropertyName("total_bytes")]
        public ulong TotalBytes { get; }

        /// <summary>
        /// The number of rows in the table.
        /// </summary>
        [JsonPropertyName("num_rows")]
        public ulong NumRows { get; }

        /// <summary>
        /// The number of indices in the table.
        /// </summary>
        [JsonPropertyName("num_indices")]
        public ulong NumIndices { get; }

        /// <summary>
        /// Statistics on table fragments.
        /// </summary>
        [JsonPropertyName("fragment_stats")]
        public FragmentStatistics FragmentStats { get; }

        internal TableStatistics(FfiTableStats ffi)
        {
            TotalBytes = ffi.TotalBytes;
            NumRows = ffi.NumRows;
            NumIndices = ffi.NumIndices;
            FragmentStats = new FragmentStatistics(ffi.FragmentStats);
        }
    }

    /// <summary>
    /// Statistics about fragments in a LanceDB table.
    /// </summary>
    public class FragmentStatistics
    {
        /// <summary>
        /// The number of fragments in the table.
        /// </summary>
        [JsonPropertyName("num_fragments")]
        public ulong NumFragments { get; }

        /// <summary>
        /// The number of uncompacted (small) fragments in the table.
        /// A high value indicates the table would benefit from compaction via
        /// <see cref="Table.Optimize"/>.
        /// </summary>
        [JsonPropertyName("num_small_fragments")]
        public ulong NumSmallFragments { get; }

        /// <summary>
        /// Summary statistics on the number of rows in each fragment.
        /// </summary>
        [JsonPropertyName("lengths")]
        public FragmentSummaryStats Lengths { get; }

        internal FragmentStatistics(FfiFragmentStats ffi)
        {
            NumFragments = ffi.NumFragments;
            NumSmallFragments = ffi.NumSmallFragments;
            Lengths = new FragmentSummaryStats(ffi.Lengths);
        }
    }

    /// <summary>
    /// Summary statistics (min, max, mean, percentiles) for fragment row counts.
    /// </summary>
    public class FragmentSummaryStats
    {
        /// <summary>The minimum number of rows in any fragment.</summary>
        [JsonPropertyName("min")]
        public ulong Min { get; }

        /// <summary>The maximum number of rows in any fragment.</summary>
        [JsonPropertyName("max")]
        public ulong Max { get; }

        /// <summary>The mean number of rows per fragment.</summary>
        [JsonPropertyName("mean")]
        public ulong Mean { get; }

        /// <summary>The 25th percentile of rows per fragment.</summary>
        [JsonPropertyName("p25")]
        public ulong P25 { get; }

        /// <summary>The 50th percentile (median) of rows per fragment.</summary>
        [JsonPropertyName("p50")]
        public ulong P50 { get; }

        /// <summary>The 75th percentile of rows per fragment.</summary>
        [JsonPropertyName("p75")]
        public ulong P75 { get; }

        /// <summary>The 99th percentile of rows per fragment.</summary>
        [JsonPropertyName("p99")]
        public ulong P99 { get; }

        internal FragmentSummaryStats(FfiFragmentSummaryStats ffi)
        {
            Min = ffi.Min;
            Max = ffi.Max;
            Mean = ffi.Mean;
            P25 = ffi.P25;
            P50 = ffi.P50;
            P75 = ffi.P75;
            P99 = ffi.P99;
        }
    }
}
