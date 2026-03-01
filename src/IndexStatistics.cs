namespace lancedb
{
    using System.Runtime.InteropServices;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Native FFI struct matching Rust FfiIndexStats layout.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct FfiIndexStats
    {
        public ulong NumIndexedRows;
        public ulong NumUnindexedRows;
        public int IndexType;
        public int DistanceType;
        public uint NumIndices;
    }

    /// <summary>
    /// Statistics about a specific index on a table.
    /// Returned by <see cref="Table.IndexStats"/>.
    /// </summary>
    public class IndexStatistics
    {
        /// <summary>
        /// The number of rows in the table that are covered by this index.
        /// </summary>
        [JsonPropertyName("num_indexed_rows")]
        public ulong NumIndexedRows { get; set; }

        /// <summary>
        /// The number of rows in the table that are not covered by this index.
        /// These are rows that haven't yet been added to the index.
        /// </summary>
        [JsonPropertyName("num_unindexed_rows")]
        public ulong NumUnindexedRows { get; set; }

        /// <summary>
        /// The type of the index (e.g. IvfPq, BTree, Bitmap, FTS).
        /// </summary>
        [JsonPropertyName("index_type")]
        public IndexType IndexType { get; set; }

        /// <summary>
        /// The distance type used by the index (e.g. L2, Cosine, Dot).
        /// Only present for vector indices; <c>null</c> for scalar and FTS indices.
        /// </summary>
        [JsonPropertyName("distance_type")]
        public DistanceType? DistanceType { get; set; }

        /// <summary>
        /// The number of parts this index is split into.
        /// Zero if not available.
        /// </summary>
        [JsonPropertyName("num_indices")]
        public uint NumIndices { get; set; }

        /// <summary>
        /// Parameterless constructor for JSON deserialization.
        /// </summary>
        public IndexStatistics() { }

        internal IndexStatistics(FfiIndexStats ffi)
        {
            NumIndexedRows = ffi.NumIndexedRows;
            NumUnindexedRows = ffi.NumUnindexedRows;
            IndexType = (IndexType)ffi.IndexType;
            DistanceType = ffi.DistanceType >= 0 ? (DistanceType?)ffi.DistanceType : null;
            NumIndices = ffi.NumIndices;
        }
    }
}
