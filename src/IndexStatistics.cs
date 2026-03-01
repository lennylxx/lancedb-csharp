namespace lancedb
{
    using System.Runtime.InteropServices;

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
        public ulong NumIndexedRows { get; }

        /// <summary>
        /// The number of rows in the table that are not covered by this index.
        /// These are rows that haven't yet been added to the index.
        /// </summary>
        public ulong NumUnindexedRows { get; }

        /// <summary>
        /// The type of the index (e.g. IvfPq, BTree, Bitmap, Fts).
        /// </summary>
        public IndexType IndexType { get; }

        /// <summary>
        /// The distance type used by the index (e.g. L2, Cosine, Dot).
        /// Only present for vector indices; <c>null</c> for scalar and FTS indices.
        /// </summary>
        public DistanceType? DistanceType { get; }

        /// <summary>
        /// The number of parts this index is split into.
        /// Zero if not available.
        /// </summary>
        public uint NumIndices { get; }

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
