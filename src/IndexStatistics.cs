namespace lancedb
{
    using System.Text.Json.Serialization;

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
        /// The type of the index (e.g. "BTREE", "BITMAP", "IVF_PQ", "IVF_HNSW_PQ", "FTS").
        /// </summary>
        [JsonPropertyName("index_type")]
        public string IndexType { get; set; } = "";

        /// <summary>
        /// The distance type used by the index (e.g. "l2", "cosine", "dot").
        /// Only present for vector indices; <c>null</c> for scalar and FTS indices.
        /// </summary>
        [JsonPropertyName("distance_type")]
        public string? DistanceType { get; set; }

        /// <summary>
        /// The number of parts this index is split into.
        /// </summary>
        [JsonPropertyName("num_indices")]
        public uint? NumIndices { get; set; }
    }
}
