namespace lancedb
{
    using System.Text.Json.Serialization;

    /// <summary>
    /// Statistics about an optimization operation.
    /// </summary>
    /// <remarks>
    /// Returned by <see cref="Table.Optimize"/>. Contains optional statistics
    /// about file compaction and version pruning.
    /// </remarks>
    public class OptimizeStats
    {
        /// <summary>
        /// Statistics about file compaction, or <c>null</c> if compaction was not performed.
        /// </summary>
        [JsonPropertyName("compaction")]
        public CompactionStats? Compaction { get; set; }

        /// <summary>
        /// Statistics about version pruning, or <c>null</c> if pruning was not performed.
        /// </summary>
        [JsonPropertyName("prune")]
        public PruneStats? Prune { get; set; }
    }

    /// <summary>
    /// Statistics about a file compaction operation.
    /// </summary>
    public class CompactionStats
    {
        /// <summary>The number of fragments that have been overwritten.</summary>
        [JsonPropertyName("fragments_removed")]
        public int FragmentsRemoved { get; set; }

        /// <summary>The number of new fragments that have been added.</summary>
        [JsonPropertyName("fragments_added")]
        public int FragmentsAdded { get; set; }

        /// <summary>The number of files that have been removed, including deletion files.</summary>
        [JsonPropertyName("files_removed")]
        public int FilesRemoved { get; set; }

        /// <summary>The number of files that have been added.</summary>
        [JsonPropertyName("files_added")]
        public int FilesAdded { get; set; }
    }

    /// <summary>
    /// Statistics about a version pruning operation.
    /// </summary>
    public class PruneStats
    {
        /// <summary>The number of bytes freed by pruning old versions.</summary>
        [JsonPropertyName("bytes_removed")]
        public ulong BytesRemoved { get; set; }

        /// <summary>The number of old versions that were removed.</summary>
        [JsonPropertyName("old_versions")]
        public ulong OldVersions { get; set; }
    }
}
