namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Metadata about an existing index on a table.
    /// Returned by <see cref="Table.ListIndices"/>.
    /// </summary>
    public class IndexConfig
    {
        /// <summary>
        /// The name of the index.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = "";

        /// <summary>
        /// The type of the index.
        /// </summary>
        [JsonPropertyName("index_type")]
        public IndexType IndexType { get; set; }

        /// <summary>
        /// The columns covered by this index.
        /// </summary>
        [JsonPropertyName("columns")]
        public List<string> Columns { get; set; } = new();

        /// <summary>
        /// The UUID of the first segment of the index.
        /// </summary>
        /// <remarks>
        /// An index may be made up of multiple segments, each with their own UUID.
        /// This is the UUID of the first segment. <c>null</c> if it could not be
        /// determined (for example, for remote tables, which do not yet surface it).
        /// </remarks>
        [JsonPropertyName("index_uuid")]
        public string? IndexUuid { get; set; }

        /// <summary>
        /// The protobuf type URL, a precise type identifier for the index.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if unavailable (for example, for remote tables).
        /// </remarks>
        [JsonPropertyName("type_url")]
        public string? TypeUrl { get; set; }

        /// <summary>
        /// When the index was created, taken as the minimum creation time across
        /// all segments.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if unavailable, such as for indices created before creation
        /// timestamps were tracked, or for remote tables.
        /// </remarks>
        [JsonPropertyName("created_at")]
        public DateTimeOffset? CreatedAt { get; set; }

        /// <summary>
        /// The number of rows indexed, across all segments.
        /// </summary>
        /// <remarks>
        /// This is approximate and may include rows that have since been deleted.
        /// <c>null</c> if unavailable (for example, for remote tables).
        /// </remarks>
        [JsonPropertyName("num_indexed_rows")]
        public ulong? NumIndexedRows { get; set; }

        /// <summary>
        /// The number of rows in the table that are not yet covered by this index.
        /// </summary>
        /// <remarks>
        /// Computed as the table's total row count minus <see cref="NumIndexedRows"/>.
        /// Optimizing the index will fold these rows into it. <c>null</c> if
        /// unavailable (for example, for remote tables).
        /// </remarks>
        [JsonPropertyName("num_unindexed_rows")]
        public ulong? NumUnindexedRows { get; set; }

        /// <summary>
        /// The total size in bytes of all index files across all segments.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if size information is unavailable, such as for indices
        /// created before file sizes were tracked, or for remote tables.
        /// </remarks>
        [JsonPropertyName("size_bytes")]
        public ulong? SizeBytes { get; set; }

        /// <summary>
        /// The number of segments that make up the index.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if unavailable (for example, for remote tables).
        /// </remarks>
        [JsonPropertyName("num_segments")]
        public uint? NumSegments { get; set; }

        /// <summary>
        /// The on-disk index format version, taken from the first segment.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if unavailable (for example, for remote tables).
        /// </remarks>
        [JsonPropertyName("index_version")]
        public int? IndexVersion { get; set; }

        /// <summary>
        /// Index-type-specific details, serialized as a JSON string.
        /// </summary>
        /// <remarks>
        /// The shape of this JSON varies by index type. <c>null</c> if the details
        /// could not be produced (for example, no plugin available) or for remote
        /// tables.
        /// </remarks>
        [JsonPropertyName("index_details")]
        public string? IndexDetails { get; set; }
    }
}
