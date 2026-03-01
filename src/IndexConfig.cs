namespace lancedb
{
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
    }
}
