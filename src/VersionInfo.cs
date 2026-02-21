namespace lancedb
{
    using System.Text.Json.Serialization;

    /// <summary>
    /// Information about a specific version of a table.
    /// </summary>
    public class VersionInfo
    {
        /// <summary>
        /// The version number.
        /// </summary>
        [JsonPropertyName("version")]
        public ulong Version { get; set; }

        /// <summary>
        /// The timestamp when this version was created (UTC, ISO 8601).
        /// </summary>
        [JsonPropertyName("timestamp")]
        public string Timestamp { get; set; } = string.Empty;

        /// <summary>
        /// Key-value metadata associated with this version.
        /// </summary>
        [JsonPropertyName("metadata")]
        public Dictionary<string, string> Metadata { get; set; } = new();
    }
}
