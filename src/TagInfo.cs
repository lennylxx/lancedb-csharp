namespace lancedb
{
    using System.Text.Json.Serialization;

    /// <summary>
    /// Information about a tag on a table version.
    /// </summary>
    /// <remarks>
    /// Tags are similar to Git tags — they provide named references to specific
    /// versions of a table. Tagged versions are exempt from cleanup operations.
    /// To remove a version that has been tagged, you must first delete the tag.
    /// </remarks>
    public class TagInfo
    {
        /// <summary>The version number this tag points to.</summary>
        [JsonPropertyName("version")]
        public ulong Version { get; set; }

        /// <summary>The size of the manifest at this version.</summary>
        [JsonPropertyName("manifest_size")]
        public long ManifestSize { get; set; }
    }
}
