namespace lancedb
{
    using System.Text.Json.Serialization;

    /// <summary>
    /// The result of an add columns operation.
    /// </summary>
    public struct AddColumnsResult
    {
        /// <summary>
        /// The commit version associated with the operation.
        /// A version of 0 indicates compatibility with legacy servers
        /// that do not return a commit version.
        /// </summary>
        [JsonPropertyName("version")]
        public ulong Version;
    }
}
