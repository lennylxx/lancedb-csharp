namespace lancedb
{
    using System.Runtime.InteropServices;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The result of an update operation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct UpdateResult
    {
        /// <summary>
        /// The number of rows updated.
        /// </summary>
        [JsonPropertyName("rows_updated")]
        public ulong RowsUpdated;

        /// <summary>
        /// The commit version associated with the operation.
        /// A version of 0 indicates compatibility with legacy servers
        /// that do not return a commit version.
        /// </summary>
        [JsonPropertyName("version")]
        public ulong Version;
    }
}
