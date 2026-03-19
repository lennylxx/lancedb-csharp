namespace lancedb
{
    using System.Text.Json.Serialization;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The result of a delete operation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct DeleteResult
    {
        /// <summary>
        /// The commit version associated with the operation.
        /// A version of 0 indicates compatibility with legacy servers
        /// that do not return a commit version.
        /// </summary>
        [JsonPropertyName("version")]
        public ulong Version;

        /// <summary>
        /// The number of rows that were deleted.
        /// </summary>
        [JsonPropertyName("num_deleted_rows")]
        public ulong NumDeletedRows;
    }
}
