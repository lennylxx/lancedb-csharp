namespace lancedb
{
    using System.Runtime.InteropServices;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The result of a merge insert (upsert) operation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct MergeResult
    {
        /// <summary>
        /// The commit version associated with the operation.
        /// A version of 0 indicates compatibility with legacy servers
        /// that do not return a commit version.
        /// </summary>
        [JsonPropertyName("version")]
        public ulong Version;

        /// <summary>
        /// Number of rows inserted during the merge operation.
        /// </summary>
        [JsonPropertyName("num_inserted_rows")]
        public ulong NumInsertedRows;

        /// <summary>
        /// Number of rows updated during the merge operation.
        /// </summary>
        [JsonPropertyName("num_updated_rows")]
        public ulong NumUpdatedRows;

        /// <summary>
        /// Number of rows deleted during the merge operation.
        /// This does not include rows that were updated (which are internally
        /// deleted and re-inserted).
        /// </summary>
        [JsonPropertyName("num_deleted_rows")]
        public ulong NumDeletedRows;

        /// <summary>
        /// Number of attempts performed during the merge operation.
        /// This includes the initial attempt plus any retries due to
        /// transaction conflicts. A value of 1 means the operation
        /// succeeded on the first try.
        /// </summary>
        [JsonPropertyName("num_attempts")]
        public uint NumAttempts;
    }
}
