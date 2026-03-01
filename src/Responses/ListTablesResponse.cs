namespace lancedb
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Response from listing tables in a LanceDB database.
    /// </summary>
    /// <remarks>
    /// Contains the list of table names and an optional page token for pagination.
    /// When <see cref="PageToken"/> is not <c>null</c>, more results are available.
    /// Pass the token to the next <see cref="Connection.ListTables"/> call to retrieve
    /// the next page.
    /// </remarks>
    public class ListTablesResponse
    {
        /// <summary>
        /// The list of table names in the current page.
        /// </summary>
        [JsonPropertyName("tables")]
        public IReadOnlyList<string> Tables { get; set; } = System.Array.Empty<string>();

        /// <summary>
        /// An opaque token for retrieving the next page of results.
        /// </summary>
        /// <remarks>
        /// <c>null</c> indicates there are no more results. Pass this value as the
        /// <c>pageToken</c> parameter in the next call to <see cref="Connection.ListTables"/>
        /// to continue pagination.
        /// </remarks>
        [JsonPropertyName("page_token")]
        public string? PageToken { get; set; }
    }
}
