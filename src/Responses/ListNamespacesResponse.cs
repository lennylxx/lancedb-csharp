namespace lancedb
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Response from <see cref="Connection.ListNamespaces"/>.
    /// </summary>
    /// <remarks>
    /// Contains the list of child namespace names and an optional page token for pagination.
    /// When <see cref="PageToken"/> is not <c>null</c>, more results are available.
    /// Pass the token to the next <see cref="Connection.ListNamespaces"/> call to retrieve
    /// the next page.
    /// </remarks>
    public class ListNamespacesResponse
    {
        /// <summary>
        /// The list of child namespace names relative to the parent namespace.
        /// </summary>
        [JsonPropertyName("namespaces")]
        public IReadOnlyList<string> Namespaces { get; set; } = System.Array.Empty<string>();

        /// <summary>
        /// An opaque token for retrieving the next page of results.
        /// </summary>
        /// <remarks>
        /// <c>null</c> indicates there are no more results. Pass this value as the
        /// <c>pageToken</c> parameter in the next call to <see cref="Connection.ListNamespaces"/>
        /// to continue pagination.
        /// </remarks>
        [JsonPropertyName("page_token")]
        public string? PageToken { get; set; }
    }
}
