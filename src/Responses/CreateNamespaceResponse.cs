namespace lancedb
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Response from <see cref="Connection.CreateNamespace"/>.
    /// </summary>
    /// <remarks>
    /// Contains the properties of the created namespace and an optional transaction identifier.
    /// </remarks>
    public class CreateNamespaceResponse
    {
        /// <summary>
        /// Properties after the namespace is created.
        /// </summary>
        /// <remarks>
        /// <c>null</c> if the server does not support namespace properties.
        /// Empty dictionary if properties are supported but none are set.
        /// </remarks>
        [JsonPropertyName("properties")]
        public Dictionary<string, string>? Properties { get; set; }

        /// <summary>
        /// Optional transaction identifier.
        /// </summary>
        [JsonPropertyName("transaction_id")]
        public string? TransactionId { get; set; }
    }
}
