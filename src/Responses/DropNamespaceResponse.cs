namespace lancedb
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Response from <see cref="Connection.DropNamespace"/>.
    /// </summary>
    /// <remarks>
    /// Contains the properties and transaction identifier(s) of the dropped namespace.
    /// </remarks>
    public class DropNamespaceResponse
    {
        /// <summary>
        /// Properties of the dropped namespace, if any.
        /// </summary>
        [JsonPropertyName("properties")]
        public Dictionary<string, string>? Properties { get; set; }

        /// <summary>
        /// Optional transaction identifier(s).
        /// </summary>
        [JsonPropertyName("transaction_id")]
        public IReadOnlyList<string>? TransactionId { get; set; }
    }
}
