namespace lancedb
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Response from <see cref="Connection.DescribeNamespace"/>.
    /// </summary>
    /// <remarks>
    /// Contains the properties (metadata) of the namespace.
    /// </remarks>
    public class DescribeNamespaceResponse
    {
        /// <summary>
        /// Properties (metadata) of the namespace.
        /// </summary>
        [JsonPropertyName("properties")]
        public Dictionary<string, string>? Properties { get; set; }
    }
}
