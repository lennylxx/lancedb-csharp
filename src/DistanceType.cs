namespace lancedb
{
    using System;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The distance metric used for vector search and index building.
    /// </summary>
    /// <remarks>
    /// The distance type used to build an index MUST match the distance type
    /// used to search the index. Failure to do so will yield inaccurate results.
    /// </remarks>
    [JsonConverter(typeof(DistanceTypeJsonConverter))]
    public enum DistanceType
    {
        /// <summary>
        /// Euclidean distance (L2). Accounts for both magnitude and direction.
        /// Range is [0, ∞).
        /// </summary>
        L2 = 0,

        /// <summary>
        /// Cosine distance. Not affected by the magnitude of the vectors.
        /// Range is [0, 2]. Undefined when one or both vectors are all zeros.
        /// </summary>
        Cosine = 1,

        /// <summary>
        /// Dot product distance. Range is (-∞, ∞). Equivalent to cosine distance
        /// when vectors are normalized (L2 norm is 1).
        /// </summary>
        Dot = 2,

        /// <summary>
        /// Hamming distance. Counts the number of differing dimensions.
        /// Useful for binary vectors.
        /// </summary>
        Hamming = 3,
    }

    /// <summary>
    /// JSON converter for <see cref="DistanceType"/> that serializes as lowercase
    /// strings matching the Rust <c>#[serde(rename_all = "lowercase")]</c> format.
    /// </summary>
    internal sealed class DistanceTypeJsonConverter : JsonConverter<DistanceType>
    {
        public override DistanceType Read(
            ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            string? value = reader.GetString();
            return value switch
            {
                "l2" => DistanceType.L2,
                "cosine" => DistanceType.Cosine,
                "dot" => DistanceType.Dot,
                "hamming" => DistanceType.Hamming,
                _ => throw new JsonException($"Unknown distance type: {value}"),
            };
        }

        public override void Write(
            Utf8JsonWriter writer, DistanceType value, JsonSerializerOptions options)
        {
            string s = value switch
            {
                DistanceType.L2 => "l2",
                DistanceType.Cosine => "cosine",
                DistanceType.Dot => "dot",
                DistanceType.Hamming => "hamming",
                _ => throw new JsonException($"Unknown distance type: {value}"),
            };
            writer.WriteStringValue(s);
        }
    }
}
