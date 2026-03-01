namespace lancedb
{
    using System;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The type of index built on a table column.
    /// </summary>
    [JsonConverter(typeof(IndexTypeJsonConverter))]
    public enum IndexType
    {
        /// <summary>IVF-Flat vector index.</summary>
        IvfFlat = 0,

        /// <summary>IVF-SQ (Scalar Quantization) vector index.</summary>
        IvfSq = 1,

        /// <summary>IVF-PQ (Product Quantization) vector index.</summary>
        IvfPq = 2,

        /// <summary>IVF-RQ (Residual Quantization) vector index.</summary>
        IvfRq = 3,

        /// <summary>IVF-HNSW-PQ vector index.</summary>
        IvfHnswPq = 4,

        /// <summary>IVF-HNSW-SQ vector index.</summary>
        IvfHnswSq = 5,

        /// <summary>BTree scalar index.</summary>
        BTree = 6,

        /// <summary>Bitmap scalar index.</summary>
        Bitmap = 7,

        /// <summary>Label list scalar index.</summary>
        LabelList = 8,

        /// <summary>Full-text search (inverted) index.</summary>
        FTS = 9,
    }

    /// <summary>
    /// JSON converter for <see cref="IndexType"/> that serializes using the
    /// same string format as the Rust LanceDB API (e.g. "IVF_PQ", "BTREE").
    /// </summary>
    internal sealed class IndexTypeJsonConverter : JsonConverter<IndexType>
    {
        public override IndexType Read(
            ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                return (IndexType)reader.GetInt32();
            }

            string? value = reader.GetString();
            return value switch
            {
                "IVF_FLAT" => IndexType.IvfFlat,
                "IVF_SQ" => IndexType.IvfSq,
                "IVF_PQ" => IndexType.IvfPq,
                "IVF_RQ" => IndexType.IvfRq,
                "IVF_HNSW_PQ" => IndexType.IvfHnswPq,
                "IVF_HNSW_SQ" => IndexType.IvfHnswSq,
                "BTREE" => IndexType.BTree,
                "BITMAP" => IndexType.Bitmap,
                "LABEL_LIST" => IndexType.LabelList,
                "FTS" => IndexType.FTS,
                _ => throw new JsonException($"Unknown index type: {value}"),
            };
        }

        public override void Write(
            Utf8JsonWriter writer, IndexType value, JsonSerializerOptions options)
        {
            string s = value switch
            {
                IndexType.IvfFlat => "IVF_FLAT",
                IndexType.IvfSq => "IVF_SQ",
                IndexType.IvfPq => "IVF_PQ",
                IndexType.IvfRq => "IVF_RQ",
                IndexType.IvfHnswPq => "IVF_HNSW_PQ",
                IndexType.IvfHnswSq => "IVF_HNSW_SQ",
                IndexType.BTree => "BTREE",
                IndexType.Bitmap => "BITMAP",
                IndexType.LabelList => "LABEL_LIST",
                IndexType.FTS => "FTS",
                _ => throw new JsonException($"Unknown index type: {value}"),
            };
            writer.WriteStringValue(s);
        }
    }
}
