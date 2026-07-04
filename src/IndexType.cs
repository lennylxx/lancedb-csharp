namespace lancedb
{
    using System;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The type of index built on a table column.
    /// </summary>
    /// <remarks>
    /// The underlying integer values are an internal FFI detail used only to
    /// communicate the index type across the native boundary; they must stay in
    /// sync with the Rust side. Do not persist or serialize these values as
    /// integers. The associated <see cref="IndexTypeJsonConverter"/> serializes
    /// each value as its string name (for example, "BTREE") instead.
    /// </remarks>
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

        /// <summary>IVF-HNSW-Flat vector index.</summary>
        IvfHnswFlat = 6,

        /// <summary>BTree scalar index.</summary>
        BTree = 7,

        /// <summary>Bitmap scalar index.</summary>
        Bitmap = 8,

        /// <summary>Label list scalar index.</summary>
        LabelList = 9,

        /// <summary>Full-text search (inverted) index.</summary>
        FTS = 10,

        /// <summary>
        /// FM-index scalar index for substring search.
        /// </summary>
        /// <remarks>
        /// Accelerates substring predicates such as <c>contains(col, 'needle')</c>.
        /// It matches arbitrary substrings of the raw bytes, unlike the tokenized
        /// <see cref="FTS"/> index.
        /// </remarks>
        Fm = 11,
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
                "IVF_HNSW_FLAT" => IndexType.IvfHnswFlat,
                "BTREE" => IndexType.BTree,
                "BITMAP" => IndexType.Bitmap,
                "LABEL_LIST" => IndexType.LabelList,
                "FTS" => IndexType.FTS,
                "FM" => IndexType.Fm,
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
                IndexType.IvfHnswFlat => "IVF_HNSW_FLAT",
                IndexType.BTree => "BTREE",
                IndexType.Bitmap => "BITMAP",
                IndexType.LabelList => "LABEL_LIST",
                IndexType.FTS => "FTS",
                IndexType.Fm => "FM",
                _ => throw new JsonException($"Unknown index type: {value}"),
            };
            writer.WriteStringValue(s);
        }
    }
}
