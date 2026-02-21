namespace lancedb
{
    using System.Text.Json;

    /// <summary>
    /// Base class for all index configurations.
    /// Create an instance of a specific index type to pass to
    /// <see cref="Table.CreateIndex"/>.
    /// </summary>
    public abstract class Index
    {
        internal abstract string IndexType { get; }
        internal abstract string ToConfigJson();
    }

    /// <summary>
    /// A BTree index on scalar columns.
    /// </summary>
    /// <remarks>
    /// The index stores a copy of the column in sorted order. A header entry is
    /// created for each block of rows (currently the block size is fixed at 4096).
    /// These header entries are stored in a separate cacheable structure (a btree).
    ///
    /// This index is good for scalar columns with mostly distinct values and does
    /// best when the query is highly selective. It works with numeric, temporal,
    /// and string columns.
    /// </remarks>
    public class BTreeIndex : Index
    {
        internal override string IndexType => "BTree";
        internal override string ToConfigJson() => "{}";
    }

    /// <summary>
    /// A Bitmap index stores a bitmap for each distinct value in the column.
    /// </summary>
    /// <remarks>
    /// This index works best for low-cardinality numeric or string columns,
    /// where the number of unique values is small (less than a few thousands).
    /// </remarks>
    public class BitmapIndex : Index
    {
        internal override string IndexType => "Bitmap";
        internal override string ToConfigJson() => "{}";
    }

    /// <summary>
    /// A LabelList index on <c>List&lt;T&gt;</c> columns.
    /// </summary>
    /// <remarks>
    /// Supports queries with <c>array_contains_all</c> and <c>array_contains_any</c>
    /// using an underlying bitmap index. Useful for tags, categories, keywords, etc.
    /// </remarks>
    public class LabelListIndex : Index
    {
        internal override string IndexType => "LabelList";
        internal override string ToConfigJson() => "{}";
    }

    /// <summary>
    /// A full-text search index on string columns.
    /// </summary>
    public class FtsIndex : Index
    {
        /// <summary>
        /// Whether to store the position of the token in the document.
        /// Setting this to <c>true</c> enables phrase queries but increases index size.
        /// Default is <c>false</c>.
        /// </summary>
        public bool WithPosition { get; set; } = false;

        /// <summary>
        /// The base tokenizer to use.
        /// - <c>"simple"</c>: splits text by whitespace and punctuation (default).
        /// - <c>"whitespace"</c>: splits text by whitespace only.
        /// - <c>"raw"</c>: no tokenization.
        /// </summary>
        public string BaseTokenizer { get; set; } = "simple";

        /// <summary>
        /// Whether to convert tokens to lower case. Default is <c>true</c>.
        /// </summary>
        public bool LowerCase { get; set; } = true;

        /// <summary>
        /// Whether to apply stemming. Default is <c>true</c>.
        /// </summary>
        public bool Stem { get; set; } = true;

        /// <summary>
        /// Whether to remove stop words. Default is <c>true</c>.
        /// </summary>
        public bool RemoveStopWords { get; set; } = true;

        /// <summary>
        /// Whether to apply ASCII folding (e.g. "café" → "cafe"). Default is <c>true</c>.
        /// </summary>
        public bool AsciiFolding { get; set; } = true;

        internal override string IndexType => "FTS";

        internal override string ToConfigJson()
        {
            var config = new
            {
                with_position = WithPosition,
                base_tokenizer = BaseTokenizer,
                lower_case = LowerCase,
                stem = Stem,
                remove_stop_words = RemoveStopWords,
                ascii_folding = AsciiFolding,
            };
            return JsonSerializer.Serialize(config);
        }
    }

    /// <summary>
    /// An IVF-PQ (Inverted File with Product Quantization) vector index.
    /// </summary>
    public class IvfPqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string? DistanceType { get; set; }

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Number of sub-vectors for PQ. Default is dimension / 16.
        /// </summary>
        public int? NumSubVectors { get; set; }

        /// <summary>
        /// Number of bits to encode each sub-vector. Only 4 and 8 are supported. Default is 8.
        /// </summary>
        public int? NumBits { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int? MaxIterations { get; set; }

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int? SampleRate { get; set; }

        internal override string IndexType => "IvfPq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>();
            if (DistanceType != null) { dict["distance_type"] = DistanceType; }
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (NumSubVectors.HasValue) { dict["num_sub_vectors"] = NumSubVectors.Value; }
            if (NumBits.HasValue) { dict["num_bits"] = NumBits.Value; }
            if (MaxIterations.HasValue) { dict["max_iterations"] = MaxIterations.Value; }
            if (SampleRate.HasValue) { dict["sample_rate"] = SampleRate.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-HNSW-PQ (Hierarchical Navigable Small World with Product Quantization) vector index.
    /// </summary>
    public class HnswPqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string? DistanceType { get; set; }

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Number of sub-vectors for PQ. Default is dimension / 16.
        /// </summary>
        public int? NumSubVectors { get; set; }

        /// <summary>
        /// Number of bits to encode each sub-vector. Only 4 and 8 are supported. Default is 8.
        /// </summary>
        public int? NumBits { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int? MaxIterations { get; set; }

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int? SampleRate { get; set; }

        /// <summary>
        /// The number of neighbors in the HNSW graph. Default is 20.
        /// </summary>
        public int? NumEdges { get; set; }

        /// <summary>
        /// The number of candidates during HNSW construction. Default is 300.
        /// </summary>
        public int? EfConstruction { get; set; }

        internal override string IndexType => "HnswPq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>();
            if (DistanceType != null) { dict["distance_type"] = DistanceType; }
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (NumSubVectors.HasValue) { dict["num_sub_vectors"] = NumSubVectors.Value; }
            if (NumBits.HasValue) { dict["num_bits"] = NumBits.Value; }
            if (MaxIterations.HasValue) { dict["max_iterations"] = MaxIterations.Value; }
            if (SampleRate.HasValue) { dict["sample_rate"] = SampleRate.Value; }
            if (NumEdges.HasValue) { dict["num_edges"] = NumEdges.Value; }
            if (EfConstruction.HasValue) { dict["ef_construction"] = EfConstruction.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-HNSW-SQ (Hierarchical Navigable Small World with Scalar Quantization) vector index.
    /// </summary>
    public class HnswSqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string? DistanceType { get; set; }

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int? MaxIterations { get; set; }

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int? SampleRate { get; set; }

        /// <summary>
        /// The number of neighbors in the HNSW graph. Default is 20.
        /// </summary>
        public int? NumEdges { get; set; }

        /// <summary>
        /// The number of candidates during HNSW construction. Default is 300.
        /// </summary>
        public int? EfConstruction { get; set; }

        internal override string IndexType => "HnswSq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>();
            if (DistanceType != null) { dict["distance_type"] = DistanceType; }
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (MaxIterations.HasValue) { dict["max_iterations"] = MaxIterations.Value; }
            if (SampleRate.HasValue) { dict["sample_rate"] = SampleRate.Value; }
            if (NumEdges.HasValue) { dict["num_edges"] = NumEdges.Value; }
            if (EfConstruction.HasValue) { dict["ef_construction"] = EfConstruction.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }
}
