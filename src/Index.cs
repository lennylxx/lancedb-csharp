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
    /// A Bitmap index stores a bitmap for each distinct value in the column
    /// for every row.
    /// </summary>
    /// <remarks>
    /// This index works best for low-cardinality numeric or string columns,
    /// where the number of unique values is small (i.e., less than a few thousands).
    /// Bitmap index can accelerate the following filters:
    /// <list type="bullet">
    /// <item><description><c>&lt;</c>, <c>&lt;=</c>, <c>=</c>, <c>&gt;</c>, <c>&gt;=</c></description></item>
    /// <item><description><c>IN (value1, value2, ...)</c></description></item>
    /// <item><description><c>BETWEEN (value1, value2)</c></description></item>
    /// <item><description><c>IS NULL</c></description></item>
    /// </list>
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
    /// <remarks>
    /// Creates a full-text search index that enables text search capabilities using
    /// BM25 scoring. Use with <see cref="QueryBase{T}.FullTextSearch"/> or
    /// <see cref="Query.NearestToText"/> to search indexed columns.
    /// </remarks>
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
        /// <list type="bullet">
        /// <item><description><c>"simple"</c> (default): splits text by whitespace and punctuation.</description></item>
        /// <item><description><c>"whitespace"</c>: splits text by whitespace only.</description></item>
        /// <item><description><c>"raw"</c>: no tokenization.</description></item>
        /// </list>
        /// </summary>
        public string BaseTokenizer { get; set; } = "simple";

        /// <summary>
        /// The language for stemming and stop words. Default is <c>"English"</c>.
        /// </summary>
        public string Language { get; set; } = "English";

        /// <summary>
        /// The maximum token length to index. Tokens longer than this are ignored.
        /// Default is 40.
        /// </summary>
        public int? MaxTokenLength { get; set; } = 40;

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

        /// <summary>
        /// Minimum ngram length for ngram tokenization. Default is 3.
        /// </summary>
        public int NgramMinLength { get; set; } = 3;

        /// <summary>
        /// Maximum ngram length for ngram tokenization. Default is 3.
        /// </summary>
        public int NgramMaxLength { get; set; } = 3;

        /// <summary>
        /// Whether to only generate prefix ngrams. Default is <c>false</c>.
        /// </summary>
        public bool PrefixOnly { get; set; } = false;

        internal override string IndexType => "FTS";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["with_position"] = WithPosition,
                ["base_tokenizer"] = BaseTokenizer,
                ["language"] = Language,
                ["lower_case"] = LowerCase,
                ["stem"] = Stem,
                ["remove_stop_words"] = RemoveStopWords,
                ["ascii_folding"] = AsciiFolding,
                ["ngram_min_length"] = NgramMinLength,
                ["ngram_max_length"] = NgramMaxLength,
                ["prefix_only"] = PrefixOnly,
            };
            if (MaxTokenLength.HasValue) { dict["max_token_length"] = MaxTokenLength.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-PQ (Inverted File with Product Quantization) vector index.
    /// </summary>
    /// <remarks>
    /// This index stores a compressed (quantized) copy of every vector. These
    /// vectors are grouped into partitions of similar vectors. Each partition
    /// keeps track of a centroid which is the average value of all vectors in
    /// the group.
    ///
    /// During a query the centroids are compared with the query vector to find
    /// the closest partitions. The compressed vectors in these partitions are
    /// then searched to find the closest vectors.
    ///
    /// The compression scheme is called product quantization. Each vector is
    /// divided into subvectors and then each subvector is quantized into a small
    /// number of bits. The parameters <see cref="NumBits"/> and
    /// <see cref="NumSubVectors"/> control this process, providing a tradeoff
    /// between index size (and thus search speed) and index accuracy.
    ///
    /// The partitioning process is called IVF and the <see cref="NumPartitions"/>
    /// parameter controls how many groups to create.
    ///
    /// Note that training an IVF PQ index on a large dataset is a slow operation
    /// and currently is also a memory intensive operation.
    /// </remarks>
    public class IvfPqIndex : Index
    {
        /// <summary>
        /// The distance metric used to train the index.
        /// Default is <c>"l2"</c>.
        /// </summary>
        /// <remarks>
        /// The distance type used to train an index MUST match the distance type
        /// used to search the index. Failure to do so will yield inaccurate results.
        /// <list type="bullet">
        /// <item><description><c>"l2"</c>: Euclidean distance. Accounts for both magnitude and direction.
        /// Range is [0, ∞).</description></item>
        /// <item><description><c>"cosine"</c>: Cosine distance. Not affected by the magnitude of the
        /// vectors. Range is [0, 2]. Undefined when one or both vectors are all zeros.</description></item>
        /// <item><description><c>"dot"</c>: Dot product. Range is (-∞, ∞). Equivalent to cosine distance
        /// when vectors are normalized (l2 norm is 1).</description></item>
        /// <item><description><c>"hamming"</c>: Hamming distance. Counts the number of differing
        /// dimensions. Useful for binary vectors.</description></item>
        /// </list>
        /// </remarks>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions to create.
        /// Default is the square root of the number of rows.
        /// </summary>
        /// <remarks>
        /// This value should generally scale with the number of rows in the dataset.
        /// If this value is too large then the first part of the search (picking the
        /// right partition) will be slow. If this value is too small then the second
        /// part of the search (searching within a partition) will be slow.
        /// </remarks>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Number of sub-vectors of PQ. Default is the vector dimension divided by 16.
        /// </summary>
        /// <remarks>
        /// This value controls how much the vector is compressed during the
        /// quantization step. The more sub-vectors there are the less the vector
        /// is compressed. If the dimension is not evenly divisible by 16, the
        /// dimension divided by 8 is used. Having 8 or 16 values per subvector
        /// allows the use of efficient SIMD instructions.
        /// </remarks>
        public int? NumSubVectors { get; set; }

        /// <summary>
        /// Number of bits to encode each sub-vector. Only 4 and 8 are supported.
        /// Default is 8.
        /// </summary>
        /// <remarks>
        /// This value controls how much the sub-vectors are compressed. The more
        /// bits the more accurate the index but the slower the search.
        /// </remarks>
        public int NumBits { get; set; } = 8;

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        /// <remarks>
        /// Increasing this might improve the quality of the index but in most cases
        /// these extra iterations have diminishing returns.
        /// </remarks>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans.
        /// Default is 256.
        /// </summary>
        /// <remarks>
        /// The total number of vectors used to train the index is
        /// <c>SampleRate * NumPartitions</c>. Increasing this value might improve
        /// the quality of the index but in most cases the default should be sufficient.
        /// </remarks>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The target size of each partition. Default is 8192.
        /// </summary>
        /// <remarks>
        /// This value controls the tradeoff between search performance and accuracy.
        /// Higher values yield faster search but less accurate results.
        /// </remarks>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "IvfPq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["num_bits"] = NumBits,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (NumSubVectors.HasValue) { dict["num_sub_vectors"] = NumSubVectors.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-HNSW-PQ (Hierarchical Navigable Small World with Product Quantization) vector index.
    /// </summary>
    /// <remarks>
    /// This index type combines an IVF partition structure with HNSW graphs and product
    /// quantization for efficient approximate nearest neighbor search. Multiple HNSW
    /// graphs are created across IVF partitions.
    /// </remarks>
    public class HnswPqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, <c>"hamming"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Number of sub-vectors for PQ. Default is dimension divided by 16.
        /// </summary>
        public int? NumSubVectors { get; set; }

        /// <summary>
        /// Number of bits to encode each sub-vector. Only 4 and 8 are supported. Default is 8.
        /// </summary>
        public int NumBits { get; set; } = 8;

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The number of neighbors per node in the HNSW graph (also known as <c>m</c>).
        /// Default is 20.
        /// </summary>
        public int NumEdges { get; set; } = 20;

        /// <summary>
        /// The number of candidates evaluated during HNSW construction (<c>ef_construction</c>).
        /// Default is 300.
        /// </summary>
        public int EfConstruction { get; set; } = 300;

        /// <summary>
        /// The target size of each partition. Default is 1,048,576.
        /// </summary>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "HnswPq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["num_bits"] = NumBits,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
                ["num_edges"] = NumEdges,
                ["ef_construction"] = EfConstruction,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (NumSubVectors.HasValue) { dict["num_sub_vectors"] = NumSubVectors.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-Flat vector index with no quantization.
    /// </summary>
    /// <remarks>
    /// This index stores raw vectors grouped into IVF partitions of similar vectors.
    /// Each partition keeps track of a centroid which is the average value of all
    /// vectors in the group. During a query, the centroids are compared with the
    /// query vector to find the closest partitions, then the raw vectors within
    /// those partitions are searched.
    ///
    /// Because vectors are not compressed, this index provides exact distances
    /// within each partition at the cost of higher storage and memory usage
    /// compared to quantized indexes.
    /// </remarks>
    public class IvfFlatIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, <c>"hamming"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The target size of each partition. Default is 8192.
        /// </summary>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "IvfFlat";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-SQ (Scalar Quantization) vector index.
    /// </summary>
    /// <remarks>
    /// This index applies scalar quantization to compress vectors and organizes the
    /// quantized vectors into IVF partitions. It offers a balance between search
    /// speed and storage efficiency while keeping good recall.
    /// </remarks>
    public class IvfSqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, <c>"hamming"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The target size of each partition.
        /// </summary>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "IvfSq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-RQ (RabitQ Quantization) vector index.
    /// </summary>
    /// <remarks>
    /// IVF-RQ compresses vectors using RabitQ quantization and organizes them into
    /// IVF partitions. Each dimension is quantized into a small number of bits.
    /// The <c>NumBits</c> and <c>NumPartitions</c> parameters control the tradeoff
    /// between index size (and thus search speed) and index accuracy.
    /// </remarks>
    public class IvfRqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, <c>"hamming"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Number of bits to encode each dimension in the RabitQ codebook. Default is 1.
        /// </summary>
        public int NumBits { get; set; } = 1;

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The target size of each partition. Default is 8192.
        /// </summary>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "IvfRq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["num_bits"] = NumBits,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }

    /// <summary>
    /// An IVF-HNSW-SQ (Hierarchical Navigable Small World with Scalar Quantization) vector index.
    /// </summary>
    /// <remarks>
    /// This index type combines an IVF partition structure with HNSW graphs and scalar
    /// quantization for efficient approximate nearest neighbor search.
    /// </remarks>
    public class HnswSqIndex : Index
    {
        /// <summary>
        /// The distance metric. One of <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, <c>"hamming"</c>.
        /// Default is <c>"l2"</c>.
        /// </summary>
        public string DistanceType { get; set; } = "l2";

        /// <summary>
        /// The number of IVF partitions. Default is the square root of the number of rows.
        /// </summary>
        public int? NumPartitions { get; set; }

        /// <summary>
        /// Max iterations to train kmeans. Default is 50.
        /// </summary>
        public int MaxIterations { get; set; } = 50;

        /// <summary>
        /// The rate used to calculate the number of training vectors for kmeans. Default is 256.
        /// </summary>
        public int SampleRate { get; set; } = 256;

        /// <summary>
        /// The number of neighbors per node in the HNSW graph (also known as <c>m</c>).
        /// Default is 20.
        /// </summary>
        public int NumEdges { get; set; } = 20;

        /// <summary>
        /// The number of candidates evaluated during HNSW construction (<c>ef_construction</c>).
        /// Default is 300.
        /// </summary>
        public int EfConstruction { get; set; } = 300;

        /// <summary>
        /// The target size of each partition. Default is 1,048,576.
        /// </summary>
        public int? TargetPartitionSize { get; set; }

        internal override string IndexType => "HnswSq";

        internal override string ToConfigJson()
        {
            var dict = new System.Collections.Generic.Dictionary<string, object>
            {
                ["distance_type"] = DistanceType,
                ["max_iterations"] = MaxIterations,
                ["sample_rate"] = SampleRate,
                ["num_edges"] = NumEdges,
                ["ef_construction"] = EfConstruction,
            };
            if (NumPartitions.HasValue) { dict["num_partitions"] = NumPartitions.Value; }
            if (TargetPartitionSize.HasValue) { dict["target_partition_size"] = TargetPartitionSize.Value; }
            return JsonSerializer.Serialize(dict);
        }
    }
}
