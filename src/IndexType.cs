namespace lancedb
{
    /// <summary>
    /// The type of index built on a table column.
    /// </summary>
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
}
