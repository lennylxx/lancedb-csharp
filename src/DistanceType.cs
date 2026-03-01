namespace lancedb
{
    /// <summary>
    /// The distance metric used for vector search and index building.
    /// </summary>
    /// <remarks>
    /// The distance type used to build an index MUST match the distance type
    /// used to search the index. Failure to do so will yield inaccurate results.
    /// </remarks>
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
}
