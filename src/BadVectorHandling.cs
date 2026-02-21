namespace lancedb
{
    /// <summary>
    /// Describes what to do when vector data contains NaN values.
    /// </summary>
    /// <remarks>
    /// Vector columns are identified as <c>FixedSizeList</c> fields whose value type
    /// is <c>Float</c> or <c>Double</c>.
    /// </remarks>
    public enum BadVectorHandling
    {
        /// <summary>
        /// An error is raised if any vector contains NaN values. This is the default.
        /// </summary>
        Error,

        /// <summary>
        /// Rows with bad vectors are silently removed from the data.
        /// </summary>
        Drop,

        /// <summary>
        /// Bad vectors are replaced with a vector where every element is
        /// <see cref="AddOptions.FillValue"/>.
        /// </summary>
        Fill,

        /// <summary>
        /// Bad vectors are replaced with null.
        /// </summary>
        Null,
    }
}
