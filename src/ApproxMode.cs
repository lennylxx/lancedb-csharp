namespace lancedb
{
    /// <summary>
    /// The speed / accuracy tradeoff to use for approximate vector search.
    /// </summary>
    /// <remarks>
    /// This currently only affects RQ-quantized vector indexes, such as
    /// <c>IVF_RQ</c>. Other index types ignore this setting.
    /// </remarks>
    public enum ApproxMode
    {
        /// <summary>
        /// Prefer lower query latency, which can reduce recall.
        /// </summary>
        Fast = 0,

        /// <summary>
        /// Use the default balance between query latency and recall.
        /// </summary>
        Normal = 1,

        /// <summary>
        /// Prefer higher recall, which can increase query latency.
        /// </summary>
        Accurate = 2,
    }
}
