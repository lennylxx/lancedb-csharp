namespace lancedb
{
    /// <summary>
    /// Options for <see cref="Table.Add(System.Collections.Generic.IReadOnlyList{Apache.Arrow.RecordBatch}, AddOptions?)"/>.
    /// </summary>
    public class AddOptions
    {
        /// <summary>
        /// The mode to use when writing the data.
        /// <list type="bullet">
        /// <item><description><c>"append"</c> (default) — Append the new data to the table.</description></item>
        /// <item><description><c>"overwrite"</c> — Replace the existing data with the new data.</description></item>
        /// </list>
        /// </summary>
        public string Mode { get; set; } = "append";

        /// <summary>
        /// What to do if any of the vectors are not the same size or contain NaN values.
        /// Only applies to columns whose Arrow type is <c>FixedSizeList(Float/Double)</c>.
        /// Default is <see cref="BadVectorHandling.Error"/>.
        /// </summary>
        public BadVectorHandling OnBadVectors { get; set; } = BadVectorHandling.Error;

        /// <summary>
        /// The value to fill bad vectors with when <see cref="OnBadVectors"/>
        /// is <see cref="BadVectorHandling.Fill"/>.
        /// Every element in the replacement vector will be set to this value.
        /// Default is <c>0.0f</c>.
        /// </summary>
        public float FillValue { get; set; } = 0.0f;
    }
}
