namespace lancedb
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Specification selecting Lance's MemWAL LSM-style write path for
    /// <c>merge_insert</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Construct via <see cref="Bucket"/>, <see cref="Identity"/>, or
    /// <see cref="Unsharded"/>, then optionally chain
    /// <see cref="WithMaintainedIndexes"/> (indexes the MemWAL keeps up to date
    /// as rows are appended) and <see cref="WithWriterConfigDefaults"/> (default
    /// <c>ShardWriter</c> configuration recorded in the MemWAL index).
    /// </para>
    /// <para>
    /// Install a spec with <see cref="Table.SetLsmWriteSpec"/> and remove it with
    /// <see cref="Table.UnsetLsmWriteSpec"/>. All variants require the table to
    /// have an unenforced primary key set via
    /// <see cref="Table.SetUnenforcedPrimaryKey(string)"/>; bucket sharding
    /// additionally requires it to be the single column being bucketed.
    /// </para>
    /// </remarks>
    public sealed class LsmWriteSpec
    {
        private LsmWriteSpec(int kind, string column, uint numBuckets)
        {
            Kind = kind;
            Column = column;
            NumBuckets = numBuckets;
            MaintainedIndexes = new List<string>();
            WriterConfigDefaults = new Dictionary<string, string>();
        }

        internal int Kind { get; }

        internal string Column { get; }

        internal uint NumBuckets { get; }

        internal IReadOnlyList<string> MaintainedIndexes { get; private set; }

        internal IReadOnlyDictionary<string, string> WriterConfigDefaults { get; private set; }

        /// <summary>
        /// Construct a hash-bucket sharding spec.
        /// </summary>
        /// <remarks>
        /// Hash-bucket writes by the single-column unenforced primary key.
        /// <paramref name="column"/> must be a non-nested column with a supported
        /// scalar type, and <paramref name="numBuckets"/> must be in <c>[1, 1024]</c>.
        /// </remarks>
        /// <param name="column">The scalar column to hash-bucket by.</param>
        /// <param name="numBuckets">The number of hash buckets.</param>
        /// <returns>A bucket-sharding spec.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="column"/> is null.</exception>
        public static LsmWriteSpec Bucket(string column, uint numBuckets)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            return new LsmWriteSpec(0, column, numBuckets);
        }

        /// <summary>
        /// Construct an identity-sharding spec (shard by the raw value of a column).
        /// </summary>
        /// <remarks>
        /// Use this when the data is already partitioned by <paramref name="column"/>;
        /// each distinct value of <paramref name="column"/> becomes its own shard.
        /// </remarks>
        /// <param name="column">The scalar column to shard by.</param>
        /// <returns>An identity-sharding spec.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="column"/> is null.</exception>
        public static LsmWriteSpec Identity(string column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            return new LsmWriteSpec(1, column, 0);
        }

        /// <summary>
        /// Construct an unsharded spec — every <c>merge_insert</c> call writes to a
        /// single MemWAL shard.
        /// </summary>
        /// <returns>An unsharded spec.</returns>
        public static LsmWriteSpec Unsharded()
        {
            return new LsmWriteSpec(2, string.Empty, 0);
        }

        /// <summary>
        /// Replace the list of indexes the MemWAL should keep up to date as rows
        /// are appended.
        /// </summary>
        /// <remarks>
        /// Each name must reference an index that already exists on the table at
        /// the time <see cref="Table.SetLsmWriteSpec"/> is called.
        /// </remarks>
        /// <param name="indexes">The index names to maintain.</param>
        /// <returns>This spec, for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="indexes"/> is null.</exception>
        public LsmWriteSpec WithMaintainedIndexes(IEnumerable<string> indexes)
        {
            if (indexes == null)
            {
                throw new ArgumentNullException(nameof(indexes));
            }

            MaintainedIndexes = new List<string>(indexes);
            return this;
        }

        /// <summary>
        /// Replace the default <c>ShardWriter</c> configuration recorded in the
        /// MemWAL index, so every writer starts from the same defaults.
        /// </summary>
        /// <remarks>
        /// Keys are <c>ShardWriter</c> config field names (<c>Duration</c> knobs use
        /// a <c>_ms</c> suffix); values are their string encodings.
        /// </remarks>
        /// <param name="defaults">The default configuration to record.</param>
        /// <returns>This spec, for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="defaults"/> is null.</exception>
        public LsmWriteSpec WithWriterConfigDefaults(IReadOnlyDictionary<string, string> defaults)
        {
            if (defaults == null)
            {
                throw new ArgumentNullException(nameof(defaults));
            }

            WriterConfigDefaults = new Dictionary<string, string>(GetDictionary(defaults));
            return this;
        }

        private static Dictionary<string, string> GetDictionary(IReadOnlyDictionary<string, string> source)
        {
            var copy = new Dictionary<string, string>();
            foreach (var kvp in source)
            {
                copy[kvp.Key] = kvp.Value;
            }

            return copy;
        }
    }
}
