namespace lancedb
{
    public class OpenTableOptions
    {
        /// <summary>
        /// Configuration for object storage.
        ///
        /// Options already set on the connection will be inherited by the table,
        /// but can be overridden here.
        ///
        /// The available options are described at https://lancedb.github.io/lancedb/guides/storage/
        /// </summary>
        public Dictionary<string, string>? StorageOptions { get; set; }

        /// <summary>
        /// Set the size of the index cache, specified as a number of entries.
        ///
        /// The exact meaning of an "entry" will depend on the type of index:
        /// - IVF: there is one entry for each IVF partition
        /// - BTREE: there is one entry for the entire index
        ///
        /// This cache applies to the entire opened table, across all indices.
        /// Setting this value higher will increase performance on larger datasets
        /// at the expense of more RAM.
        /// </summary>
        public int? IndexCacheSize { get; set; }
    }
}