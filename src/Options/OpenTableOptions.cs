namespace lancedb
{
    using System.Collections.Generic;

    /// <summary>
    /// Options to control the behavior when opening a table.
    /// </summary>
    public class OpenTableOptions
    {
        /// <summary>
        /// Additional options for the storage backend.
        ///
        /// Options already set on the connection will be inherited by the table,
        /// but can be overridden here.
        ///
        /// See available options at https://lancedb.github.io/lancedb/guides/storage/
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
        public uint? IndexCacheSize { get; set; }

        /// <summary>
        /// The explicit location (URI) of the table. If provided, the table will
        /// be opened from this location instead of deriving it from the database
        /// URI and table name. Useful when integrating with namespace systems
        /// that manage table locations independently.
        /// </summary>
        public string? Location { get; set; }

        /// <summary>
        /// The namespace for the table, specified as a hierarchical path.
        ///
        /// Namespaces organize tables into logical groups (like folders).
        /// For example, <c>["team", "project"]</c> places the table under
        /// the "team/project" namespace.
        /// </summary>
        public IReadOnlyList<string>? Namespace { get; set; }
    }
}
