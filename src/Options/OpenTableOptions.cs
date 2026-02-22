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
