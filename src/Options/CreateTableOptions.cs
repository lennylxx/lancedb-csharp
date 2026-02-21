namespace lancedb
{
    using System.Collections.Generic;
    using Apache.Arrow;

    /// <summary>
    /// Options to control the behavior when creating a table.
    /// </summary>
    public class CreateTableOptions
    {
        /// <summary>
        /// The initial data to populate the table with, as one or more Arrow
        /// <see cref="RecordBatch"/> objects. The table schema is inferred from the data.
        /// </summary>
        public IReadOnlyList<RecordBatch>? Data { get; set; }

        /// <summary>
        /// The mode to use when creating the table. Default is <c>"create"</c>.
        /// - <c>"create"</c> - Create the table. An error is raised if the table already exists.
        /// - <c>"overwrite"</c> - If a table with the same name already exists, it is replaced.
        /// </summary>
        public string Mode { get; set; } = "create";

        /// <summary>
        /// Additional options for the storage backend.
        ///
        /// Options already set on the connection will be inherited by the table,
        /// but can be overridden here.
        ///
        /// See available options at https://lancedb.github.io/lancedb/guides/storage/
        /// </summary>
        public Dictionary<string, string>? StorageOptions { get; set; }
    }
}
