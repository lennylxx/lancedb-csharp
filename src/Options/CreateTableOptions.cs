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
        ///
        /// Either <see cref="Data"/> or <see cref="Schema"/> must be provided.
        /// </summary>
        public IReadOnlyList<RecordBatch>? Data { get; set; }

        /// <summary>
        /// The schema of the table. Used to create an empty table without data.
        ///
        /// Either <see cref="Data"/> or <see cref="Schema"/> must be provided.
        /// If both are provided, <see cref="Data"/> takes precedence.
        /// </summary>
        public Schema? Schema { get; set; }

        /// <summary>
        /// The mode to use when creating the table. Default is <c>"create"</c>.
        /// - <c>"create"</c> - Create the table. An error is raised if the table already exists.
        /// - <c>"overwrite"</c> - If a table with the same name already exists, it is replaced.
        /// </summary>
        public string Mode { get; set; } = "create";

        /// <summary>
        /// If <c>true</c>, open the existing table instead of raising an error
        /// when a table with the same name already exists. The provided data
        /// will not be added, but the schema will be validated if specified.
        /// Default is <c>false</c>.
        /// </summary>
        public bool ExistOk { get; set; }

        /// <summary>
        /// What to do if any of the vectors are not the same size or contain NaNs.
        /// - <c>"error"</c> (default) - raise an error
        /// - <c>"drop"</c> - drop rows with bad vectors
        /// - <c>"fill"</c> - fill bad vectors with <see cref="FillValue"/>
        /// - <c>"null"</c> - set bad vectors to null
        ///
        /// Not yet implemented in this SDK. This is client-side data validation.
        /// </summary>
        public string? OnBadVectors { get; set; }

        /// <summary>
        /// The value to use when filling vectors. Only used if
        /// <see cref="OnBadVectors"/> is <c>"fill"</c>. Default is <c>0.0</c>.
        ///
        /// Not yet implemented in this SDK. This is client-side data validation.
        /// </summary>
        public float? FillValue { get; set; }

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
        /// be created at this location instead of deriving it from the database
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
