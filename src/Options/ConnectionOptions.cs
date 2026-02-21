namespace lancedb
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Options to control the connection behavior when connecting to a LanceDB database.
    /// </summary>
    public class ConnectionOptions
    {
        /// <summary>
        /// API key for LanceDB Cloud connections.
        ///
        /// Can also be set via environment variable <c>LANCEDB_API_KEY</c>.
        /// This option is only used when connecting to LanceDB Cloud (db:// URIs)
        /// and requires the remote feature (not yet supported in this SDK).
        /// </summary>
        public string? ApiKey { get; set; }

        /// <summary>
        /// Region to use for LanceDB Cloud. Default is <c>"us-east-1"</c>.
        ///
        /// This option is only used when connecting to LanceDB Cloud (db:// URIs)
        /// and requires the remote feature (not yet supported in this SDK).
        /// </summary>
        public string Region { get; set; } = "us-east-1";

        /// <summary>
        /// Override the host URL for LanceDB Cloud.
        ///
        /// Useful for local testing. Requires the remote feature (not yet supported
        /// in this SDK).
        /// </summary>
        public string? HostOverride { get; set; }

        /// <summary>
        /// (For LanceDB OSS only) The interval at which to check for updates to
        /// the table from other processes.
        ///
        /// If <c>null</c>, consistency is not checked (the default). For strong
        /// consistency, set this to <see cref="TimeSpan.Zero"/>. For eventual
        /// consistency, set to a non-zero interval.
        ///
        /// Only applies to read operations. Write operations are always consistent.
        /// </summary>
        public TimeSpan? ReadConsistencyInterval { get; set; }

        /// <summary>
        /// Additional options for the storage backend.
        ///
        /// Options such as S3 credentials, request timeouts, or cloud storage
        /// configuration. See available options at
        /// https://lancedb.github.io/lancedb/guides/storage/
        /// </summary>
        public Dictionary<string, string>? StorageOptions { get; set; }
    }
}
