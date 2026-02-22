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
        /// Configuration for the LanceDB Cloud HTTP client, including retry
        /// policy, timeouts, and TLS settings.
        ///
        /// Requires the remote feature (not yet supported in this SDK).
        /// </summary>
        public ClientConfig? ClientConfig { get; set; }

        /// <summary>
        /// Additional options for the storage backend.
        ///
        /// Options such as S3 credentials, request timeouts, or cloud storage
        /// configuration. See available options at
        /// https://lancedb.github.io/lancedb/guides/storage/
        /// </summary>
        public Dictionary<string, string>? StorageOptions { get; set; }

        /// <summary>
        /// (For LanceDB OSS only) A session to use for this connection.
        ///
        /// Sessions allow you to configure cache sizes for index and metadata
        /// caches, which can significantly impact memory use and performance.
        /// They can also be re-used across multiple connections to share the
        /// same cache state.
        /// </summary>
        public Session? Session { get; set; }
    }

    /// <summary>
    /// Configuration for the LanceDB Cloud HTTP client.
    ///
    /// Not yet supported in this SDK.
    /// </summary>
    public class ClientConfig
    {
        /// <summary>
        /// User agent string for HTTP requests.
        /// </summary>
        public string? UserAgent { get; set; }

        /// <summary>
        /// Retry configuration for HTTP requests.
        /// </summary>
        public RetryConfig? RetryConfig { get; set; }

        /// <summary>
        /// Timeout configuration for HTTP requests.
        /// </summary>
        public TimeoutConfig? TimeoutConfig { get; set; }

        /// <summary>
        /// Extra HTTP headers to include in every request.
        /// </summary>
        public Dictionary<string, string>? ExtraHeaders { get; set; }
    }

    /// <summary>
    /// Retry configuration for HTTP requests.
    /// </summary>
    public class RetryConfig
    {
        /// <summary>Maximum number of retries.</summary>
        public int? Retries { get; set; }

        /// <summary>Maximum number of connection retries.</summary>
        public int? ConnectRetries { get; set; }

        /// <summary>Maximum number of read retries.</summary>
        public int? ReadRetries { get; set; }

        /// <summary>Backoff factor between retries.</summary>
        public float? BackoffFactor { get; set; }

        /// <summary>Backoff jitter between retries.</summary>
        public float? BackoffJitter { get; set; }

        /// <summary>HTTP status codes to retry on.</summary>
        public IReadOnlyList<int>? Statuses { get; set; }
    }

    /// <summary>
    /// Timeout configuration for HTTP requests.
    /// </summary>
    public class TimeoutConfig
    {
        /// <summary>Overall request timeout.</summary>
        public TimeSpan? Timeout { get; set; }

        /// <summary>Connection timeout.</summary>
        public TimeSpan? ConnectTimeout { get; set; }

        /// <summary>Read timeout.</summary>
        public TimeSpan? ReadTimeout { get; set; }

        /// <summary>Pool idle timeout.</summary>
        public TimeSpan? PoolIdleTimeout { get; set; }
    }

    /// <summary>
    /// Session for managing index and metadata caches.
    ///
    /// Sessions can be re-used across multiple connections to share cache state.
    /// </summary>
    public class Session
    {
        /// <summary>
        /// Maximum size of the index cache in bytes.
        /// If <c>null</c>, the default cache size 6 GiB is used.
        /// </summary>
        public long? IndexCacheSizeBytes { get; set; }

        /// <summary>
        /// Maximum size of the metadata cache in bytes.
        /// If <c>null</c>, the default cache size 1 GiB is used.
        /// </summary>
        public long? MetadataCacheSizeBytes { get; set; }
    }
}
