namespace lancedb
{
    public class ConnectionOptions
    {
        /// <summary>
        /// LanceDB database URI.
        ///
        /// - `/path/to/database` - local database
        /// - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
        /// - `db://host:port` - remote database (LanceDB cloud)
        /// </summary>
        public string? Uri { get; set; }

        /// <summary>
        /// User provided options for object storage. For example, S3 credentials or request timeouts.
        ///
        /// The various options are described at https://lancedb.github.io/lancedb/guides/storage/
        /// </summary>
        public Dictionary<string, string>? StorageOptions { get; set; }

        /// <summary>
        /// API key for the remote connections
        ///
        /// Can also be passed by setting environment variable `LANCEDB_API_KEY`
        /// </summary>
        public string? ApiKey { get; set; }

        /// <summary>
        /// Region to connect. Default is 'us-east-1'
        /// </summary>
        public string? Region { get; set; }

        /// <summary>
        /// Override the host URL for the remote connection.
        ///
        /// This is useful for local testing.
        /// </summary>
        public string? HostOverride { get; set; }

        /// <summary>
        /// Duration in milliseconds for request timeout. Default = 10,000 (10 seconds)
        /// </summary>
        public int? Timeout { get; set; }

        /// <summary>
        /// (For LanceDB OSS only): The interval, in seconds, at which to check for
        /// updates to the table from other processes. If None, then consistency is not
        /// checked. For performance reasons, this is the default. For strong
        /// consistency, set this to zero seconds. Then every read will check for
        /// updates from other processes. As a compromise, you can set this to a
        /// non-zero value for eventual consistency. If more than that interval
        /// has passed since the last check, then the table will be checked for updates.
        /// Note: this consistency only applies to read operations. Write operations are
        /// always consistent.
        /// </summary>
        public int? ReadConsistencyInterval { get; set; }
    }
}