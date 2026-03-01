namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Text.Json;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.C;

    /// <summary>
    /// A connection to a LanceDB database.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implements <see cref="IDisposable"/> to release the underlying native connection handle.
    /// </para>
    /// <para>
    /// Closing a connection is optional. If not closed, it will be closed when the object
    /// is garbage collected.
    /// </para>
    /// </remarks>
    public class Connection : IDisposable
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_connect(IntPtr uri, double read_consistency_interval_secs, IntPtr storage_options_json, long index_cache_size_bytes, long metadata_cache_size_bytes, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_open_table(IntPtr connection_ptr, IntPtr table_name, IntPtr storage_options_json, uint index_cache_size, IntPtr location, IntPtr namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe void connection_create_empty_table(IntPtr connection_ptr, IntPtr table_name, CArrowSchema* schema_cdata, IntPtr mode, IntPtr storage_options_json, IntPtr location, IntPtr namespace_json, [MarshalAs(UnmanagedType.U1)] bool exist_ok, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe void connection_create_table(IntPtr connection_ptr, IntPtr table_name, CArrowArray* arrays, CArrowSchema* schema, nuint batch_count, IntPtr mode, IntPtr storage_options_json, IntPtr location, IntPtr namespace_json, [MarshalAs(UnmanagedType.U1)] bool exist_ok, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_table_names(IntPtr connection_ptr, IntPtr start_after, uint limit, IntPtr namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_list_tables(IntPtr connection_ptr, IntPtr page_token, uint limit, IntPtr namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_drop_table(IntPtr connection_ptr, IntPtr table_name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_drop_all_tables(IntPtr connection_ptr, NativeCall.FfiCallback completion);

        private ConnectionHandle? _handle;

        public Connection()
        {
        }

        /// <summary>
        /// Connect to a LanceDB database.
        /// </summary>
        /// <param name="uri">
        /// The URI of the database. Accepted formats:
        /// <list type="bullet">
        /// <item><description><c>/path/to/database</c> — local database on file system.</description></item>
        /// <item><description><c>s3://bucket/path</c> or <c>gs://bucket/path</c> — database on cloud storage.</description></item>
        /// <item><description><c>db://host:port</c> — remote database (LanceDB Cloud).</description></item>
        /// </list>
        /// </param>
        /// <param name="options">Options to control the connection behavior.</param>
        /// <returns>A task that completes when the connection is established.</returns>
        public async Task Connect(string uri, ConnectionOptions? options = null)
        {
            byte[] uriBytes = NativeCall.ToUtf8(uri);
            double rciSecs = options?.ReadConsistencyInterval.HasValue == true
                ? options.ReadConsistencyInterval!.Value.TotalSeconds
                : double.NaN;
            byte[]? storageJson = options?.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;
            long indexCacheSizeBytes = options?.Session != null
                ? (options.Session.IndexCacheSizeBytes ?? 0)
                : -1;
            long metadataCacheSizeBytes = options?.Session != null
                ? (options.Session.MetadataCacheSizeBytes ?? 0)
                : -1;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = uriBytes)
                    fixed (byte* pStorage = storageJson)
                    {
                        connection_connect(
                            new IntPtr(p),
                            rciSecs,
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                            indexCacheSizeBytes,
                            metadataCacheSizeBytes,
                            callback);
                    }
                }
            });
            _handle = new ConnectionHandle(ptr);
        }

        /// <summary>
        /// Close the connection, releasing any underlying resources.
        /// </summary>
        /// <remarks>
        /// It is safe to call this method multiple times.
        /// Any attempt to use the connection after it is closed will result in an error.
        /// </remarks>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Check whether the connection is open.
        /// </summary>
        /// <returns>
        /// <c>true</c> if the connection is open and usable; <c>false</c> if
        /// it has been closed, disposed, or was never connected.
        /// </returns>
        public bool IsOpen()
        {
            return _handle != null && !_handle.IsInvalid && !_handle.IsClosed;
        }

        public void Dispose()
        {
            _handle?.Dispose();
            _handle = null;
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Open a Lance Table in the database.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <param name="options">Options to control the open behavior, including storage
        /// options and location.</param>
        /// <returns>A <see cref="Table"/> representing the opened table.</returns>
        /// <exception cref="LanceDbException">Thrown if the table does not exist.</exception>
        public async Task<Table> OpenTable(string name, OpenTableOptions? options = null)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            byte[]? storageJson = options?.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;
            byte[]? locationBytes = options?.Location != null
                ? NativeCall.ToUtf8(options.Location)
                : null;
            byte[]? namespaceJson = options?.Namespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.Namespace))
                : null;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    fixed (byte* pStorage = storageJson)
                    fixed (byte* pLocation = locationBytes)
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        connection_open_table(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(p),
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                            0,
                            locationBytes != null ? new IntPtr(pLocation) : IntPtr.Zero,
                            namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                            callback);
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Create an empty table with the given name and a minimal schema.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <param name="options">Options to control the create behavior.</param>
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">Thrown if a table with the same name already exists.</exception>
        /// <remarks>
        /// The vector index is not created by default.
        /// To create the index, call the <c>CreateIndex</c> method on the table.
        /// </remarks>
        public async Task<Table> CreateEmptyTable(string name, CreateTableOptions? options = null)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            byte[]? modeBytes = options != null
                ? NativeCall.ToUtf8(options.Mode)
                : null;
            byte[]? storageJson = options?.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;
            byte[]? locationBytes = options?.Location != null
                ? NativeCall.ToUtf8(options.Location)
                : null;
            byte[]? namespaceJson = options?.Namespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.Namespace))
                : null;
            bool existOk = options?.ExistOk ?? false;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pName = nameBytes)
                    fixed (byte* pMode = modeBytes)
                    fixed (byte* pStorage = storageJson)
                    fixed (byte* pLocation = locationBytes)
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        var cSchemaArr = new CArrowSchema[1];
                        fixed (CArrowSchema* pSchema = cSchemaArr)
                        {
                            CArrowSchema* schemaPtr = null;
                            if (options?.Schema != null)
                            {
                                CArrowSchemaExporter.ExportSchema(options.Schema, pSchema);
                                schemaPtr = pSchema;
                            }
                            connection_create_empty_table(
                                _handle!.DangerousGetHandle(),
                                new IntPtr(pName),
                                schemaPtr,
                                modeBytes != null ? new IntPtr(pMode) : IntPtr.Zero,
                                storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                                locationBytes != null ? new IntPtr(pLocation) : IntPtr.Zero,
                                namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                                existOk,
                                callback);
                        }
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Create an <see cref="Table"/> in the database.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <param name="options">
        /// Options to control the create behavior, including the initial data,
        /// schema, create mode, storage options, and namespace.
        /// User must provide at least one of <see cref="CreateTableOptions.Data"/> or
        /// <see cref="CreateTableOptions.Schema"/>.
        /// </param>
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">
        /// Thrown if a table with the same name already exists and mode is <c>"create"</c>.
        /// </exception>
        /// <remarks>
        /// The vector index is not created by default.
        /// To create the index, call the <c>CreateIndex</c> method on the table.
        /// </remarks>
        public async Task<Table> CreateTable(string name, CreateTableOptions options)
        {
            if (options.Data == null || options.Data.Count == 0)
            {
                if (options.Schema != null)
                {
                    return await CreateEmptyTable(name, options);
                }
                throw new ArgumentException("Either Data or Schema must be provided.", nameof(options));
            }

            byte[] nameBytes = NativeCall.ToUtf8(name);
            byte[] modeBytes = NativeCall.ToUtf8(options.Mode);
            byte[]? storageJson = options.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;
            byte[]? locationBytes = options.Location != null
                ? NativeCall.ToUtf8(options.Location)
                : null;
            byte[]? namespaceJson = options.Namespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.Namespace))
                : null;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pName = nameBytes)
                    fixed (byte* pMode = modeBytes)
                    fixed (byte* pStorage = storageJson)
                    fixed (byte* pLocation = locationBytes)
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        var cArrays = new CArrowArray[options.Data.Count];
                        var cSchemaArr = new CArrowSchema[1];
                        fixed (CArrowSchema* pSchema = cSchemaArr)
                        {
                            CArrowSchemaExporter.ExportSchema(options.Data[0].Schema, pSchema);
                            for (int i = 0; i < options.Data.Count; i++)
                            {
                                cArrays[i] = default;
                                var clone = ArrowExportHelper.CloneBatchForExport(options.Data[i]);
                                fixed (CArrowArray* pArr = &cArrays[i])
                                {
                                    CArrowArrayExporter.ExportRecordBatch(clone, pArr);
                                }
                            }
                            fixed (CArrowArray* pArrays = cArrays)
                            {
                                connection_create_table(
                                    _handle!.DangerousGetHandle(),
                                    (IntPtr)pName,
                                    pArrays, pSchema, (nuint)options.Data.Count,
                                    (IntPtr)pMode,
                                    storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                                    locationBytes != null ? new IntPtr(pLocation) : IntPtr.Zero,
                                    namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                                    options.ExistOk,
                                    callback);
                            }
                        }
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Create a table in the database with a single <see cref="RecordBatch"/>.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <param name="data">
        /// The initial data to populate the table with. The table schema is inferred
        /// from the data.
        /// </param>
        /// <param name="mode">
        /// The mode to use when creating the table. Default is <c>"create"</c>.
        /// <list type="bullet">
        /// <item><description><c>"create"</c> — Create the table. An error is raised if the table already exists.</description></item>
        /// <item><description><c>"overwrite"</c> — If a table with the same name already exists, it is replaced.</description></item>
        /// </list>
        /// </param>
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">
        /// Thrown if a table with the same name already exists and mode is <c>"create"</c>.
        /// </exception>
        /// <remarks>
        /// The vector index is not created by default.
        /// To create the index, call the <c>CreateIndex</c> method on the table.
        /// </remarks>
        public Task<Table> CreateTable(string name, RecordBatch data, string mode = "create")
        {
            return CreateTable(name, new CreateTableOptions { Data = new[] { data }, Mode = mode });
        }

        /// <summary>
        /// Get the names of all tables in the database, in sorted order.
        /// </summary>
        /// <param name="startAfter">
        /// If present, only return names that come lexicographically after the supplied
        /// value. This can be combined with <paramref name="limit"/> to implement pagination
        /// by setting this to the last table name from the previous page.
        /// </param>
        /// <param name="limit">
        /// The maximum number of table names to return. If <c>null</c> or 0, all names
        /// are returned.
        /// </param>
        /// <param name="ns">
        /// The namespace to list tables from, specified as a hierarchical path.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <returns>A list of table names in lexicographical order.</returns>
        public async Task<IReadOnlyList<string>> TableNames(string? startAfter = null, uint limit = 0, IReadOnlyList<string>? ns = null)
        {
            byte[]? startAfterBytes = startAfter != null ? NativeCall.ToUtf8(startAfter) : null;
            byte[]? namespaceJson = ns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(ns))
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pStartAfter = startAfterBytes)
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        connection_table_names(
                            _handle!.DangerousGetHandle(),
                            startAfterBytes != null ? new IntPtr(pStartAfter) : IntPtr.Zero,
                            limit,
                            namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                            callback);
                    }
                }
            });
            string joined = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(joined))
            {
                return System.Array.Empty<string>();
            }
            return joined.Split('\n');
        }

        /// <summary>
        /// List tables in the database with pagination support.
        /// </summary>
        /// <remarks>
        /// Returns a <see cref="ListTablesResponse"/> containing table names and an opaque
        /// page token for retrieving subsequent pages. When the response's
        /// <see cref="ListTablesResponse.PageToken"/> is not <c>null</c>, pass it as
        /// <paramref name="pageToken"/> to retrieve the next page.
        /// </remarks>
        /// <param name="pageToken">
        /// An opaque token from a previous <see cref="ListTablesResponse.PageToken"/>
        /// to continue pagination. <c>null</c> starts from the beginning.
        /// </param>
        /// <param name="limit">
        /// The maximum number of table names to return. If 0, all names are returned.
        /// </param>
        /// <param name="ns">
        /// The namespace to list tables from, specified as a hierarchical path.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <returns>
        /// A <see cref="ListTablesResponse"/> containing the table names and an optional
        /// page token for the next page.
        /// </returns>
        public async Task<ListTablesResponse> ListTables(string? pageToken = null, uint limit = 0, IReadOnlyList<string>? ns = null)
        {
            byte[]? pageTokenBytes = pageToken != null ? NativeCall.ToUtf8(pageToken) : null;
            byte[]? namespaceJson = ns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(ns))
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pPageToken = pageTokenBytes)
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        connection_list_tables(
                            _handle!.DangerousGetHandle(),
                            pageTokenBytes != null ? new IntPtr(pPageToken) : IntPtr.Zero,
                            limit,
                            namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                            callback);
                    }
                }
            });
            string json = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(json))
            {
                return new ListTablesResponse();
            }
            return JsonSerializer.Deserialize<ListTablesResponse>(json) ?? new ListTablesResponse();
        }

        /// <summary>
        /// Drop a table from the database.
        /// </summary>
        /// <param name="name">The name of the table to drop.</param>
        /// <param name="ignoreMissing">
        /// If <c>true</c>, ignore if the table does not exist.
        /// If <c>false</c> (default), a <see cref="LanceDbException"/> is thrown.
        /// </param>
        /// <exception cref="LanceDbException">
        /// Thrown if the table does not exist and <paramref name="ignoreMissing"/> is <c>false</c>.
        /// </exception>
        public async Task DropTable(string name, bool ignoreMissing = false)
        {
            try
            {
                byte[] nameBytes = NativeCall.ToUtf8(name);
                await NativeCall.Async(callback =>
                {
                    unsafe
                    {
                        fixed (byte* p = nameBytes)
                        {
                            connection_drop_table(_handle!.DangerousGetHandle(), new IntPtr(p), callback);
                        }
                    }
                });
            }
            catch (LanceDbException) when (ignoreMissing)
            {
            }
        }

        /// <summary>
        /// Rename a table in the database.
        /// </summary>
        /// <param name="currentName">The current name of the table.</param>
        /// <param name="newName">The new name for the table.</param>
        /// <remarks>
        /// This operation is only supported in LanceDB Cloud. Calling this method
        /// on a local (OSS) database will throw <see cref="NotImplementedException"/>.
        /// </remarks>
        /// <exception cref="NotImplementedException">Always thrown. LanceDB OSS does not support this operation.</exception>
        public Task RenameTable(string currentName, string newName)
        {
            return Task.FromException(new NotImplementedException("RenameTable is only supported in LanceDB Cloud."));
        }

        /// <summary>
        /// Drop all tables from the database.
        /// </summary>
        public async Task DropAllTables()
        {
            await NativeCall.Async(callback =>
            {
                connection_drop_all_tables(_handle!.DangerousGetHandle(), callback);
            });
        }
    }
}