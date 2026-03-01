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
        private static extern void connection_connect_namespace(IntPtr ns_impl, IntPtr properties_json, IntPtr storage_options_json, double read_consistency_interval_secs, NativeCall.FfiCallback completion);

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
        private static extern void connection_drop_table(IntPtr connection_ptr, IntPtr table_name, IntPtr namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_drop_all_tables(IntPtr connection_ptr, IntPtr namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_rename_table(IntPtr connection_ptr, IntPtr old_name, IntPtr new_name, IntPtr cur_namespace_json, IntPtr new_namespace_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_clone_table(IntPtr connection_ptr, IntPtr target_table_name, IntPtr source_uri, IntPtr target_namespace_json, long source_version, IntPtr source_tag, [MarshalAs(UnmanagedType.U1)] bool is_shallow, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_list_namespaces(IntPtr connection_ptr, IntPtr namespace_json, IntPtr page_token, int limit, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_create_namespace(IntPtr connection_ptr, IntPtr namespace_json, IntPtr mode, IntPtr properties_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_drop_namespace(IntPtr connection_ptr, IntPtr namespace_json, IntPtr mode, IntPtr behavior, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void connection_describe_namespace(IntPtr connection_ptr, IntPtr namespace_json, NativeCall.FfiCallback completion);

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
        /// Connect to a LanceDB database through a namespace.
        /// </summary>
        /// <remarks>
        /// Namespace connections support namespace CRUD operations (create, list, drop,
        /// describe namespaces) and allow organizing tables into hierarchical namespaces.
        /// Use this instead of <see cref="Connect"/> when you need namespace functionality.
        /// </remarks>
        /// <param name="nsImpl">
        /// The namespace implementation to use:
        /// <list type="bullet">
        /// <item><description><c>"dir"</c> — directory-based namespace for local storage.</description></item>
        /// <item><description><c>"rest"</c> — REST API namespace for LanceDB Cloud.</description></item>
        /// </list>
        /// </param>
        /// <param name="properties">
        /// Configuration properties for the namespace implementation.
        /// For <c>"dir"</c>, use <c>{"root": "/path/to/db"}</c>.
        /// </param>
        /// <param name="options">Options to control the connection behavior.</param>
        /// <returns>A task that completes when the connection is established.</returns>
        public async Task ConnectNamespace(string nsImpl, Dictionary<string, string> properties, ConnectionOptions? options = null)
        {
            byte[] nsImplBytes = NativeCall.ToUtf8(nsImpl);
            byte[] propsJson = NativeCall.ToUtf8(JsonSerializer.Serialize(properties));
            double rciSecs = options?.ReadConsistencyInterval.HasValue == true
                ? options.ReadConsistencyInterval!.Value.TotalSeconds
                : double.NaN;
            byte[]? storageJson = options?.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNsImpl = nsImplBytes)
                    fixed (byte* pProps = propsJson)
                    fixed (byte* pStorage = storageJson)
                    {
                        connection_connect_namespace(
                            new IntPtr(pNsImpl),
                            new IntPtr(pProps),
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                            rciSecs,
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
        /// <param name="ns">
        /// The namespace to drop the table from.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <param name="ignoreMissing">
        /// If <c>true</c>, ignore if the table does not exist.
        /// If <c>false</c> (default), a <see cref="LanceDbException"/> is thrown.
        /// </param>
        /// <exception cref="LanceDbException">
        /// Thrown if the table does not exist and <paramref name="ignoreMissing"/> is <c>false</c>.
        /// </exception>
        public async Task DropTable(string name, IReadOnlyList<string>? ns = null, bool ignoreMissing = false)
        {
            try
            {
                byte[] nameBytes = NativeCall.ToUtf8(name);
                byte[]? namespaceJson = ns != null
                    ? NativeCall.ToUtf8(JsonSerializer.Serialize(ns))
                    : null;
                await NativeCall.Async(callback =>
                {
                    unsafe
                    {
                        fixed (byte* p = nameBytes)
                        fixed (byte* pNamespace = namespaceJson)
                        {
                            connection_drop_table(
                                _handle!.DangerousGetHandle(),
                                new IntPtr(p),
                                namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                                callback);
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
        /// <param name="newName">The new name of the table.</param>
        /// <param name="curNamespace">
        /// The namespace of the current table.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <param name="newNamespace">
        /// The namespace to move the table to. If not specified, defaults
        /// to the same as <paramref name="curNamespace"/>.
        /// </param>
        public async Task RenameTable(string currentName, string newName, IReadOnlyList<string>? curNamespace = null, IReadOnlyList<string>? newNamespace = null)
        {
            byte[] oldNameBytes = NativeCall.ToUtf8(currentName);
            byte[] newNameBytes = NativeCall.ToUtf8(newName);
            byte[]? curNsJson = curNamespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(curNamespace))
                : null;
            byte[]? newNsJson = newNamespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(newNamespace))
                : null;

            await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pOld = oldNameBytes)
                    fixed (byte* pNew = newNameBytes)
                    fixed (byte* pCurNs = curNsJson)
                    fixed (byte* pNewNs = newNsJson)
                    {
                        connection_rename_table(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(pOld),
                            new IntPtr(pNew),
                            curNsJson != null ? new IntPtr(pCurNs) : IntPtr.Zero,
                            newNsJson != null ? new IntPtr(pNewNs) : IntPtr.Zero,
                            callback);
                    }
                }
            });
        }

        /// <summary>
        /// Clone a table from a source table.
        /// </summary>
        /// <remarks>
        /// A shallow clone creates a new table that shares the underlying data files
        /// with the source table but has its own independent manifest. This allows
        /// both the source and cloned tables to evolve independently while initially
        /// sharing the same data, deletion, and index files.
        /// </remarks>
        /// <param name="targetTableName">The name of the target table to create.</param>
        /// <param name="sourceUri">The URI of the source table to clone from.</param>
        /// <param name="targetNamespace">
        /// The namespace for the target table.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <param name="sourceVersion">
        /// The version of the source table to clone. If not specified, the latest
        /// version is used.
        /// </param>
        /// <param name="sourceTag">
        /// The tag of the source table to clone. Cannot be combined with
        /// <paramref name="sourceVersion"/>.
        /// </param>
        /// <param name="isShallow">
        /// Whether to perform a shallow clone (<c>true</c>) or deep clone (<c>false</c>).
        /// Currently only shallow clone is supported. Defaults to <c>true</c>.
        /// </param>
        /// <returns>A <see cref="Table"/> object representing the cloned table.</returns>
        public async Task<Table> CloneTable(
            string targetTableName,
            string sourceUri,
            IReadOnlyList<string>? targetNamespace = null,
            long? sourceVersion = null,
            string? sourceTag = null,
            bool isShallow = true)
        {
            byte[] targetNameBytes = NativeCall.ToUtf8(targetTableName);
            byte[] sourceUriBytes = NativeCall.ToUtf8(sourceUri);
            byte[]? targetNsJson = targetNamespace != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(targetNamespace))
                : null;
            byte[]? sourceTagBytes = sourceTag != null
                ? NativeCall.ToUtf8(sourceTag)
                : null;
            long versionSentinel = sourceVersion.HasValue ? (long)sourceVersion.Value : -1;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pTarget = targetNameBytes)
                    fixed (byte* pSourceUri = sourceUriBytes)
                    fixed (byte* pTargetNs = targetNsJson)
                    fixed (byte* pSourceTag = sourceTagBytes)
                    {
                        connection_clone_table(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(pTarget),
                            new IntPtr(pSourceUri),
                            targetNsJson != null ? new IntPtr(pTargetNs) : IntPtr.Zero,
                            versionSentinel,
                            sourceTagBytes != null ? new IntPtr(pSourceTag) : IntPtr.Zero,
                            isShallow,
                            callback);
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Drop all tables from the database.
        /// </summary>
        /// <param name="ns">
        /// The namespace to drop all tables from.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        public async Task DropAllTables(IReadOnlyList<string>? ns = null)
        {
            byte[]? namespaceJson = ns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(ns))
                : null;
            await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        connection_drop_all_tables(
                            _handle!.DangerousGetHandle(),
                            namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                            callback);
                    }
                }
            });
        }

        /// <summary>
        /// List immediate child namespace names in the given namespace.
        /// </summary>
        /// <param name="ns">
        /// The parent namespace to list namespaces in.
        /// <c>null</c> or an empty list represents the root namespace.
        /// </param>
        /// <param name="pageToken">
        /// Token for pagination. Use the token from a previous response
        /// to get the next page of results.
        /// </param>
        /// <param name="limit">
        /// The maximum number of namespaces to return. 0 or negative means no limit.
        /// </param>
        /// <returns>
        /// A <see cref="ListNamespacesResponse"/> containing the namespace names and
        /// an optional page token for the next page.
        /// </returns>
        public async Task<ListNamespacesResponse> ListNamespaces(IReadOnlyList<string>? ns = null, string? pageToken = null, int limit = 0)
        {
            byte[]? namespaceJson = ns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(ns))
                : null;
            byte[]? pageTokenBytes = pageToken != null
                ? NativeCall.ToUtf8(pageToken)
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNamespace = namespaceJson)
                    fixed (byte* pPageToken = pageTokenBytes)
                    {
                        connection_list_namespaces(
                            _handle!.DangerousGetHandle(),
                            namespaceJson != null ? new IntPtr(pNamespace) : IntPtr.Zero,
                            pageTokenBytes != null ? new IntPtr(pPageToken) : IntPtr.Zero,
                            limit,
                            callback);
                    }
                }
            });
            string json = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(json))
            {
                return new ListNamespacesResponse();
            }
            return JsonSerializer.Deserialize<ListNamespacesResponse>(json) ?? new ListNamespacesResponse();
        }

        /// <summary>
        /// Create a new namespace.
        /// </summary>
        /// <param name="ns">
        /// The namespace identifier to create, specified as a hierarchical path.
        /// For example, <c>["parent", "child"]</c> creates a nested namespace.
        /// </param>
        /// <param name="mode">
        /// The creation mode. Case insensitive:
        /// <list type="bullet">
        /// <item><description><c>"Create"</c> (default) — Fail if the namespace already exists.</description></item>
        /// <item><description><c>"ExistOk"</c> — Succeed even if the namespace already exists.</description></item>
        /// <item><description><c>"Overwrite"</c> — Overwrite the namespace if it exists.</description></item>
        /// </list>
        /// </param>
        /// <param name="properties">
        /// Optional key-value properties to set on the namespace.
        /// </param>
        /// <returns>
        /// A <see cref="CreateNamespaceResponse"/> containing the properties of the
        /// created namespace.
        /// </returns>
        public async Task<CreateNamespaceResponse> CreateNamespace(IReadOnlyList<string> ns, string? mode = null, Dictionary<string, string>? properties = null)
        {
            byte[] namespaceJson = NativeCall.ToUtf8(JsonSerializer.Serialize(ns));
            byte[]? modeBytes = mode != null
                ? NativeCall.ToUtf8(mode)
                : null;
            byte[]? propsJson = properties != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(properties))
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNamespace = namespaceJson)
                    fixed (byte* pMode = modeBytes)
                    fixed (byte* pProps = propsJson)
                    {
                        connection_create_namespace(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(pNamespace),
                            modeBytes != null ? new IntPtr(pMode) : IntPtr.Zero,
                            propsJson != null ? new IntPtr(pProps) : IntPtr.Zero,
                            callback);
                    }
                }
            });
            string json = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(json))
            {
                return new CreateNamespaceResponse();
            }
            return JsonSerializer.Deserialize<CreateNamespaceResponse>(json) ?? new CreateNamespaceResponse();
        }

        /// <summary>
        /// Drop a namespace.
        /// </summary>
        /// <param name="ns">
        /// The namespace identifier to drop, specified as a hierarchical path.
        /// </param>
        /// <param name="mode">
        /// Whether to skip if not exists or fail. Case insensitive:
        /// <list type="bullet">
        /// <item><description><c>"Fail"</c> (default) — Fail if the namespace does not exist.</description></item>
        /// <item><description><c>"Skip"</c> — Succeed even if the namespace does not exist.</description></item>
        /// </list>
        /// </param>
        /// <param name="behavior">
        /// Whether to restrict drop if not empty or cascade. Case insensitive:
        /// <list type="bullet">
        /// <item><description><c>"Restrict"</c> (default) — Fail if the namespace is not empty.</description></item>
        /// <item><description><c>"Cascade"</c> — Drop all child namespaces and tables.</description></item>
        /// </list>
        /// </param>
        /// <returns>
        /// A <see cref="DropNamespaceResponse"/> containing properties and
        /// transaction ID if applicable.
        /// </returns>
        public async Task<DropNamespaceResponse> DropNamespace(IReadOnlyList<string> ns, string? mode = null, string? behavior = null)
        {
            byte[] namespaceJson = NativeCall.ToUtf8(JsonSerializer.Serialize(ns));
            byte[]? modeBytes = mode != null
                ? NativeCall.ToUtf8(mode)
                : null;
            byte[]? behaviorBytes = behavior != null
                ? NativeCall.ToUtf8(behavior)
                : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNamespace = namespaceJson)
                    fixed (byte* pMode = modeBytes)
                    fixed (byte* pBehavior = behaviorBytes)
                    {
                        connection_drop_namespace(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(pNamespace),
                            modeBytes != null ? new IntPtr(pMode) : IntPtr.Zero,
                            behaviorBytes != null ? new IntPtr(pBehavior) : IntPtr.Zero,
                            callback);
                    }
                }
            });
            string json = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(json))
            {
                return new DropNamespaceResponse();
            }
            return JsonSerializer.Deserialize<DropNamespaceResponse>(json) ?? new DropNamespaceResponse();
        }

        /// <summary>
        /// Describe a namespace.
        /// </summary>
        /// <param name="ns">
        /// The namespace identifier to describe, specified as a hierarchical path.
        /// </param>
        /// <returns>
        /// A <see cref="DescribeNamespaceResponse"/> containing the namespace properties.
        /// </returns>
        public async Task<DescribeNamespaceResponse> DescribeNamespace(IReadOnlyList<string> ns)
        {
            byte[] namespaceJson = NativeCall.ToUtf8(JsonSerializer.Serialize(ns));

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pNamespace = namespaceJson)
                    {
                        connection_describe_namespace(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(pNamespace),
                            callback);
                    }
                }
            });
            string json = NativeCall.ReadStringAndFree(ptr);
            if (string.IsNullOrEmpty(json))
            {
                return new DescribeNamespaceResponse();
            }
            return JsonSerializer.Deserialize<DescribeNamespaceResponse>(json) ?? new DescribeNamespaceResponse();
        }
    }
}