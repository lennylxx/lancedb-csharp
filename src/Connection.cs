namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Text.Json;
    using Apache.Arrow;
    using Apache.Arrow.Ipc;

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
        private static extern void database_connect(IntPtr uri, double read_consistency_interval_secs, IntPtr storage_options_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_open_table(IntPtr connection_ptr, IntPtr table_name, IntPtr storage_options_json, uint index_cache_size, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_create_empty_table(IntPtr connection_ptr, IntPtr table_name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_create_table(IntPtr connection_ptr, IntPtr table_name, IntPtr ipc_data, nuint ipc_len, IntPtr mode, IntPtr storage_options_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_table_names(IntPtr connection_ptr, IntPtr start_after, uint limit, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_drop_table(IntPtr connection_ptr, IntPtr table_name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_drop_all_tables(IntPtr connection_ptr, NativeCall.FfiCallback completion);

        private ConnectionHandle? _handle;

        public Connection()
        {
        }

        /// <summary>
        /// Connect to a LanceDB database.
        /// </summary>
        /// <param name="uri">
        /// The uri of the database. Accepted formats:
        /// - /path/to/database — local database on file system
        /// - s3://bucket/path or gs://bucket/path — database on cloud storage
        /// - db://host:port — remote database (LanceDB Cloud)
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

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = uriBytes)
                    fixed (byte* pStorage = storageJson)
                    {
                        database_connect(
                            new IntPtr(p),
                            rciSecs,
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
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
        /// <param name="options">Options to control the open behavior.</param>
        /// <returns>A <see cref="Table"/> representing the opened table.</returns>
        /// <exception cref="LanceDbException">Thrown if the table does not exist.</exception>
        public async Task<Table> OpenTable(string name, OpenTableOptions? options = null)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            byte[]? storageJson = options?.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;
            uint indexCacheSize = options?.IndexCacheSize ?? 0;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    fixed (byte* pStorage = storageJson)
                    {
                        database_open_table(
                            _handle!.DangerousGetHandle(),
                            new IntPtr(p),
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                            indexCacheSize,
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
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">Thrown if a table with the same name already exists.</exception>
        /// <remarks>
        /// The vector index is not created by default.
        /// To create the index, call the <c>CreateIndex</c> method on the table.
        /// </remarks>
        public async Task<Table> CreateEmptyTable(string name)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        database_create_empty_table(_handle!.DangerousGetHandle(), new IntPtr(p), callback);
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Create a table in the database.
        /// </summary>
        /// <param name="name">The name of the table.</param>
        /// <param name="options">
        /// Options to control the create behavior, including the initial data,
        /// create mode, and storage options.
        /// </param>
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">
        /// Thrown if a table with the same name already exists and mode is <c>"create"</c>.
        /// </exception>
        public async Task<Table> CreateTable(string name, CreateTableOptions options)
        {
            if (options.Data == null || options.Data.Count == 0)
            {
                throw new ArgumentException("At least one RecordBatch is required in Data.", nameof(options));
            }

            byte[] nameBytes = NativeCall.ToUtf8(name);
            byte[] ipcBytes = SerializeToIpc(options.Data);
            byte[] modeBytes = NativeCall.ToUtf8(options.Mode);
            byte[]? storageJson = options.StorageOptions != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(options.StorageOptions))
                : null;

            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pName = nameBytes)
                    fixed (byte* pData = ipcBytes)
                    fixed (byte* pMode = modeBytes)
                    fixed (byte* pStorage = storageJson)
                    {
                        database_create_table(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pName,
                            (IntPtr)pData, (nuint)ipcBytes.Length, (IntPtr)pMode,
                            storageJson != null ? new IntPtr(pStorage) : IntPtr.Zero,
                            callback);
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
        /// - <c>"create"</c> - Create the table. An error is raised if the table already exists.
        /// - <c>"overwrite"</c> - If a table with the same name already exists, it is replaced.
        /// </param>
        /// <returns>A <see cref="Table"/> representing the newly created table.</returns>
        /// <exception cref="LanceDbException">
        /// Thrown if a table with the same name already exists and mode is <c>"create"</c>.
        /// </exception>
        public Task<Table> CreateTable(string name, RecordBatch data, string mode = "create")
        {
            return CreateTable(name, new CreateTableOptions { Data = new[] { data }, Mode = mode });
        }

        /// <summary>
        /// Get the names of all tables in the database.
        /// </summary>
        /// <param name="startAfter">
        /// If present, only return names that come lexicographically after the supplied
        /// value. Can be combined with <paramref name="limit"/> to implement pagination.
        /// </param>
        /// <param name="limit">
        /// The maximum number of table names to return. If <c>null</c> or 0, all names
        /// are returned.
        /// </param>
        /// <returns>A list of table names in lexicographical order.</returns>
        public async Task<IReadOnlyList<string>> TableNames(string? startAfter = null, uint limit = 0)
        {
            byte[]? startAfterBytes = startAfter != null ? NativeCall.ToUtf8(startAfter) : null;

            IntPtr ptr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* pStartAfter = startAfterBytes)
                    {
                        database_table_names(
                            _handle!.DangerousGetHandle(),
                            startAfterBytes != null ? new IntPtr(pStartAfter) : IntPtr.Zero,
                            limit,
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
        /// Drop a table from the database.
        /// </summary>
        /// <param name="name">The name of the table to drop.</param>
        /// <exception cref="LanceDbException">Thrown if the table does not exist.</exception>
        public async Task DropTable(string name)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        database_drop_table(_handle!.DangerousGetHandle(), new IntPtr(p), callback);
                    }
                }
            });
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
                database_drop_all_tables(_handle!.DangerousGetHandle(), callback);
            });
        }

        private static byte[] SerializeToIpc(IReadOnlyList<RecordBatch> batches)
        {
            if (batches.Count == 0)
            {
                throw new ArgumentException("At least one RecordBatch is required.", nameof(batches));
            }

            using var stream = new MemoryStream();
            using (var writer = new ArrowFileWriter(stream, batches[0].Schema))
            {
                foreach (var batch in batches)
                {
                    writer.WriteRecordBatch(batch);
                }
                writer.WriteEnd();
            }
            return stream.ToArray();
        }
    }
}