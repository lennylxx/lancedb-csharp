namespace lancedb
{
    using System.Runtime.InteropServices;
    using System.Text.Json;
    using Apache.Arrow;
    using Apache.Arrow.Ipc;

    /// <summary>
    /// A Table is a collection of Records in a LanceDB Database.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A Table object is expected to be long lived and reused for multiple operations.
    /// Table objects will cache a certain amount of index data in memory. This cache
    /// will be freed when the Table is garbage collected. To eagerly free the cache you
    /// can call the <see cref="Close"/> method. Once the Table is closed, it cannot be
    /// used for any further operations.
    /// </para>
    /// <para>
    /// Closing a table is optional. If not closed, it will be closed when it is garbage
    /// collected.
    /// </para>
    /// </remarks>
    public class Table : IDisposable
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustStringHandle table_get_name(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool table_is_open(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr table_create_query(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_count_rows(
            IntPtr table_ptr, IntPtr filter, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_delete(
            IntPtr table_ptr, IntPtr predicate, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_update(
            IntPtr table_ptr, IntPtr filter, IntPtr columns_json,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_schema(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void free_ffi_bytes(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_add(
            IntPtr table_ptr, IntPtr ipc_data, nuint ipc_len, IntPtr mode,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_version(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_list_versions(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_checkout(
            IntPtr table_ptr, ulong version, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_checkout_latest(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_restore(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_uri(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_create_index(
            IntPtr table_ptr, IntPtr columns_json, IntPtr index_type, IntPtr config_json,
            bool replace, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_list_indices(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_add_columns(
            IntPtr table_ptr, IntPtr transforms_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_alter_columns(
            IntPtr table_ptr, IntPtr alterations_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_drop_columns(
            IntPtr table_ptr, IntPtr columns_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_optimize(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_tags_list(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_tags_create(
            IntPtr table_ptr, IntPtr tag, ulong version, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_tags_delete(
            IntPtr table_ptr, IntPtr tag, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_tags_update(
            IntPtr table_ptr, IntPtr tag, ulong version, NativeCall.FfiCallback completion);

        private TableHandle? _handle;

        internal Table(IntPtr tablePtr)
        {
            _handle = new TableHandle(tablePtr);
        }

        /// <summary>
        /// The name of the table.
        /// </summary>
        public string Name
        {
            get
            {
                using var nameHandle = table_get_name(_handle!.DangerousGetHandle());
                return nameHandle.AsString();
            }
        }

        /// <summary>
        /// Return <c>true</c> if the table has not been closed.
        /// </summary>
        /// <returns><c>true</c> if the table is open; otherwise, <c>false</c>.</returns>
        public bool IsOpen() => _handle != null && !_handle.IsInvalid && !_handle.IsClosed;

        /// <summary>
        /// Close the table and free any resources associated with it.
        /// </summary>
        /// <remarks>
        /// It is safe to call this method multiple times.
        /// Any attempt to use the table after it has been closed will raise an error.
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
        /// Create a <see cref="lancedb.Query"/> builder.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Queries allow you to search your existing data. By default the query will
        /// return all the data in the table in no particular order. The builder
        /// returned by this method can be used to control the query using filtering,
        /// vector similarity, sorting, and more.
        /// </para>
        /// <para>
        /// By default, all columns are returned. For best performance, you should
        /// only fetch the columns you need.
        /// </para>
        /// <para>
        /// When appropriate, various indices and statistics will be used to accelerate
        /// the query.
        /// </para>
        /// </remarks>
        /// <returns>A builder that can be used to parameterize the query.</returns>
        public Query Query()
        {
            IntPtr queryPtr = table_create_query(_handle!.DangerousGetHandle());
            return new Query(queryPtr);
        }

        /// <summary>
        /// Count the number of rows in the table.
        /// </summary>
        /// <param name="filter">
        /// A SQL where clause to filter the rows to count. If <c>null</c>, counts all rows.
        /// </param>
        /// <returns>The number of rows matching the filter, or total rows if no filter.</returns>
        public async Task<long> CountRows(string? filter = null)
        {
            IntPtr result;
            if (filter == null)
            {
                result = await NativeCall.Async(completion =>
                {
                    table_count_rows(_handle!.DangerousGetHandle(), IntPtr.Zero, completion);
                }).ConfigureAwait(false);
            }
            else
            {
                byte[] utf8Filter = NativeCall.ToUtf8(filter);
                result = await NativeCall.Async(completion =>
                {
                    unsafe
                    {
                        fixed (byte* p = utf8Filter)
                        {
                            table_count_rows(
                                _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                        }
                    }
                }).ConfigureAwait(false);
            }
            return result.ToInt64();
        }

        /// <summary>
        /// Delete rows from the table.
        /// </summary>
        /// <remarks>
        /// This can be used to delete a single row, many rows, all rows, or
        /// sometimes no rows (if your predicate matches nothing).
        /// </remarks>
        /// <param name="predicate">
        /// The SQL where clause to use when deleting rows.
        /// For example, <c>"x = 2"</c> or <c>"x IN (1, 2, 3)"</c>.
        /// The filter must not be empty, or it will error.
        /// </param>
        public async Task Delete(string predicate)
        {
            byte[] utf8Predicate = NativeCall.ToUtf8(predicate);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Predicate)
                    {
                        table_delete(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Update rows in the table.
        /// </summary>
        /// <remarks>
        /// An update operation allows you to change the values of existing rows
        /// in a table. The update operation is similar to a SQL UPDATE statement.
        /// </remarks>
        /// <param name="values">
        /// A dictionary mapping column names to SQL expressions describing the
        /// updated value. For example, <c>new Dictionary&lt;string, string&gt;
        /// { { "x", "x + 1" } }</c>.
        /// </param>
        /// <param name="where">
        /// A SQL where clause to filter which rows are updated.
        /// If <c>null</c>, all rows are updated.
        /// </param>
        public async Task Update(Dictionary<string, string> values, string? @where = null)
        {
            var columns = values.Select(kv => new[] { kv.Key, kv.Value }).ToArray();
            byte[] utf8Columns = NativeCall.ToUtf8(JsonSerializer.Serialize(columns));

            if (@where == null)
            {
                await NativeCall.Async(completion =>
                {
                    unsafe
                    {
                        fixed (byte* pc = utf8Columns)
                        {
                            table_update(
                                _handle!.DangerousGetHandle(), IntPtr.Zero, (IntPtr)pc,
                                completion);
                        }
                    }
                }).ConfigureAwait(false);
            }
            else
            {
                byte[] utf8Where = NativeCall.ToUtf8(@where);
                await NativeCall.Async(completion =>
                {
                    unsafe
                    {
                        fixed (byte* pw = utf8Where)
                        fixed (byte* pc = utf8Columns)
                        {
                            table_update(
                                _handle!.DangerousGetHandle(), (IntPtr)pw, (IntPtr)pc,
                                completion);
                        }
                    }
                }).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Return the schema of the table.
        /// </summary>
        /// <returns>
        /// An <see cref="Apache.Arrow.Schema"/> object describing the columns in the table.
        /// </returns>
        public async Task<Schema> Schema()
        {
            IntPtr ffiBytesPtr = await NativeCall.Async(completion =>
            {
                table_schema(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);

            try
            {
                return ReadSchemaFromFfiBytes(ffiBytesPtr);
            }
            finally
            {
                free_ffi_bytes(ffiBytesPtr);
            }
        }

        private static unsafe Schema ReadSchemaFromFfiBytes(IntPtr ffiBytesPtr)
        {
            var dataPtr = Marshal.ReadIntPtr(ffiBytesPtr);
            var len = Marshal.ReadIntPtr(ffiBytesPtr + IntPtr.Size).ToInt64();

            byte[] managedBytes = new byte[len];
            Marshal.Copy(dataPtr, managedBytes, 0, (int)len);

            using var stream = new MemoryStream(managedBytes);
            using var reader = new ArrowFileReader(stream);
            return reader.Schema;
        }

        /// <summary>
        /// Add more data to the Table.
        /// </summary>
        /// <param name="data">
        /// The data to add, as one or more Arrow <see cref="RecordBatch"/> objects.
        /// </param>
        /// <param name="mode">
        /// The mode to use when adding data. Default is <c>"append"</c>.
        /// - <c>"append"</c> - Append the new data to the table.
        /// - <c>"overwrite"</c> - Replace the existing data with the new data.
        /// </param>
        public async Task Add(IReadOnlyList<RecordBatch> data, string mode = "append")
        {
            byte[] ipcBytes = SerializeToIpc(data);
            byte[] utf8Mode = NativeCall.ToUtf8(mode);

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pData = ipcBytes)
                    fixed (byte* pMode = utf8Mode)
                    {
                        table_add(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pData, (nuint)ipcBytes.Length, (IntPtr)pMode,
                            completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Add a single <see cref="RecordBatch"/> to the Table.
        /// </summary>
        /// <param name="data">The data to add.</param>
        /// <param name="mode">
        /// The mode to use when adding data. Default is <c>"append"</c>.
        /// - <c>"append"</c> - Append the new data to the table.
        /// - <c>"overwrite"</c> - Replace the existing data with the new data.
        /// </param>
        public Task Add(RecordBatch data, string mode = "append")
        {
            return Add(new[] { data }, mode);
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

        /// <summary>
        /// The version of this table.
        /// </summary>
        /// <returns>The current version number.</returns>
        public async Task<ulong> Version()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_version(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            return (ulong)result.ToInt64();
        }

        /// <summary>
        /// List all versions of the table.
        /// </summary>
        /// <returns>A list of <see cref="VersionInfo"/> describing each version.</returns>
        public async Task<IReadOnlyList<VersionInfo>> ListVersions()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_list_versions(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<List<VersionInfo>>(json)
                ?? new List<VersionInfo>();
        }

        /// <summary>
        /// Checks out a specific version of the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Any read operation on the table will now access the data at the checked out
        /// version. As a consequence, calling this method will disable any read consistency
        /// interval that was previously set.
        /// </para>
        /// <para>
        /// This is a read-only operation that turns the table into a sort of "view"
        /// or "detached head". Other table instances will not be affected. To make the
        /// change permanent you can use the <see cref="Restore"/> method.
        /// </para>
        /// <para>
        /// Any operation that modifies the table will fail while the table is in a checked
        /// out state.
        /// </para>
        /// <para>
        /// To return the table to a normal state use <see cref="CheckoutLatest"/>.
        /// </para>
        /// </remarks>
        /// <param name="version">The version number to check out.</param>
        public async Task Checkout(ulong version)
        {
            await NativeCall.Async(completion =>
            {
                table_checkout(_handle!.DangerousGetHandle(), version, completion);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures the table is pointing at the latest version.
        /// </summary>
        /// <remarks>
        /// This can be used to manually update a table when the read consistency interval
        /// is not set. It can also be used to undo a <see cref="Checkout"/> operation.
        /// </remarks>
        public async Task CheckoutLatest()
        {
            await NativeCall.Async(completion =>
            {
                table_checkout_latest(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Restore the table to the currently checked out version.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This operation will fail if <see cref="Checkout"/> has not been called previously.
        /// </para>
        /// <para>
        /// This operation will overwrite the latest version of the table with a
        /// previous version. Any changes made since the checked out version will
        /// no longer be visible.
        /// </para>
        /// <para>
        /// Once the operation concludes the table will no longer be in a checked
        /// out state and the read consistency interval, if any, will apply.
        /// </para>
        /// </remarks>
        public async Task Restore()
        {
            await NativeCall.Async(completion =>
            {
                table_restore(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the table's storage URI.
        /// </summary>
        /// <returns>The URI of the table's storage location.</returns>
        public async Task<string> Uri()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_uri(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            return NativeCall.ReadStringAndFree(result);
        }

        /// <summary>
        /// Create an index on this table.
        /// </summary>
        /// <param name="columns">
        /// The columns to index. Currently only single-column indices are supported,
        /// but this accepts a list for future composite index support.
        /// </param>
        /// <param name="index">
        /// The index configuration. Use one of the concrete index classes:
        /// <see cref="BTreeIndex"/>, <see cref="BitmapIndex"/>, <see cref="LabelListIndex"/>,
        /// <see cref="FtsIndex"/>, <see cref="IvfPqIndex"/>, <see cref="HnswPqIndex"/>,
        /// <see cref="HnswSqIndex"/>.
        /// </param>
        /// <param name="replace">
        /// Whether to replace an existing index on the same columns. Default is <c>true</c>.
        /// </param>
        public async Task CreateIndex(IReadOnlyList<string> columns, Index index, bool replace = true)
        {
            string columnsJson = JsonSerializer.Serialize(columns);
            byte[] columnsBytes = NativeCall.ToUtf8(columnsJson);
            byte[] typeBytes = NativeCall.ToUtf8(index.IndexType);
            byte[] configBytes = NativeCall.ToUtf8(index.ToConfigJson());

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pColumns = columnsBytes)
                    fixed (byte* pType = typeBytes)
                    fixed (byte* pConfig = configBytes)
                    {
                        table_create_index(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pColumns, (IntPtr)pType, (IntPtr)pConfig,
                            replace, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// List all indices that have been created on this table.
        /// </summary>
        /// <returns>A list of <see cref="IndexInfo"/> describing each index.</returns>
        public async Task<IReadOnlyList<IndexInfo>> ListIndices()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_list_indices(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<List<IndexInfo>>(json)
                ?? new List<IndexInfo>();
        }

        /// <summary>
        /// Add new columns with defined values.
        /// </summary>
        /// <remarks>
        /// A map of column name to a SQL expression to use to calculate the
        /// value of the new column. These expressions will be evaluated for
        /// each row in the table, and can reference existing columns.
        /// </remarks>
        /// <param name="transforms">
        /// A dictionary mapping new column names to SQL expressions.
        /// For example, <c>new Dictionary&lt;string, string&gt; { { "doubled", "id * 2" } }</c>.
        /// </param>
        public async Task AddColumns(Dictionary<string, string> transforms)
        {
            var pairs = transforms.Select(kv => new[] { kv.Key, kv.Value }).ToArray();
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(pairs));

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Json)
                    {
                        table_add_columns(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Alter column names and nullability.
        /// </summary>
        /// <remarks>
        /// Each alteration specifies a column path and optional changes:
        /// - <c>path</c>: The column name to alter. For nested columns, use dot-separated paths.
        /// - <c>rename</c>: The new name of the column.
        /// - <c>nullable</c>: Whether the column should be nullable. Only non-nullable columns
        ///   can be changed to nullable.
        /// </remarks>
        /// <param name="alterations">
        /// A list of alterations, each as a dictionary with keys: <c>"path"</c> (required),
        /// <c>"rename"</c> (optional), <c>"nullable"</c> (optional).
        /// </param>
        public async Task AlterColumns(IReadOnlyList<Dictionary<string, object>> alterations)
        {
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(alterations));

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Json)
                    {
                        table_alter_columns(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Drop columns from the table.
        /// </summary>
        /// <param name="columns">The names of the columns to drop.</param>
        public async Task DropColumns(IReadOnlyList<string> columns)
        {
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(columns));

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Json)
                    {
                        table_drop_columns(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Optimize the on-disk data and indices for better performance.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Modeled after <c>VACUUM</c> in PostgreSQL. Optimization covers three operations:
        /// </para>
        /// <para>
        /// - Compaction: Merges small files into larger ones
        /// </para>
        /// <para>
        /// - Prune: Removes old versions of the dataset
        /// </para>
        /// <para>
        /// - Index: Optimizes the indices, adding new data to existing indices
        /// </para>
        /// <para>
        /// The frequency an application should call optimize is based on the frequency of
        /// data modifications. If data is frequently added, deleted, or updated then
        /// optimize should be run frequently. A good rule of thumb is to run optimize if
        /// you have added or modified 100,000 or more records or run more than 20 data
        /// modification operations.
        /// </para>
        /// </remarks>
        /// <returns>Statistics about the optimization operation.</returns>
        public async Task<OptimizeStats> Optimize()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_optimize(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<OptimizeStats>(json) ?? new OptimizeStats();
        }

        /// <summary>
        /// List all tags on this table.
        /// </summary>
        /// <remarks>
        /// Tags are similar to Git tags — they provide named references to specific
        /// versions of a table. Tagged versions are exempt from cleanup operations.
        /// To remove a version that has been tagged, you must first delete the tag.
        /// </remarks>
        /// <returns>A dictionary mapping tag names to <see cref="TagInfo"/>.</returns>
        public async Task<Dictionary<string, TagInfo>> ListTags()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_tags_list(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<Dictionary<string, TagInfo>>(json)
                ?? new Dictionary<string, TagInfo>();
        }

        /// <summary>
        /// Create a new tag for the given version of the table.
        /// </summary>
        /// <param name="tag">The name of the tag to create.</param>
        /// <param name="version">The version number to tag.</param>
        public async Task CreateTag(string tag, ulong version)
        {
            byte[] utf8Tag = NativeCall.ToUtf8(tag);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Tag)
                    {
                        table_tags_create(
                            _handle!.DangerousGetHandle(), (IntPtr)p, version, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete a tag from the table.
        /// </summary>
        /// <param name="tag">The name of the tag to delete.</param>
        public async Task DeleteTag(string tag)
        {
            byte[] utf8Tag = NativeCall.ToUtf8(tag);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Tag)
                    {
                        table_tags_delete(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Update an existing tag to point to a new version of the table.
        /// </summary>
        /// <param name="tag">The name of the tag to update.</param>
        /// <param name="version">The new version number for the tag.</param>
        public async Task UpdateTag(string tag, ulong version)
        {
            byte[] utf8Tag = NativeCall.ToUtf8(tag);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Tag)
                    {
                        table_tags_update(
                            _handle!.DangerousGetHandle(), (IntPtr)p, version, completion);
                    }
                }
            }).ConfigureAwait(false);
        }
    }
}