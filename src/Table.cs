namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text.Json;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.C;

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
        private static extern IntPtr table_get_name(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool table_is_open(IntPtr table_ptr);

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
        private static extern void table_update_result_free(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_merge_result_free(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_schema(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe void table_add(
            IntPtr table_ptr, CArrowArray* arrays, CArrowSchema* schema, nuint batch_count,
            IntPtr mode, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_version(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_uses_v2_manifest_paths(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_migrate_manifest_paths_v2(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_replace_field_metadata(
            IntPtr table_ptr, IntPtr field_name, IntPtr metadata_json,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_list_versions(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_checkout(
            IntPtr table_ptr, ulong version, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_checkout_tag(
            IntPtr table_ptr, IntPtr tag, NativeCall.FfiCallback completion);

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
            IntPtr table_ptr, IntPtr columns_json, int index_type, IntPtr config_json,
            bool replace, IntPtr name, bool train, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_list_indices(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_drop_index(
            IntPtr table_ptr, IntPtr name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_prewarm_index(
            IntPtr table_ptr, IntPtr name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_wait_for_index(
            IntPtr table_ptr, IntPtr index_names_json, long timeout_ms,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_index_stats(
            IntPtr table_ptr, IntPtr index_name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_index_stats_free(IntPtr ptr);

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
            IntPtr table_ptr, long cleanup_older_than_ms, bool delete_unverified,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_stats(
            IntPtr table_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_stats_free(IntPtr ptr);

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

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_tags_get_version(
            IntPtr table_ptr, IntPtr tag, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe void table_merge_insert(
            IntPtr table_ptr, IntPtr on_columns_json,
            bool when_matched_update_all, IntPtr when_matched_update_all_filter,
            bool when_not_matched_insert_all,
            bool when_not_matched_by_source_delete, IntPtr when_not_matched_by_source_delete_filter,
            CArrowArray* arrays, CArrowSchema* schema, nuint batch_count,
            [MarshalAs(UnmanagedType.U1)] bool use_index, long timeout_ms,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_take_offsets(
            IntPtr table_ptr, IntPtr offsets, nuint offsets_len, IntPtr columns_json,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_take_row_ids(
            IntPtr table_ptr, IntPtr row_ids, nuint row_ids_len, IntPtr columns_json,
            NativeCall.FfiCallback completion);

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
                IntPtr ptr = table_get_name(_handle!.DangerousGetHandle());
                NativeCall.ThrowIfNullWithError(ptr, "Failed to get table name");
                return NativeCall.ReadStringAndFree(ptr);
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
            return new Query(_handle!.DangerousGetHandle());
        }

        /// <summary>
        /// Return the first <paramref name="n"/> rows of the table.
        /// </summary>
        /// <param name="n">The number of rows to return. Defaults to 5.</param>
        /// <returns>A <see cref="Apache.Arrow.RecordBatch"/> containing the first <paramref name="n"/> rows.</returns>
        public async Task<Apache.Arrow.RecordBatch> Head(int n = 5)
        {
            using var query = Query().Limit(n);
            return await query.ToArrow();
        }

        /// <summary>
        /// Return the entire table as an Arrow <see cref="Apache.Arrow.RecordBatch"/>.
        /// </summary>
        /// <remarks>
        /// This is a convenience method equivalent to <c>Query().ToArrow()</c>.
        /// For large tables, consider using <see cref="Query"/> with filters or limits instead.
        /// </remarks>
        /// <returns>A <see cref="Apache.Arrow.RecordBatch"/> containing all rows in the table.</returns>
        public async Task<Apache.Arrow.RecordBatch> ToArrow()
        {
            using var query = Query();
            return await query.ToArrow();
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
        /// Return statistics about the table.
        /// </summary>
        /// <remarks>
        /// This includes the total number of bytes, rows, and indices in the table,
        /// as well as fragment-level statistics (number of fragments, number of small
        /// fragments, and percentile summaries of fragment row counts).
        /// </remarks>
        /// <returns>
        /// A <see cref="TableStatistics"/> object with the table's current statistics.
        /// </returns>
        public async Task<TableStatistics> Stats()
        {
            var result = await NativeCall.Async(completion =>
            {
                table_stats(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);

            try
            {
                var ffi = Marshal.PtrToStructure<FfiTableStats>(result);
                return new TableStatistics(ffi);
            }
            finally
            {
                table_stats_free(result);
            }
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
        /// <returns>
        /// A <see cref="DeleteResult"/> containing the commit version of the operation.
        /// </returns>
        public async Task<DeleteResult> Delete(string predicate)
        {
            byte[] utf8Predicate = NativeCall.ToUtf8(predicate);
            IntPtr resultPtr = await NativeCall.Async(completion =>
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

            return new DeleteResult { Version = (ulong)resultPtr.ToInt64() };
        }

        /// <summary>
        /// Update rows in the table using SQL expressions.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This can be used to update zero to all rows in the table.
        /// If a filter is provided with <paramref name="where"/> then only rows matching
        /// the filter will be updated. Otherwise all rows will be updated.
        /// </para>
        /// <para>
        /// The keys are column names, and the values are SQL expressions describing
        /// the updated value (e.g., <c>"x + 1"</c> or <c>"'hello'"</c>).
        /// </para>
        /// </remarks>
        /// <param name="updatesSql">
        /// A dictionary mapping column names to SQL expressions describing the
        /// updated value. For example, <c>new Dictionary&lt;string, string&gt;
        /// { { "x", "x + 1" } }</c>.
        /// </param>
        /// <param name="where">
        /// An optional SQL filter that controls which rows are updated
        /// (e.g., <c>"x = 2"</c>). If <c>null</c>, all rows are updated.
        /// </param>
        /// <returns>
        /// An <see cref="UpdateResult"/> containing the number of rows updated and the
        /// commit version of the operation.
        /// </returns>
        public async Task<UpdateResult> Update(Dictionary<string, string> updatesSql, string? @where = null)
        {
            var columnSqlExprs = updatesSql.Select(kv => new[] { kv.Key, kv.Value }).ToArray();
            byte[] utf8ColumnSqlExprs = NativeCall.ToUtf8(JsonSerializer.Serialize(columnSqlExprs));

            IntPtr resultPtr;
            if (@where == null)
            {
                resultPtr = await NativeCall.Async(completion =>
                {
                    unsafe
                    {
                        fixed (byte* pc = utf8ColumnSqlExprs)
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
                resultPtr = await NativeCall.Async(completion =>
                {
                    unsafe
                    {
                        fixed (byte* pw = utf8Where)
                        fixed (byte* pc = utf8ColumnSqlExprs)
                        {
                            table_update(
                                _handle!.DangerousGetHandle(), (IntPtr)pw, (IntPtr)pc,
                                completion);
                        }
                    }
                }).ConfigureAwait(false);
            }

            try
            {
                return Marshal.PtrToStructure<UpdateResult>(resultPtr);
            }
            finally
            {
                table_update_result_free(resultPtr);
            }
        }

        /// <summary>
        /// Return the <see cref="Apache.Arrow.Schema">Arrow Schema</see> of this table.
        /// </summary>
        /// <returns>
        /// An <see cref="Apache.Arrow.Schema"/> object describing the columns in the table.
        /// </returns>
        public async Task<Schema> Schema()
        {
            IntPtr ffiSchemaPtr = await NativeCall.Async(completion =>
            {
                table_schema(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);

            try
            {
                unsafe
                {
                    return CArrowSchemaImporter.ImportSchema(
                        (CArrowSchema*)ffiSchemaPtr);
                }
            }
            finally
            {
                NativeCall.free_ffi_schema(ffiSchemaPtr);
            }
        }

        /// <summary>
        /// Add more data to the <see cref="Table"/>.
        /// </summary>
        /// <param name="data">
        /// The data to insert into the table, as one or more Arrow <see cref="RecordBatch"/> objects.
        /// </param>
        /// <param name="mode">
        /// The mode to use when writing the data. Valid values are:
        /// <list type="bullet">
        /// <item><description><c>"append"</c> (default) — Append the new data to the table.</description></item>
        /// <item><description><c>"overwrite"</c> — Replace the existing data with the new data.</description></item>
        /// </list>
        /// </param>
        /// <returns>
        /// An <see cref="AddResult"/> containing the commit version of the operation.
        /// </returns>
        public Task<AddResult> Add(IReadOnlyList<RecordBatch> data, string mode = "append")
        {
            return Add(data, new AddOptions { Mode = mode });
        }

        /// <summary>
        /// Add more data to the <see cref="Table"/> with options for handling bad vectors.
        /// </summary>
        /// <param name="data">
        /// The data to insert into the table, as one or more Arrow <see cref="RecordBatch"/> objects.
        /// </param>
        /// <param name="options">Options controlling the write mode and bad vector handling.
        /// See <see cref="AddOptions.OnBadVectors"/> for what to do if any of the vectors
        /// are not the same size or contain NaNs.</param>
        /// <returns>
        /// An <see cref="AddResult"/> containing the commit version of the operation.
        /// </returns>
        public async Task<AddResult> Add(IReadOnlyList<RecordBatch> data, AddOptions options)
        {
            var processed = new RecordBatch[data.Count];
            for (int i = 0; i < data.Count; i++)
            {
                processed[i] = VectorValidator.HandleBadVectors(
                    data[i], options.OnBadVectors, options.FillValue);
            }

            byte[] utf8Mode = NativeCall.ToUtf8(options.Mode);

            IntPtr resultPtr = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pMode = utf8Mode)
                    {
                        var cArrays = new CArrowArray[processed.Length];
                        var cSchemaArr = new CArrowSchema[1];
                        fixed (CArrowSchema* pSchema = cSchemaArr)
                        {
                            CArrowSchemaExporter.ExportSchema(processed[0].Schema, pSchema);
                            for (int i = 0; i < processed.Length; i++)
                            {
                                cArrays[i] = default;
                                var clone = ArrowExportHelper.CloneBatchForExport(processed[i]);
                                fixed (CArrowArray* pArr = &cArrays[i])
                                {
                                    CArrowArrayExporter.ExportRecordBatch(clone, pArr);
                                }
                            }
                            fixed (CArrowArray* pArrays = cArrays)
                            {
                                table_add(
                                    _handle!.DangerousGetHandle(),
                                    pArrays, pSchema, (nuint)processed.Length,
                                    (IntPtr)pMode,
                                    completion);
                            }
                        }
                    }
                }
            }).ConfigureAwait(false);

            return new AddResult { Version = (ulong)resultPtr.ToInt64() };
        }

        /// <summary>
        /// Add a single <see cref="RecordBatch"/> to the <see cref="Table"/>.
        /// </summary>
        /// <param name="data">The data to insert into the table.</param>
        /// <param name="mode">
        /// The mode to use when writing the data. Valid values are:
        /// <list type="bullet">
        /// <item><description><c>"append"</c> (default) — Append the new data to the table.</description></item>
        /// <item><description><c>"overwrite"</c> — Replace the existing data with the new data.</description></item>
        /// </list>
        /// </param>
        /// <returns>
        /// An <see cref="AddResult"/> containing the commit version of the operation.
        /// </returns>
        public Task<AddResult> Add(RecordBatch data, string mode = "append")
        {
            return Add(new[] { data }, mode);
        }

        /// <summary>
        /// Add a single <see cref="RecordBatch"/> to the <see cref="Table"/> with options.
        /// </summary>
        /// <param name="data">The data to insert into the table.</param>
        /// <param name="options">Options controlling the write mode and bad vector handling.</param>
        /// <returns>
        /// An <see cref="AddResult"/> containing the commit version of the operation.
        /// </returns>
        public Task<AddResult> Add(RecordBatch data, AddOptions options)
        {
            return Add(new[] { data }, options);
        }

        /// <summary>
        /// Retrieve the version of the table.
        /// </summary>
        /// <remarks>
        /// LanceDB supports versioning. Every operation that modifies the table increases
        /// the version. As long as a version hasn't been deleted you can
        /// <see cref="Checkout(ulong)"/> that version to view the data at that point.
        /// In addition, you can <see cref="Restore(ulong)"/> to replace the current table
        /// with a previous version.
        /// </remarks>
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
        /// Check whether the table uses V2 manifest paths.
        /// </summary>
        /// <remarks>
        /// See <see cref="MigrateManifestPathsV2"/> to migrate to V2 manifest paths.
        /// </remarks>
        /// <returns><c>true</c> if the table uses V2 manifest paths; otherwise, <c>false</c>.</returns>
        public async Task<bool> UsesV2ManifestPaths()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_uses_v2_manifest_paths(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            return result.ToInt64() != 0;
        }

        /// <summary>
        /// Migrate the table to use the new V2 manifest path scheme.
        /// </summary>
        /// <remarks>
        /// This renames all V1 manifests to V2 manifest paths. It is safe to run
        /// this migration multiple times. Use <see cref="UsesV2ManifestPaths"/> to
        /// check if the table is already using V2 manifest paths.
        /// </remarks>
        public async Task MigrateManifestPathsV2()
        {
            await NativeCall.Async(completion =>
            {
                table_migrate_manifest_paths_v2(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Replace the metadata of a field in the table schema.
        /// </summary>
        /// <param name="fieldName">The name of the field to replace the metadata for.</param>
        /// <param name="metadata">The new metadata key-value pairs to set on the field.</param>
        public async Task ReplaceFieldMetadata(string fieldName, Dictionary<string, string> metadata)
        {
            byte[] fieldBytes = NativeCall.ToUtf8(fieldName);
            byte[] metaBytes = NativeCall.ToUtf8(JsonSerializer.Serialize(metadata));
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pField = fieldBytes)
                    fixed (byte* pMeta = metaBytes)
                    {
                        table_replace_field_metadata(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pField, (IntPtr)pMeta,
                            completion);
                    }
                }
            }).ConfigureAwait(false);
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
        /// Checks out a specific version of the table by tag name.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Any read operation on the table will now access the data at the version
        /// referenced by the tag. As a consequence, calling this method will disable
        /// any read consistency interval that was previously set.
        /// </para>
        /// <para>
        /// This is a read-only operation that turns the table into a sort of "view"
        /// or "detached head". Other table instances will not be affected. To make the
        /// change permanent you can use the <see cref="Restore()"/> method.
        /// </para>
        /// <para>
        /// Any operation that modifies the table will fail while the table is in a checked
        /// out state.
        /// </para>
        /// <para>
        /// To return the table to a normal state use <see cref="CheckoutLatest"/>.
        /// </para>
        /// </remarks>
        /// <param name="tag">The tag name to check out.</param>
        public async Task Checkout(string tag)
        {
            byte[] tagBytes = NativeCall.ToUtf8(tag);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = tagBytes)
                    {
                        table_checkout_tag(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
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
        /// Restore the table to a specific version.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a convenience method that checks out the specified version and
        /// then restores it, making it the latest version.
        /// </para>
        /// <para>
        /// This operation will overwrite the latest version of the table with the
        /// specified version. Any changes made since that version will no longer
        /// be visible.
        /// </para>
        /// <para>
        /// Once the operation concludes the table will no longer be in a checked
        /// out state and the read consistency interval, if any, will apply.
        /// </para>
        /// </remarks>
        /// <param name="version">The version number to restore to.</param>
        public async Task Restore(ulong version)
        {
            await Checkout(version).ConfigureAwait(false);
            await Restore().ConfigureAwait(false);
        }

        /// <summary>
        /// Restore the table to the version referenced by a tag.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is a convenience method that checks out the tagged version and
        /// then restores it, making it the latest version.
        /// </para>
        /// <para>
        /// This operation will overwrite the latest version of the table with the
        /// tagged version. Any changes made since that version will no longer
        /// be visible.
        /// </para>
        /// <para>
        /// Once the operation concludes the table will no longer be in a checked
        /// out state and the read consistency interval, if any, will apply.
        /// </para>
        /// </remarks>
        /// <param name="tag">The tag name of the version to restore to.</param>
        public async Task Restore(string tag)
        {
            await Checkout(tag).ConfigureAwait(false);
            await Restore().ConfigureAwait(false);
        }

        /// <summary>
        /// Get the table's storage URI (location).
        /// </summary>
        /// <returns>The full storage location of the table (e.g., S3/GCS path or local path).</returns>
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
        /// <param name="name">
        /// An optional custom name for the index. If <c>null</c>, the name is auto-generated.
        /// </param>
        /// <param name="train">
        /// Whether to train the index with existing data. Default is <c>true</c>.
        /// When <c>false</c>, an empty index is created that will be populated later
        /// by the optimize step.
        /// </param>
        public async Task CreateIndex(
            IReadOnlyList<string> columns, Index index,
            bool replace = true, string? name = null, bool train = true)
        {
            string columnsJson = JsonSerializer.Serialize(columns);
            byte[] columnsBytes = NativeCall.ToUtf8(columnsJson);
            int indexType = (int)index.IndexType;
            byte[] configBytes = NativeCall.ToUtf8(index.ToConfigJson());
            byte[]? nameBytes = name != null ? NativeCall.ToUtf8(name) : null;

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pColumns = columnsBytes)
                    fixed (byte* pConfig = configBytes)
                    fixed (byte* pName = nameBytes)
                    {
                        table_create_index(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pColumns, indexType, (IntPtr)pConfig,
                            replace, (IntPtr)pName, train, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// List all indices that have been created on this table.
        /// </summary>
        /// <returns>A list of <see cref="IndexConfig"/> describing each index.</returns>
        public async Task<IReadOnlyList<IndexConfig>> ListIndices()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                table_list_indices(_handle!.DangerousGetHandle(), completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<List<IndexConfig>>(json)
                ?? new List<IndexConfig>();
        }

        /// <summary>
        /// Drop an index from the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This does not delete the index from disk, it just removes it from the table.
        /// To delete the index, run <see cref="Optimize"/> after dropping the index.
        /// </para>
        /// <para>
        /// Use <see cref="ListIndices"/> to find the names of the indices.
        /// </para>
        /// </remarks>
        /// <param name="name">The name of the index to drop.</param>
        public async Task DropIndex(string name)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        table_drop_index(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Prewarm an index in the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This will load the index into memory. This may reduce cold-start time for
        /// future queries.
        /// </para>
        /// <para>
        /// It is generally wasteful to call this if the index does not fit into the
        /// available cache.
        /// </para>
        /// <para>
        /// This function is not yet supported on all index types, in which case it
        /// may do nothing.
        /// </para>
        /// <para>
        /// Use <see cref="ListIndices"/> to find the names of the indices.
        /// </para>
        /// </remarks>
        /// <param name="name">The name of the index to prewarm.</param>
        public async Task PrewarmIndex(string name)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        table_prewarm_index(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Wait for indexing to complete for the given index names.
        /// </summary>
        /// <remarks>
        /// This will poll the table until all the indices are fully indexed,
        /// or throw if the timeout is reached.
        /// </remarks>
        /// <param name="indexNames">The names of the indices to wait for.</param>
        /// <param name="timeout">
        /// Maximum time to wait for indexing to complete. Defaults to 5 minutes
        /// if not specified.
        /// </param>
        /// <exception cref="LanceDbException">
        /// Thrown if the indices are not fully indexed within the timeout.
        /// </exception>
        public async Task WaitForIndex(
            IEnumerable<string> indexNames, TimeSpan? timeout = null)
        {
            long timeoutMs = timeout.HasValue
                ? (long)timeout.Value.TotalMilliseconds
                : -1;
            byte[] namesJson = NativeCall.ToUtf8(
                JsonSerializer.Serialize(indexNames));

            await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = namesJson)
                    {
                        table_wait_for_index(
                            _handle!.DangerousGetHandle(), (IntPtr)p, timeoutMs,
                            completion);
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieve statistics about an index.
        /// </summary>
        /// <param name="indexName">The name of the index to retrieve statistics for.</param>
        /// <returns>
        /// An <see cref="IndexStatistics"/> object, or <c>null</c> if the index does not exist.
        /// </returns>
        public async Task<IndexStatistics?> IndexStats(string indexName)
        {
            byte[] nameBytes = NativeCall.ToUtf8(indexName);
            IntPtr result = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        table_index_stats(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);

            if (result == IntPtr.Zero)
            {
                return null;
            }

            try
            {
                var ffi = Marshal.PtrToStructure<FfiIndexStats>(result);
                return new IndexStatistics(ffi);
            }
            finally
            {
                table_index_stats_free(result);
            }
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
        /// <returns>
        /// An <see cref="AddColumnsResult"/> containing the commit version of the operation.
        /// </returns>
        public async Task<AddColumnsResult> AddColumns(Dictionary<string, string> transforms)
        {
            var pairs = transforms.Select(kv => new[] { kv.Key, kv.Value }).ToArray();
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(pairs));

            IntPtr resultPtr = await NativeCall.Async(completion =>
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

            return new AddColumnsResult { Version = (ulong)resultPtr.ToInt64() };
        }

        /// <summary>
        /// Alter column names and nullability.
        /// </summary>
        /// <remarks>
        /// Each alteration specifies a column path and optional changes:
        /// <list type="bullet">
        /// <item><description><c>path</c>: The column name to alter. For nested columns, use dot-separated paths.</description></item>
        /// <item><description><c>rename</c>: The new name of the column.</description></item>
        /// <item><description><c>nullable</c>: Whether the column should be nullable. Only non-nullable columns
        ///   can be changed to nullable.</description></item>
        /// </list>
        /// </remarks>
        /// <param name="alterations">
        /// A list of alterations, each as a dictionary with keys: <c>"path"</c> (required),
        /// <c>"rename"</c> (optional), <c>"nullable"</c> (optional).
        /// </param>
        /// <returns>
        /// An <see cref="AlterColumnsResult"/> containing the commit version of the operation.
        /// </returns>
        public async Task<AlterColumnsResult> AlterColumns(IReadOnlyList<Dictionary<string, object>> alterations)
        {
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(alterations));

            IntPtr resultPtr = await NativeCall.Async(completion =>
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

            return new AlterColumnsResult { Version = (ulong)resultPtr.ToInt64() };
        }

        /// <summary>
        /// Drop columns from the table.
        /// </summary>
        /// <param name="columns">The names of the columns to drop.</param>
        /// <returns>
        /// A <see cref="DropColumnsResult"/> containing the commit version of the operation.
        /// </returns>
        public async Task<DropColumnsResult> DropColumns(IReadOnlyList<string> columns)
        {
            byte[] utf8Json = NativeCall.ToUtf8(JsonSerializer.Serialize(columns));

            IntPtr resultPtr = await NativeCall.Async(completion =>
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

            return new DropColumnsResult { Version = (ulong)resultPtr.ToInt64() };
        }

        /// <summary>
        /// Optimize the on-disk data and indices for better performance.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Modeled after <c>VACUUM</c> in PostgreSQL. Optimization covers three operations:
        /// </para>
        /// <list type="bullet">
        /// <item><description>Compaction: Merges small files into larger ones.</description></item>
        /// <item><description>Prune: Removes old versions of the dataset.</description></item>
        /// <item><description>Index: Optimizes the indices, adding new data to existing indices.</description></item>
        /// </list>
        /// <para>
        /// The frequency an application should call optimize is based on the frequency of
        /// data modifications. If data is frequently added, deleted, or updated then
        /// optimize should be run frequently. A good rule of thumb is to run optimize if
        /// you have added or modified 100,000 or more records or run more than 20 data
        /// modification operations.
        /// </para>
        /// </remarks>
        /// <param name="cleanupOlderThan">
        /// If specified, prune versions older than this duration. If <c>null</c>, old
        /// versions are pruned with default settings. Once a version is pruned it
        /// can no longer be checked out.
        /// </param>
        /// <param name="deleteUnverified">
        /// If <c>true</c>, delete files that are not verified (files newer than 7 days
        /// that may be part of an in-progress transaction). Only set this to <c>true</c>
        /// if you are sure there are no in-progress transactions.
        /// </param>
        /// <returns>Statistics about the optimization operation.</returns>
        public async Task<OptimizeStats> Optimize(
            TimeSpan? cleanupOlderThan = null, bool deleteUnverified = false)
        {
            long cleanupMs = cleanupOlderThan.HasValue
                ? (long)cleanupOlderThan.Value.TotalMilliseconds
                : -1;

            IntPtr result = await NativeCall.Async(completion =>
            {
                table_optimize(
                    _handle!.DangerousGetHandle(), cleanupMs, deleteUnverified, completion);
            }).ConfigureAwait(false);
            string json = NativeCall.ReadStringAndFree(result);
            return JsonSerializer.Deserialize<OptimizeStats>(json) ?? new OptimizeStats();
        }

        /// <summary>
        /// Create a builder for a merge insert (upsert) operation.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This operation can add rows, update rows, or remove rows in a single operation
        /// based on a column to join on. This is a useful operation when you have a stream
        /// of new data and want to update the table based on new and changed records while
        /// keeping the old unchanged records.
        /// </para>
        /// <para>
        /// Use the returned <see cref="MergeInsertBuilder"/> to configure how to handle:
        /// </para>
        /// <list type="bullet">
        /// <item><description>Matched rows (exist in both old and new data) — <see cref="MergeInsertBuilder.WhenMatchedUpdateAll"/>.</description></item>
        /// <item><description>Not-matched rows (exist only in new data) — <see cref="MergeInsertBuilder.WhenNotMatchedInsertAll"/>.</description></item>
        /// <item><description>Not-matched-by-source rows (exist only in old data) — <see cref="MergeInsertBuilder.WhenNotMatchedBySourceDelete"/>.</description></item>
        /// </list>
        /// <para>
        /// Then call <see cref="MergeInsertBuilder.Execute(IReadOnlyList{RecordBatch})"/> with the new data.
        /// </para>
        /// </remarks>
        /// <param name="on">
        /// The column name to join on. Rows are considered matching if they have the
        /// same value for this column.
        /// </param>
        /// <returns>A <see cref="MergeInsertBuilder"/> to configure and execute the operation.</returns>
        public MergeInsertBuilder MergeInsert(string on)
        {
            return new MergeInsertBuilder(this, new[] { on });
        }

        /// <summary>
        /// Create a builder for a merge insert (upsert) operation with multiple join columns.
        /// </summary>
        /// <param name="on">
        /// The column names to join on. Rows are considered matching if they have the
        /// same values for all of these columns.
        /// </param>
        /// <returns>A <see cref="MergeInsertBuilder"/> to configure and execute the operation.</returns>
        public MergeInsertBuilder MergeInsert(IReadOnlyList<string> on)
        {
            return new MergeInsertBuilder(this, on);
        }

        internal async Task<MergeResult> ExecuteMergeInsert(
            IReadOnlyList<string> onColumns,
            bool whenMatchedUpdateAll, string? whenMatchedUpdateAllFilter,
            bool whenNotMatchedInsertAll,
            bool whenNotMatchedBySourceDelete, string? whenNotMatchedBySourceDeleteFilter,
            IReadOnlyList<RecordBatch> data,
            bool useIndex = true, TimeSpan? timeout = null)
        {
            string onColumnsJson = JsonSerializer.Serialize(onColumns);
            byte[] onColumnsBytes = NativeCall.ToUtf8(onColumnsJson);
            byte[]? matchedFilterBytes = whenMatchedUpdateAllFilter != null
                ? NativeCall.ToUtf8(whenMatchedUpdateAllFilter) : null;
            byte[]? sourceDeleteFilterBytes = whenNotMatchedBySourceDeleteFilter != null
                ? NativeCall.ToUtf8(whenNotMatchedBySourceDeleteFilter) : null;
            long timeoutMs = timeout.HasValue ? (long)timeout.Value.TotalMilliseconds : -1;

            IntPtr resultPtr = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* pOnColumns = onColumnsBytes)
                    fixed (byte* pMatchedFilter = matchedFilterBytes)
                    fixed (byte* pSourceDeleteFilter = sourceDeleteFilterBytes)
                    {
                        var cArrays = new CArrowArray[data.Count];
                        var cSchemaArr = new CArrowSchema[1];
                        fixed (CArrowSchema* pSchema = cSchemaArr)
                        {
                            CArrowSchemaExporter.ExportSchema(data[0].Schema, pSchema);
                            for (int i = 0; i < data.Count; i++)
                            {
                                cArrays[i] = default;
                                var clone = ArrowExportHelper.CloneBatchForExport(data[i]);
                                fixed (CArrowArray* pArr = &cArrays[i])
                                {
                                    CArrowArrayExporter.ExportRecordBatch(clone, pArr);
                                }
                            }
                            fixed (CArrowArray* pArrays = cArrays)
                            {
                                table_merge_insert(
                                    _handle!.DangerousGetHandle(),
                                    (IntPtr)pOnColumns,
                                    whenMatchedUpdateAll, (IntPtr)pMatchedFilter,
                                    whenNotMatchedInsertAll,
                                    whenNotMatchedBySourceDelete, (IntPtr)pSourceDeleteFilter,
                                    pArrays, pSchema, (nuint)data.Count,
                                    useIndex, timeoutMs,
                                    completion);
                            }
                        }
                    }
                }
            }).ConfigureAwait(false);

            try
            {
                return Marshal.PtrToStructure<MergeResult>(resultPtr);
            }
            finally
            {
                table_merge_result_free(resultPtr);
            }
        }

        /// <summary>
        /// Take rows at the given offset positions from the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Offsets are 0-indexed and relative to the current version of the table. Offsets
        /// are not stable. A row with an offset of N may have a different offset in a
        /// different version of the table (e.g., if an earlier row is deleted).
        /// </para>
        /// <para>
        /// Offsets are mostly useful for sampling as the set of all valid offsets is easily
        /// known in advance to be [0, <see cref="CountRows"/>).
        /// </para>
        /// </remarks>
        /// <param name="offsets">The offset positions of the rows to retrieve.</param>
        /// <param name="columns">
        /// Optional list of column names to return. If <c>null</c>, all columns are returned.
        /// </param>
        /// <returns>A <see cref="RecordBatch"/> containing the requested rows.</returns>
        public async Task<RecordBatch> TakeOffsets(
            IReadOnlyList<ulong> offsets, IReadOnlyList<string>? columns = null)
        {
            ulong[] offsetArray = offsets is ulong[] arr ? arr : offsets.ToArray();
            byte[]? columnsBytes = columns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(columns)) : null;

            IntPtr ffiCDataPtr = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (ulong* pOffsets = offsetArray)
                    fixed (byte* pColumns = columnsBytes)
                    {
                        table_take_offsets(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pOffsets, (nuint)offsetArray.Length,
                            (IntPtr)pColumns, completion);
                    }
                }
            }).ConfigureAwait(false);

            try
            {
                return ReadRecordBatchFromCData(ffiCDataPtr);
            }
            finally
            {
                NativeCall.free_ffi_cdata(ffiCDataPtr);
            }
        }

        /// <summary>
        /// Take rows with the given row IDs from the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Row IDs are not stable and are relative to the current version of the table.
        /// They can change due to compaction and updates.
        /// </para>
        /// <para>
        /// Unlike offsets, row IDs are not 0-indexed and no assumptions should be made
        /// about the possible range of row IDs. In order to use this method you must
        /// first obtain the row IDs by scanning or searching the table using
        /// <see cref="QueryBase{T}.WithRowId"/>.
        /// </para>
        /// <para>
        /// Even so, row IDs are more stable than offsets and can be useful in some situations.
        /// </para>
        /// </remarks>
        /// <param name="rowIds">The row IDs of the rows to retrieve.</param>
        /// <param name="columns">
        /// Optional list of column names to return. If <c>null</c>, all columns are returned.
        /// </param>
        /// <returns>A <see cref="RecordBatch"/> containing the requested rows.</returns>
        public async Task<RecordBatch> TakeRowIds(
            IReadOnlyList<ulong> rowIds, IReadOnlyList<string>? columns = null)
        {
            ulong[] idArray = rowIds is ulong[] arr ? arr : rowIds.ToArray();
            byte[]? columnsBytes = columns != null
                ? NativeCall.ToUtf8(JsonSerializer.Serialize(columns)) : null;

            IntPtr ffiCDataPtr = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (ulong* pRowIds = idArray)
                    fixed (byte* pColumns = columnsBytes)
                    {
                        table_take_row_ids(
                            _handle!.DangerousGetHandle(),
                            (IntPtr)pRowIds, (nuint)idArray.Length,
                            (IntPtr)pColumns, completion);
                    }
                }
            }).ConfigureAwait(false);

            try
            {
                return ReadRecordBatchFromCData(ffiCDataPtr);
            }
            finally
            {
                NativeCall.free_ffi_cdata(ffiCDataPtr);
            }
        }

        private static unsafe RecordBatch ReadRecordBatchFromCData(IntPtr ffiCDataPtr)
        {
            var arrayPtr = Marshal.ReadIntPtr(ffiCDataPtr);
            var schemaPtr = Marshal.ReadIntPtr(ffiCDataPtr + IntPtr.Size);

            var schema = CArrowSchemaImporter.ImportSchema(
                (CArrowSchema*)schemaPtr);

            return CArrowArrayImporter.ImportRecordBatch(
                (CArrowArray*)arrayPtr, schema);
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

        /// <summary>
        /// Get the version number that a tag points to.
        /// </summary>
        /// <param name="tag">The name of the tag.</param>
        /// <returns>The version number associated with the tag.</returns>
        /// <exception cref="LanceDbException">Thrown if the tag does not exist.</exception>
        public async Task<ulong> GetTagVersion(string tag)
        {
            byte[] utf8Tag = NativeCall.ToUtf8(tag);
            IntPtr result = await NativeCall.Async(completion =>
            {
                unsafe
                {
                    fixed (byte* p = utf8Tag)
                    {
                        table_tags_get_version(
                            _handle!.DangerousGetHandle(), (IntPtr)p, completion);
                    }
                }
            }).ConfigureAwait(false);
            return (ulong)result.ToInt64();
        }
    }
}