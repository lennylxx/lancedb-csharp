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
    }
}