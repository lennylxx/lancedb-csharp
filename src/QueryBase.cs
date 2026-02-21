namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Text.Json;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Ipc;

    /// <summary>
    /// Base class for LanceDB query builders (scan and vector).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is not intended to be created directly. Instead, use the
    /// <see cref="Table.Query"/> method to create a query.
    /// </para>
    /// <para>
    /// Implements <see cref="IDisposable"/> to release the underlying native query handle.
    /// All builder methods return the concrete type <typeparamref name="T"/> for fluent
    /// method chaining.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The concrete query type for fluent method chaining.</typeparam>
    public abstract class QueryBase<T> : IDisposable where T : QueryBase<T>
    {
        private IntPtr _ptr;
        private bool _disposed;

        /// <summary>
        /// Gets the native pointer for FFI calls.
        /// </summary>
        protected IntPtr NativePtr => _ptr;

        internal QueryBase(IntPtr ptr)
        {
            _ptr = ptr;
        }

        /// <summary>
        /// Frees the native pointer using the type-specific free function.
        /// </summary>
        protected abstract void NativeFree(IntPtr ptr);

        /// <summary>
        /// Calls the native select FFI function.
        /// </summary>
        protected abstract IntPtr NativeSelect(IntPtr ptr, IntPtr columnsJson);

        /// <summary>
        /// Calls the native where/only_if FFI function.
        /// </summary>
        protected abstract IntPtr NativeOnlyIf(IntPtr ptr, IntPtr predicate);

        /// <summary>
        /// Calls the native limit FFI function.
        /// </summary>
        protected abstract IntPtr NativeLimit(IntPtr ptr, ulong limit);

        /// <summary>
        /// Calls the native offset FFI function.
        /// </summary>
        protected abstract IntPtr NativeOffset(IntPtr ptr, ulong offset);

        /// <summary>
        /// Calls the native with_row_id FFI function.
        /// </summary>
        protected abstract IntPtr NativeWithRowId(IntPtr ptr);

        /// <summary>
        /// Calls the native execute FFI function.
        /// </summary>
        private protected abstract void NativeExecute(IntPtr ptr, NativeCall.FfiCallback callback);

        /// <summary>
        /// Replaces the current native pointer with a new one, freeing the old one.
        /// </summary>
        protected void ReplacePtr(IntPtr newPtr)
        {
            IntPtr old = _ptr;
            _ptr = newPtr;
            if (old != IntPtr.Zero && old != newPtr)
            {
                NativeFree(old);
            }
        }

        /// <summary>
        /// Set the columns to return.
        /// </summary>
        /// <remarks>
        /// If this is not called then every column will be returned.
        /// </remarks>
        /// <param name="columns">The column names to return.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T Select(IReadOnlyList<string> columns)
        {
            string json = JsonSerializer.Serialize(columns);
            byte[] jsonBytes = NativeCall.ToUtf8(json);
            unsafe
            {
                fixed (byte* p = jsonBytes)
                {
                    IntPtr newPtr = NativeSelect(_ptr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return (T)this;
        }

        /// <summary>
        /// Set the columns to return, with SQL expression transformations.
        /// </summary>
        /// <remarks>
        /// Each key is the output column name, and each value is a SQL expression
        /// that computes the column value.
        /// </remarks>
        /// <param name="columns">A dictionary mapping output column names to SQL expressions.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T Select(Dictionary<string, string> columns)
        {
            string json = JsonSerializer.Serialize(columns);
            byte[] jsonBytes = NativeCall.ToUtf8(json);
            unsafe
            {
                fixed (byte* p = jsonBytes)
                {
                    IntPtr newPtr = NativeSelect(_ptr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return (T)this;
        }

        /// <summary>
        /// Only return rows which match the given filter.
        /// </summary>
        /// <remarks>
        /// The filter should be supplied as a SQL filter expression. For example
        /// <c>"x &gt; 10"</c>, <c>"y &gt; 0 AND y &lt; 100"</c>, <c>"x IS NOT NULL"</c>.
        /// </remarks>
        /// <param name="predicate">A SQL filter expression.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T Where(string predicate)
        {
            byte[] predicateBytes = NativeCall.ToUtf8(predicate);
            unsafe
            {
                fixed (byte* p = predicateBytes)
                {
                    IntPtr newPtr = NativeOnlyIf(_ptr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return (T)this;
        }

        /// <summary>
        /// Set the maximum number of results to return.
        /// </summary>
        /// <remarks>
        /// By default, a plain query has no limit. If this query is a vector query,
        /// the default limit is 10.
        /// </remarks>
        /// <param name="limit">The maximum number of results to return.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T Limit(int limit)
        {
            IntPtr newPtr = NativeLimit(_ptr, (ulong)limit);
            ReplacePtr(newPtr);
            return (T)this;
        }

        /// <summary>
        /// Set the offset for the results.
        /// </summary>
        /// <remarks>
        /// This is useful for pagination. Note that large offsets can be slow.
        /// </remarks>
        /// <param name="offset">The number of results to skip.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T Offset(int offset)
        {
            IntPtr newPtr = NativeOffset(_ptr, (ulong)offset);
            ReplacePtr(newPtr);
            return (T)this;
        }

        /// <summary>
        /// Return the internal row ID in the results.
        /// </summary>
        /// <returns>This query instance for method chaining.</returns>
        public T WithRowId()
        {
            IntPtr newPtr = NativeWithRowId(_ptr);
            ReplacePtr(newPtr);
            return (T)this;
        }

        /// <summary>
        /// Execute the query and return the results as an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        /// <returns>The query results as a RecordBatch.</returns>
        public async Task<RecordBatch> ToArrow()
        {
            IntPtr ffiBytesPtr = await NativeCall.Async(completion =>
            {
                NativeExecute(_ptr, completion);
            }).ConfigureAwait(false);

            try
            {
                return ReadRecordBatchFromFfiBytes(ffiBytesPtr);
            }
            finally
            {
                NativeCall.free_ffi_bytes(ffiBytesPtr);
            }
        }

        /// <summary>
        /// Execute the query and return the results as a list of dictionaries.
        /// </summary>
        /// <remarks>
        /// Each dictionary maps column names to their values. This is a convenience
        /// method that calls <see cref="ToArrow"/> and converts the result.
        /// </remarks>
        /// <returns>A list of dictionaries, one per row.</returns>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ToList()
        {
            var batch = await ToArrow().ConfigureAwait(false);
            return RecordBatchToList(batch);
        }

        /// <summary>
        /// Reads all RecordBatches from an FfiBytes pointer containing Arrow IPC data
        /// and concatenates them into a single RecordBatch.
        /// </summary>
        private static unsafe RecordBatch ReadRecordBatchFromFfiBytes(IntPtr ffiBytesPtr)
        {
            var dataPtr = Marshal.ReadIntPtr(ffiBytesPtr);
            var len = Marshal.ReadIntPtr(ffiBytesPtr + IntPtr.Size).ToInt64();

            byte[] managedBytes = new byte[len];
            Marshal.Copy(dataPtr, managedBytes, 0, (int)len);

            using var stream = new System.IO.MemoryStream(managedBytes);
            using var reader = new ArrowFileReader(stream);

            var batches = new System.Collections.Generic.List<RecordBatch>();
            RecordBatch? batch;
            while ((batch = reader.ReadNextRecordBatch()) != null)
            {
                batches.Add(batch);
            }

            if (batches.Count == 0)
            {
                return new RecordBatch(reader.Schema, System.Array.Empty<IArrowArray>(), 0);
            }

            if (batches.Count == 1)
            {
                return batches[0];
            }

            return ConcatenateBatches(batches, reader.Schema);
        }

        /// <summary>
        /// Concatenates multiple RecordBatches into a single RecordBatch.
        /// </summary>
        private static RecordBatch ConcatenateBatches(
            System.Collections.Generic.List<RecordBatch> batches, Schema schema)
        {
            int totalLength = 0;
            foreach (var b in batches)
            {
                totalLength += b.Length;
            }

            var arrays = new IArrowArray[schema.FieldsList.Count];
            for (int col = 0; col < schema.FieldsList.Count; col++)
            {
                var columnArrays = new System.Collections.Generic.List<IArrowArray>(batches.Count);
                foreach (var b in batches)
                {
                    columnArrays.Add(b.Column(col));
                }

                arrays[col] = ArrowArrayConcatenator.Concatenate(columnArrays);
            }

            return new RecordBatch(schema, arrays, totalLength);
        }

        /// <summary>
        /// Converts a RecordBatch to a list of row dictionaries.
        /// </summary>
        private static IReadOnlyList<Dictionary<string, object?>> RecordBatchToList(RecordBatch batch)
        {
            var result = new List<Dictionary<string, object?>>(batch.Length);
            for (int row = 0; row < batch.Length; row++)
            {
                var dict = new Dictionary<string, object?>(batch.ColumnCount);
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    string name = batch.Schema.FieldsList[col].Name;
                    dict[name] = GetArrowValue(batch.Column(col), row);
                }

                result.Add(dict);
            }

            return result;
        }

        /// <summary>
        /// Extracts a single value from an Arrow array at the given index.
        /// </summary>
        private static object? GetArrowValue(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return null;
            }

            return array switch
            {
                Int8Array a => a.GetValue(index),
                Int16Array a => a.GetValue(index),
                Int32Array a => a.GetValue(index),
                Int64Array a => a.GetValue(index),
                UInt8Array a => a.GetValue(index),
                UInt16Array a => a.GetValue(index),
                UInt32Array a => a.GetValue(index),
                UInt64Array a => a.GetValue(index),
                FloatArray a => a.GetValue(index),
                DoubleArray a => a.GetValue(index),
                StringArray a => a.GetString(index),
                BooleanArray a => a.GetValue(index),
                Date32Array a => a.GetDateTimeOffset(index),
                Date64Array a => a.GetDateTimeOffset(index),
                TimestampArray a => a.GetTimestamp(index),
                BinaryArray a => a.GetBytes(index).ToArray(),
                _ => array.GetType().Name,
            };
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (!_disposed && _ptr != IntPtr.Zero)
            {
                NativeFree(_ptr);
                _ptr = IntPtr.Zero;
                _disposed = true;
            }

            GC.SuppressFinalize(this);
        }
    }
}