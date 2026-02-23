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
        /// Calls the native full_text_search FFI function.
        /// </summary>
        protected abstract IntPtr NativeFullTextSearch(IntPtr ptr, IntPtr queryText);

        /// <summary>
        /// Calls the native fast_search FFI function.
        /// </summary>
        protected abstract IntPtr NativeFastSearch(IntPtr ptr);

        /// <summary>
        /// Calls the native postfilter FFI function.
        /// </summary>
        protected abstract IntPtr NativePostfilter(IntPtr ptr);

        /// <summary>
        /// Calls the native execute FFI function using Arrow C Data Interface.
        /// </summary>
        private protected abstract void NativeExecute(
            IntPtr ptr, long timeoutMs, uint maxBatchLength, NativeCall.FfiCallback callback);

        /// <summary>
        /// Calls the native explain_plan FFI function.
        /// </summary>
        private protected abstract void NativeExplainPlan(
            IntPtr ptr, bool verbose, NativeCall.FfiCallback callback);

        /// <summary>
        /// Calls the native analyze_plan FFI function.
        /// </summary>
        private protected abstract void NativeAnalyzePlan(
            IntPtr ptr, NativeCall.FfiCallback callback);

        /// <summary>
        /// Calls the native output_schema FFI function.
        /// </summary>
        private protected abstract void NativeOutputSchema(
            IntPtr ptr, NativeCall.FfiCallback callback);

        /// <summary>
        /// Replaces the current native pointer with a new one, freeing the old one.
        /// Throws LanceDbException if the new pointer is null due to an FFI error.
        /// </summary>
        protected void ReplacePtr(IntPtr newPtr)
        {
            NativeCall.ThrowIfNullWithError(newPtr, "Native FFI call returned null");
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
        /// <param name="columns">
        /// A list of column names to return.
        /// </param>
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
        /// that computes the column value. This is equivalent to the Python
        /// <c>select({"new_col": "old_col + 1"})</c> pattern.
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
        ///
        /// By default, filtering is applied before the search (prefiltering).
        /// Use <see cref="Postfilter"/> to apply the filter after the search instead.
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
        /// Perform a full-text search on the table.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The results will be returned in order of relevance (BM25 scores).
        /// </para>
        /// <para>
        /// This method is only valid on tables that have a full-text search index.
        /// Use <see cref="Table.CreateIndex"/> with <see cref="FtsIndex"/> to create one.
        /// </para>
        /// <para>
        /// Full-text search always has a limit. If <see cref="Limit"/> has not
        /// been called then a default limit of 10 will be used.
        /// </para>
        /// <para>
        /// When called on a <see cref="VectorQuery"/>, this creates a hybrid query
        /// that combines vector search with full-text search.
        /// </para>
        /// </remarks>
        /// <param name="query">The search query string.</param>
        /// <returns>This query instance for method chaining.</returns>
        public T FullTextSearch(string query)
        {
            byte[] textBytes = NativeCall.ToUtf8(query);
            unsafe
            {
                fixed (byte* p = textBytes)
                {
                    IntPtr newPtr = NativeFullTextSearch(_ptr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return (T)this;
        }

        /// <summary>
        /// Skip searching unindexed data.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When this is called, any data that has not been indexed will be skipped
        /// during the search. This can make queries faster but results may be incomplete
        /// if the index is not up to date.
        /// </para>
        /// <para>
        /// This is primarily useful for full-text search queries where you want to
        /// skip unindexed fragments for better performance.
        /// </para>
        /// </remarks>
        /// <returns>This query instance for method chaining.</returns>
        public T FastSearch()
        {
            IntPtr newPtr = NativeFastSearch(_ptr);
            ReplacePtr(newPtr);
            return (T)this;
        }

        /// <summary>
        /// Apply filtering after the search instead of before.
        /// </summary>
        /// <remarks>
        /// <para>
        /// By default, filters are applied before the search (prefiltering).
        /// This can sometimes reduce the number of results below the requested limit.
        /// </para>
        /// <para>
        /// Postfiltering applies the filter after the search, which guarantees
        /// the requested number of results but may be slower and less accurate.
        /// </para>
        /// </remarks>
        /// <returns>This query instance for method chaining.</returns>
        public T Postfilter()
        {
            IntPtr newPtr = NativePostfilter(_ptr);
            ReplacePtr(newPtr);
            return (T)this;
        }

        /// <summary>
        /// Execute the query and return the results as an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        /// <param name="timeout">
        /// Optional maximum time for the query to run. If <c>null</c>, no timeout is applied.
        /// </param>
        /// <param name="maxBatchLength">
        /// Optional maximum number of rows per batch. If <c>null</c>, uses the default (1024).
        /// </param>
        /// <returns>The query results as a RecordBatch.</returns>
        public async Task<RecordBatch> ToArrow(
            TimeSpan? timeout = null, int? maxBatchLength = null)
        {
            long timeoutMs = timeout.HasValue ? (long)timeout.Value.TotalMilliseconds : -1;
            uint batchLen = maxBatchLength.HasValue ? (uint)maxBatchLength.Value : 0;

            IntPtr ffiCDataPtr = await NativeCall.Async(completion =>
            {
                NativeExecute(_ptr, timeoutMs, batchLen, completion);
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
        /// Execute the query and return the results as a list of dictionaries.
        /// </summary>
        /// <remarks>
        /// Each dictionary maps column names to their values. This is a convenience
        /// method that calls <see cref="ToArrow"/> and converts the result.
        /// </remarks>
        /// <param name="timeout">
        /// Optional maximum time for the query to run. If <c>null</c>, no timeout is applied.
        /// </param>
        /// <param name="maxBatchLength">
        /// Optional maximum number of rows per batch. If <c>null</c>, uses the default (1024).
        /// </param>
        /// <returns>A list of dictionaries, one per row.</returns>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ToList(
            TimeSpan? timeout = null, int? maxBatchLength = null)
        {
            var batch = await ToArrow(timeout, maxBatchLength).ConfigureAwait(false);
            return RecordBatchToList(batch);
        }

        /// <summary>
        /// Return the query execution plan as a string.
        /// </summary>
        /// <remarks>
        /// This will not execute the query. It creates a string representation of the
        /// plan that will be used to execute the query. Useful for debugging query
        /// performance.
        /// </remarks>
        /// <param name="verbose">If <c>true</c>, includes additional details in the plan.</param>
        /// <returns>A string representation of the execution plan.</returns>
        public async Task<string> ExplainPlan(bool verbose = false)
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                NativeExplainPlan(_ptr, verbose, completion);
            }).ConfigureAwait(false);
            return NativeCall.ReadStringAndFree(result);
        }

        /// <summary>
        /// Execute the query and return the plan with runtime metrics.
        /// </summary>
        /// <remarks>
        /// Shows the same plan as <see cref="ExplainPlan"/> but includes runtime metrics.
        /// The query is actually executed to collect the metrics.
        /// </remarks>
        /// <returns>A string representation of the execution plan with runtime metrics.</returns>
        public async Task<string> AnalyzePlan()
        {
            IntPtr result = await NativeCall.Async(completion =>
            {
                NativeAnalyzePlan(_ptr, completion);
            }).ConfigureAwait(false);
            return NativeCall.ReadStringAndFree(result);
        }

        /// <summary>
        /// Return the output schema for the query without executing it.
        /// </summary>
        /// <remarks>
        /// This can be useful when the selection for a query is built dynamically
        /// as it is not always obvious what the output schema will be.
        /// </remarks>
        /// <returns>The output <see cref="Schema"/>.</returns>
        public async Task<Schema> OutputSchema()
        {
            IntPtr ffiSchemaPtr = await NativeCall.Async(completion =>
            {
                NativeOutputSchema(_ptr, completion);
            }).ConfigureAwait(false);

            try
            {
                return ReadSchemaFromCData(ffiSchemaPtr);
            }
            finally
            {
                NativeCall.free_ffi_schema(ffiSchemaPtr);
            }
        }

        private static unsafe Schema ReadSchemaFromCData(IntPtr ffiSchemaPtr)
        {
            return CArrowSchemaImporter.ImportSchema(
                (CArrowSchema*)ffiSchemaPtr);
        }

        /// <summary>
        /// Imports a RecordBatch from an FfiCData pointer using the Arrow C Data Interface.
        /// The FfiCData struct contains pointers to FFI_ArrowArray and FFI_ArrowSchema.
        /// </summary>
        private static unsafe RecordBatch ReadRecordBatchFromCData(IntPtr ffiCDataPtr)
        {
            // FfiCData layout: { array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema }
            var arrayPtr = Marshal.ReadIntPtr(ffiCDataPtr);
            var schemaPtr = Marshal.ReadIntPtr(ffiCDataPtr + IntPtr.Size);

            var schema = CArrowSchemaImporter.ImportSchema(
                (CArrowSchema*)schemaPtr);

            var recordBatch = CArrowArrayImporter.ImportRecordBatch(
                (CArrowArray*)arrayPtr, schema);

            return recordBatch;
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