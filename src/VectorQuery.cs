namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// A builder for vector queries, created by <see cref="Query.NearestTo"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A vector query is a query that searches for the nearest vectors to a given
    /// query vector. It inherits all the builder methods from <see cref="QueryBase{T}"/>
    /// and adds vector-search-specific options.
    /// </para>
    /// </remarks>
    public class VectorQuery : QueryBase<VectorQuery>
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_free(IntPtr vq_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_select(IntPtr vq_ptr, IntPtr columns_json);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_only_if(IntPtr vq_ptr, IntPtr predicate);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_limit(IntPtr vq_ptr, ulong limit);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_offset(IntPtr vq_ptr, ulong offset);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_with_row_id(IntPtr vq_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_execute(
            IntPtr vq_ptr, long timeout_ms, uint max_batch_length,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_explain_plan(
            IntPtr vq_ptr, [MarshalAs(UnmanagedType.U1)] bool verbose,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_analyze_plan(
            IntPtr vq_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_output_schema(
            IntPtr vq_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_minimum_nprobes(IntPtr vq_ptr, uint n);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_maximum_nprobes(IntPtr vq_ptr, uint n);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_add_query_vector(
            IntPtr vq_ptr, float[] vector, UIntPtr len);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_column(IntPtr vq_ptr, IntPtr column_name);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_distance_type(IntPtr vq_ptr, IntPtr distance_type);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_nprobes(IntPtr vq_ptr, ulong nprobes);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_refine_factor(IntPtr vq_ptr, uint refine_factor);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_bypass_vector_index(IntPtr vq_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_postfilter(IntPtr vq_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_full_text_search(IntPtr vq_ptr, IntPtr query_text);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_fast_search(IntPtr vq_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_ef(IntPtr vq_ptr, ulong ef);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_distance_range(IntPtr vq_ptr, float lower, float upper);

        internal VectorQuery(IntPtr vectorQueryPtr)
            : base(vectorQueryPtr)
        {
        }

        /// <inheritdoc/>
        protected override void NativeFree(IntPtr ptr) => vector_query_free(ptr);

        /// <inheritdoc/>
        protected override IntPtr NativeSelect(IntPtr ptr, IntPtr columnsJson)
            => vector_query_select(ptr, columnsJson);

        /// <inheritdoc/>
        protected override IntPtr NativeOnlyIf(IntPtr ptr, IntPtr predicate)
            => vector_query_only_if(ptr, predicate);

        /// <inheritdoc/>
        protected override IntPtr NativeLimit(IntPtr ptr, ulong limit)
            => vector_query_limit(ptr, limit);

        /// <inheritdoc/>
        protected override IntPtr NativeOffset(IntPtr ptr, ulong offset)
            => vector_query_offset(ptr, offset);

        /// <inheritdoc/>
        protected override IntPtr NativeWithRowId(IntPtr ptr)
            => vector_query_with_row_id(ptr);

        /// <inheritdoc/>
        private protected override void NativeExecute(
            IntPtr ptr, long timeoutMs, uint maxBatchLength, NativeCall.FfiCallback callback)
            => vector_query_execute(ptr, timeoutMs, maxBatchLength, callback);

        /// <inheritdoc/>
        private protected override void NativeExplainPlan(
            IntPtr ptr, bool verbose, NativeCall.FfiCallback callback)
            => vector_query_explain_plan(ptr, verbose, callback);

        /// <inheritdoc/>
        private protected override void NativeAnalyzePlan(
            IntPtr ptr, NativeCall.FfiCallback callback)
            => vector_query_analyze_plan(ptr, callback);

        /// <inheritdoc/>
        private protected override void NativeOutputSchema(
            IntPtr ptr, NativeCall.FfiCallback callback)
            => vector_query_output_schema(ptr, callback);

        /// <inheritdoc/>
        protected override IntPtr NativeFullTextSearch(IntPtr ptr, IntPtr queryText)
            => vector_query_full_text_search(ptr, queryText);

        /// <inheritdoc/>
        protected override IntPtr NativeFastSearch(IntPtr ptr)
            => vector_query_fast_search(ptr);

        /// <inheritdoc/>
        protected override IntPtr NativePostfilter(IntPtr ptr)
            => vector_query_postfilter(ptr);

        /// <summary>
        /// Set the vector column to query.
        /// </summary>
        /// <remarks>
        /// This controls which column is compared to the query vector supplied in
        /// the call to <see cref="Query.NearestTo"/>.
        /// This parameter must be specified if the table has more than one column
        /// whose data type is a fixed-size-list of floats.
        /// </remarks>
        /// <param name="column">The name of the vector column.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery Column(string column)
        {
            byte[] columnBytes = NativeCall.ToUtf8(column);
            unsafe
            {
                fixed (byte* p = columnBytes)
                {
                    IntPtr newPtr = vector_query_column(NativePtr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return this;
        }

        /// <summary>
        /// Set the distance metric to use for the vector search.
        /// </summary>
        /// <remarks>
        /// Supported values are <c>"l2"</c>, <c>"cosine"</c>, <c>"dot"</c>, and <c>"hamming"</c>.
        /// If not specified, the distance type is inferred from the vector column's metadata
        /// or defaults to <c>"l2"</c>.
        /// </remarks>
        /// <param name="distanceType">The distance metric to use.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery DistanceType(string distanceType)
        {
            byte[] dtBytes = NativeCall.ToUtf8(distanceType);
            unsafe
            {
                fixed (byte* p = dtBytes)
                {
                    IntPtr newPtr = vector_query_distance_type(NativePtr, new IntPtr(p));
                    ReplacePtr(newPtr);
                }
            }

            return this;
        }

        /// <summary>
        /// Set the number of probes to use for an IVF index search.
        /// </summary>
        /// <remarks>
        /// Higher values will yield more accurate results but will be slower.
        /// </remarks>
        /// <param name="nprobes">The number of probes to use.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery Nprobes(int nprobes)
        {
            IntPtr newPtr = vector_query_nprobes(NativePtr, (ulong)nprobes);
            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Set a refine factor to use for vector search.
        /// </summary>
        /// <remarks>
        /// A refine step uses the original vector values to re-rank the results
        /// from the ANN search. This trades off extra computation for more accurate
        /// results.
        /// </remarks>
        /// <param name="refineFactor">The refine factor to use.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery RefineFactor(int refineFactor)
        {
            IntPtr newPtr = vector_query_refine_factor(NativePtr, (uint)refineFactor);
            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Skip the vector index and perform a brute-force search.
        /// </summary>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery BypassVectorIndex()
        {
            IntPtr newPtr = vector_query_bypass_vector_index(NativePtr);
            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Set the HNSW ef search parameter.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This controls the size of the dynamic candidate list during HNSW search.
        /// Higher values improve recall at the cost of search speed.
        /// </para>
        /// <para>
        /// This parameter is only used when the vector index is an HNSW-based index
        /// (HnswPq or HnswSq).
        /// </para>
        /// </remarks>
        /// <param name="ef">The ef search parameter.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery Ef(int ef)
        {
            IntPtr newPtr = vector_query_ef(NativePtr, (ulong)ef);
            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Filter results to only include vectors within a distance range.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The bounds are inclusive. Pass <c>null</c> for either bound to leave it open.
        /// The distance values depend on the distance metric being used (L2, cosine, dot).
        /// </para>
        /// </remarks>
        /// <param name="lowerBound">The minimum distance (inclusive), or <c>null</c> for no lower bound.</param>
        /// <param name="upperBound">The maximum distance (inclusive), or <c>null</c> for no upper bound.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery DistanceRange(float? lowerBound = null, float? upperBound = null)
        {
            IntPtr newPtr = vector_query_distance_range(
                NativePtr,
                lowerBound ?? float.NaN,
                upperBound ?? float.NaN);
            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Set the minimum number of probes for an IVF index search.
        /// </summary>
        /// <remarks>
        /// Ensures at least this many probes are used regardless of adaptive probe settings.
        /// </remarks>
        /// <param name="n">The minimum number of probes.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery MinimumNprobes(int n)
        {
            IntPtr newPtr = vector_query_minimum_nprobes(NativePtr, (uint)n);
            if (newPtr == IntPtr.Zero)
            {
                throw new LanceDbException("Invalid minimum nprobes value");
            }

            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Set the maximum number of probes for an IVF index search.
        /// </summary>
        /// <remarks>
        /// Limits the number of probes even if adaptive probe settings request more.
        /// Pass 0 for unlimited.
        /// </remarks>
        /// <param name="n">The maximum number of probes, or 0 for unlimited.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery MaximumNprobes(int n)
        {
            IntPtr newPtr = vector_query_maximum_nprobes(NativePtr, (uint)n);
            if (newPtr == IntPtr.Zero)
            {
                throw new LanceDbException("Invalid maximum nprobes value");
            }

            ReplacePtr(newPtr);
            return this;
        }

        /// <summary>
        /// Add an additional query vector for multi-vector search.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This allows running the same query against multiple vectors simultaneously.
        /// Each query vector is searched independently and the results are combined.
        /// </para>
        /// <para>
        /// The added vector must have the same dimensionality as the original query vector
        /// supplied to <see cref="Query.NearestTo"/>.
        /// </para>
        /// </remarks>
        /// <param name="vector">The additional query vector.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery AddQueryVector(float[] vector)
        {
            IntPtr newPtr = vector_query_add_query_vector(
                NativePtr, vector, (UIntPtr)vector.Length);
            if (newPtr == IntPtr.Zero)
            {
                throw new LanceDbException("Failed to add query vector");
            }

            ReplacePtr(newPtr);
            return this;
        }
    }
}