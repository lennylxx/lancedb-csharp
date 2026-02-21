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
        private static extern void vector_query_execute(IntPtr vq_ptr, NativeCall.FfiCallback completion);

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
        private protected override void NativeExecute(IntPtr ptr, NativeCall.FfiCallback callback)
            => vector_query_execute(ptr, callback);

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
        /// Supported values are "l2", "cosine", and "dot".
        /// If not specified, the distance type is inferred from the vector column's metadata
        /// or defaults to "l2".
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
        /// Apply filtering after the vector search, instead of before.
        /// </summary>
        /// <remarks>
        /// By default, filters are applied before the vector search (prefiltering).
        /// This can sometimes reduce the number of results below the requested limit.
        /// Postfiltering applies the filter after the vector search, which guarantees
        /// the requested number of results but may be slower and less accurate.
        /// </remarks>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery Postfilter()
        {
            IntPtr newPtr = vector_query_postfilter(NativePtr);
            ReplacePtr(newPtr);
            return this;
        }
    }
}