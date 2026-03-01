namespace lancedb
{
    using System;
    using System.Collections.Generic;
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
        private static extern void vector_query_execute(
            IntPtr table_ptr, double[] vector, UIntPtr vector_len, IntPtr params_json,
            long timeout_ms, uint max_batch_length, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_explain_plan(
            IntPtr table_ptr, double[] vector, UIntPtr vector_len, IntPtr params_json,
            [MarshalAs(UnmanagedType.U1)] bool verbose, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_analyze_plan(
            IntPtr table_ptr, double[] vector, UIntPtr vector_len, IntPtr params_json,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_output_schema(
            IntPtr table_ptr, double[] vector, UIntPtr vector_len, IntPtr params_json,
            NativeCall.FfiCallback completion);

        internal readonly double[] _vector;

        // Vector-specific stored parameters
        internal string? _column;
        internal int? _distanceType;
        internal int? _nprobes;
        internal int? _refineFactor;
        internal bool _bypassVectorIndex;
        internal int? _ef;
        private float? _distanceRangeLower;
        private float? _distanceRangeUpper;
        internal int? _minimumNprobes;
        internal int? _maximumNprobes;
        private List<float[]>? _additionalVectors;

        internal VectorQuery(IntPtr tablePtr, Query parentQuery, double[] vector)
            : base(tablePtr)
        {
            _vector = vector;
            // Copy base params from the parent query
            _selectJson = parentQuery._selectJson;
            _predicate = parentQuery._predicate;
            _limit = parentQuery._limit;
            _offset = parentQuery._offset;
            _withRowId = parentQuery._withRowId;
            _fullTextSearchQuery = parentQuery._fullTextSearchQuery;
            _fastSearch = parentQuery._fastSearch;
            _postfilter = parentQuery._postfilter;
        }

        /// <inheritdoc/>
        internal override Dictionary<string, object> BuildParamsDict()
        {
            var dict = base.BuildParamsDict();
            if (_column != null)
            {
                dict["column"] = _column;
            }
            if (_distanceType.HasValue)
            {
                dict["distance_type"] = _distanceType.Value;
            }
            if (_nprobes.HasValue)
            {
                dict["nprobes"] = _nprobes.Value;
            }
            if (_refineFactor.HasValue)
            {
                dict["refine_factor"] = _refineFactor.Value;
            }
            if (_bypassVectorIndex)
            {
                dict["bypass_vector_index"] = true;
            }
            if (_ef.HasValue)
            {
                dict["ef"] = _ef.Value;
            }
            if (_distanceRangeLower.HasValue)
            {
                dict["distance_range_lower"] = _distanceRangeLower.Value;
            }
            if (_distanceRangeUpper.HasValue)
            {
                dict["distance_range_upper"] = _distanceRangeUpper.Value;
            }
            if (_minimumNprobes.HasValue)
            {
                dict["minimum_nprobes"] = _minimumNprobes.Value;
            }
            if (_maximumNprobes.HasValue)
            {
                dict["maximum_nprobes"] = _maximumNprobes.Value;
            }
            if (_additionalVectors != null && _additionalVectors.Count > 0)
            {
                dict["additional_vectors"] = _additionalVectors;
            }
            return dict;
        }

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExecute(
            IntPtr tablePtr, IntPtr paramsJson, long timeoutMs, uint maxBatchLength,
            NativeCall.FfiCallback callback)
            => vector_query_execute(
                tablePtr, _vector, (UIntPtr)_vector.Length, paramsJson,
                timeoutMs, maxBatchLength, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExplainPlan(
            IntPtr tablePtr, IntPtr paramsJson, bool verbose, NativeCall.FfiCallback callback)
            => vector_query_explain_plan(
                tablePtr, _vector, (UIntPtr)_vector.Length, paramsJson, verbose, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedAnalyzePlan(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback)
            => vector_query_analyze_plan(
                tablePtr, _vector, (UIntPtr)_vector.Length, paramsJson, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedOutputSchema(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback)
            => vector_query_output_schema(
                tablePtr, _vector, (UIntPtr)_vector.Length, paramsJson, callback);

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
            _column = column;
            return this;
        }

        /// <summary>
        /// Set the distance metric to use for the vector search.
        /// </summary>
        /// <remarks>
        /// If not specified, the distance type is inferred from the vector column's metadata
        /// or defaults to <see cref="lancedb.DistanceType.L2"/>.
        /// </remarks>
        /// <param name="distanceType">The distance metric to use.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery DistanceType(DistanceType distanceType)
        {
            _distanceType = (int)distanceType;
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
            _nprobes = nprobes;
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
            _refineFactor = refineFactor;
            return this;
        }

        /// <summary>
        /// Skip the vector index and perform a brute-force search.
        /// </summary>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery BypassVectorIndex()
        {
            _bypassVectorIndex = true;
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
            _ef = ef;
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
            _distanceRangeLower = lowerBound;
            _distanceRangeUpper = upperBound;
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
            _minimumNprobes = n;
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
            _maximumNprobes = n;
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
            if (_additionalVectors == null)
            {
                _additionalVectors = new List<float[]>();
            }
            _additionalVectors.Add(vector);
            return this;
        }

        /// <summary>
        /// Combine this vector search with a full-text search to create a hybrid query.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The resulting <see cref="HybridQuery"/> executes both vector and FTS searches
        /// independently and merges the results using a reranker (default: <see cref="RRFReranker"/>).
        /// </para>
        /// </remarks>
        /// <param name="query">The full-text search query string.</param>
        /// <param name="columns">
        /// Optional list of column names to search. If <c>null</c>, all FTS-indexed
        /// columns are searched.
        /// </param>
        /// <returns>A <see cref="HybridQuery"/> that can be further parameterized.</returns>
        public HybridQuery NearestToText(string query, string[]? columns = null)
        {
            return new HybridQuery(this, query, columns);
        }
    }
}