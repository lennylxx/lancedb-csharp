namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using System.Threading.Tasks;
    using Apache.Arrow;

    /// <summary>
    /// A builder for full-text search queries, created by <see cref="Query.NearestToText"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A full-text search query returns results ordered by BM25 relevance score.
    /// It inherits all the builder methods from <see cref="QueryBase{T}"/>
    /// for filtering, projection, and pagination.
    /// </para>
    /// <para>
    /// To combine full-text search with vector search (hybrid search), call
    /// <see cref="NearestTo"/> to transition to a <see cref="HybridQuery"/>.
    /// </para>
    /// <para>
    /// To apply a custom reranker to the FTS results, call <see cref="Rerank"/>.
    /// The reranker must implement <see cref="IReranker.RerankFts"/>.
    /// </para>
    /// </remarks>
    public class FTSQuery : QueryBase<FTSQuery>
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute(
            IntPtr table_ptr, IntPtr params_json, long timeout_ms, uint max_batch_length,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute_stream(
            IntPtr table_ptr, IntPtr params_json, long timeout_ms, uint max_batch_length,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_explain_plan(
            IntPtr table_ptr, IntPtr params_json,
            [MarshalAs(UnmanagedType.U1)] bool verbose,
            NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_analyze_plan(
            IntPtr table_ptr, IntPtr params_json, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_output_schema(
            IntPtr table_ptr, IntPtr params_json, NativeCall.FfiCallback completion);

        private IReranker? _reranker;

        internal FTSQuery(IntPtr tablePtr)
            : base(tablePtr)
        {
        }

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExecute(
            IntPtr tablePtr, IntPtr paramsJson, long timeoutMs, uint maxBatchLength,
            NativeCall.FfiCallback callback)
            => query_execute(tablePtr, paramsJson, timeoutMs, maxBatchLength, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExplainPlan(
            IntPtr tablePtr, IntPtr paramsJson, bool verbose, NativeCall.FfiCallback callback)
            => query_explain_plan(tablePtr, paramsJson, verbose, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedAnalyzePlan(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback)
            => query_analyze_plan(tablePtr, paramsJson, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedOutputSchema(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback)
            => query_output_schema(tablePtr, paramsJson, callback);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExecuteStream(
            IntPtr tablePtr, IntPtr paramsJson, long timeoutMs, uint maxBatchLength,
            NativeCall.FfiCallback callback)
            => query_execute_stream(tablePtr, paramsJson, timeoutMs, maxBatchLength, callback);

        /// <summary>
        /// Rerank the FTS results using the specified reranker.
        /// </summary>
        /// <remarks>
        /// The reranker must implement <see cref="IReranker.RerankFts"/>.
        /// The reranker receives the FTS results and the query string, and returns
        /// reranked results with a <c>_relevance_score</c> column.
        /// </remarks>
        /// <param name="reranker">The reranker to apply to the FTS results.</param>
        /// <returns>This query instance for method chaining.</returns>
        public FTSQuery Rerank(IReranker reranker)
        {
            _reranker = reranker ?? throw new ArgumentNullException(nameof(reranker));
            return this;
        }

        /// <inheritdoc/>
        public override async Task<RecordBatch> ToArrow(
            TimeSpan? timeout = null, int? maxBatchLength = null)
        {
            var result = await base.ToArrow(timeout, maxBatchLength).ConfigureAwait(false);
            if (_reranker != null)
            {
                result = await _reranker.RerankFts(_fullTextSearchQuery!, result)
                    .ConfigureAwait(false);
            }
            return result;
        }

        /// <inheritdoc/>
        public override async Task<AsyncRecordBatchReader> ToBatches(
            TimeSpan? timeout = null, int? maxBatchLength = null)
        {
            if (_reranker == null)
            {
                return await base.ToBatches(timeout, maxBatchLength).ConfigureAwait(false);
            }
            var result = await ToArrow(timeout).ConfigureAwait(false);
            return AsyncRecordBatchReader.FromRecordBatch(
                result, maxBatchLength.HasValue ? maxBatchLength.Value : null);
        }

        /// <summary>
        /// Find the nearest vectors to the given query vector, creating a hybrid search
        /// that combines full-text search with vector search.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This transitions the full-text search query to a <see cref="HybridQuery"/>
        /// that executes both vector and FTS queries internally and merges the results
        /// using a reranker (default: <see cref="RRFReranker"/>).
        /// </para>
        /// </remarks>
        /// <param name="vector">The query vector to search for nearest neighbors.</param>
        /// <returns>A <see cref="HybridQuery"/> that can be further parameterized.</returns>
        public HybridQuery NearestTo(double[] vector)
        {
            return new HybridQuery(this, vector);
        }
    }
}
