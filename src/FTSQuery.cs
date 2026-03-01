namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;

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
    /// </remarks>
    public class FTSQuery : QueryBase<FTSQuery>
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute(
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
