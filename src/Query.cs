namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// A builder for LanceDB queries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is not intended to be created directly. Instead, use the
    /// <see cref="Table.Query"/> method to create a query.
    /// </para>
    /// <para>
    /// Queries allow you to search your existing data. By default the query will
    /// return all the data in the table in no particular order. The builder
    /// methods can be used to control the query using filtering, projection,
    /// and limits.
    /// </para>
    /// </remarks>
    public class Query : QueryBase<Query>
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute(
            IntPtr table_ptr, IntPtr params_json, long timeout_ms, uint max_batch_length,
            NativeCall.FfiCallback completion, IntPtr userData);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute_stream(
            IntPtr table_ptr, IntPtr params_json, long timeout_ms, uint max_batch_length,
            NativeCall.FfiCallback completion, IntPtr userData);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_explain_plan(
            IntPtr table_ptr, IntPtr params_json,
            [MarshalAs(UnmanagedType.U1)] bool verbose,
            NativeCall.FfiCallback completion, IntPtr userData);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_analyze_plan(
            IntPtr table_ptr, IntPtr params_json, NativeCall.FfiCallback completion, IntPtr userData);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_output_schema(
            IntPtr table_ptr, IntPtr params_json, NativeCall.FfiCallback completion, IntPtr userData);

        internal Query(IntPtr tablePtr)
            : base(tablePtr)
        {
        }

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExecute(
            IntPtr tablePtr, IntPtr paramsJson, long timeoutMs, uint maxBatchLength,
            NativeCall.FfiCallback callback, IntPtr userData)
            => query_execute(tablePtr, paramsJson, timeoutMs, maxBatchLength, callback, userData);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExplainPlan(
            IntPtr tablePtr, IntPtr paramsJson, bool verbose, NativeCall.FfiCallback callback, IntPtr userData)
            => query_explain_plan(tablePtr, paramsJson, verbose, callback, userData);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedAnalyzePlan(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback, IntPtr userData)
            => query_analyze_plan(tablePtr, paramsJson, callback, userData);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedOutputSchema(
            IntPtr tablePtr, IntPtr paramsJson, NativeCall.FfiCallback callback, IntPtr userData)
            => query_output_schema(tablePtr, paramsJson, callback, userData);

        /// <inheritdoc/>
        private protected override void NativeConsolidatedExecuteStream(
            IntPtr tablePtr, IntPtr paramsJson, long timeoutMs, uint maxBatchLength,
            NativeCall.FfiCallback callback, IntPtr userData)
            => query_execute_stream(tablePtr, paramsJson, timeoutMs, maxBatchLength, callback, userData);

        /// <summary>
        /// Find the nearest vectors to the given query vector.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This converts the query from a plain query to a vector query.
        /// </para>
        /// <para>
        /// The input should be an array of floats (or doubles) that represents the
        /// query vector.
        /// </para>
        /// <para>
        /// If there is only one vector column (a column whose data type is a
        /// fixed size list of floats) then the column does not need to be specified.
        /// If there is more than one vector column you must use
        /// <see cref="VectorQuery.Column"/> to specify which column to compare with.
        /// </para>
        /// <para>
        /// If no index has been created on the vector column then a vector query
        /// will perform a distance comparison between the query vector and every
        /// vector in the database and then sort the results. This is sometimes
        /// called a "flat search".
        /// </para>
        /// <para>
        /// For small databases, with tens of thousands of vectors or less, this can
        /// be reasonably fast. In larger databases you should create a vector index
        /// on the column. If there is a vector index then an "approximate" nearest
        /// neighbor search (frequently called an ANN search) will be performed. This
        /// search is much faster, but the results will be approximate.
        /// </para>
        /// <para>
        /// Vector searches always have a limit. If <c>Limit</c> has not been called then
        /// a default limit of 10 will be used.
        /// </para>
        /// </remarks>
        /// <param name="vector">The query vector to search for nearest neighbors.</param>
        /// <returns>A <see cref="VectorQuery"/> that can be used to further parameterize the search.</returns>
        public VectorQuery NearestTo(float[] vector)
        {
            return new VectorQuery(_tablePtr, this, vector);
        }

        /// <summary>
        /// Convenience overload accepting <c>double[]</c>. Values are cast to
        /// <c>float</c> before crossing the FFI, matching Python SDK's sync
        /// path which normalizes query vector to <c>Float32</c> before
        /// invoking the native layer. No precision is lost. Prefer the
        /// <c>float[]</c> overload to skip this conversion when possible.
        /// </summary>
        public VectorQuery NearestTo(double[] vector)
        {
            var f = new float[vector.Length];
            for (int i = 0; i < vector.Length; i++) { f[i] = (float)vector[i]; }
            return new VectorQuery(_tablePtr, this, f);
        }

        /// <summary>
        /// Find the nearest rows to the given text query using full-text search.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This creates a new <see cref="FTSQuery"/> with the full-text search configured.
        /// The results will be returned in order of relevance (BM25 scores).
        /// </para>
        /// <para>
        /// This method is only valid on tables that have a full-text search index.
        /// Use <see cref="Table.CreateIndex"/> with <see cref="FtsIndex"/> to create one.
        /// </para>
        /// <para>
        /// Full-text search always has a limit. If <see cref="QueryBase{T}.Limit"/> has not
        /// been called then a default limit of 10 will be used.
        /// </para>
        /// <para>
        /// To combine full-text search with vector search (hybrid search), chain
        /// with <see cref="FTSQuery.NearestTo"/>:
        /// <c>table.Query().NearestToText("search terms").NearestTo(vector)</c>
        /// </para>
        /// </remarks>
        /// <param name="query">The search query string.</param>
        /// <param name="columns">
        /// Optional list of column names to search. If <c>null</c>, all FTS-indexed
        /// columns are searched.
        /// </param>
        /// <returns>A <see cref="FTSQuery"/> with full-text search applied.</returns>
        public FTSQuery NearestToText(string query, string[]? columns = null)
        {
            var ftsQuery = new FTSQuery(_tablePtr);
            ftsQuery._selectColumns = _selectColumns;
            ftsQuery._selectExpressions = _selectExpressions;
            ftsQuery._predicate = _predicate;
            ftsQuery._limit = _limit;
            ftsQuery._offset = _offset;
            ftsQuery._withRowId = _withRowId;
            ftsQuery._fastSearch = _fastSearch;
            ftsQuery._postfilter = _postfilter;
            ftsQuery._fullTextSearchQuery = query;
            ftsQuery._fullTextSearchColumns = columns;
            return ftsQuery;
        }

        /// <summary>
        /// Find the nearest rows to the given structured full-text query.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This creates a new <see cref="FTSQuery"/> configured with a structured
        /// <see cref="FullTextQuery"/> (for example <see cref="MatchQuery"/>,
        /// <see cref="PhraseQuery"/>, <see cref="BoostQuery"/>,
        /// <see cref="MultiMatchQuery"/>, or <see cref="BooleanQuery"/>). The columns
        /// to search are carried by the query nodes themselves.
        /// </para>
        /// <para>
        /// This method is only valid on tables that have a full-text search index.
        /// Use <see cref="Table.CreateIndex"/> with <see cref="FtsIndex"/> to create one.
        /// </para>
        /// <para>
        /// To combine full-text search with vector search (hybrid search), chain
        /// with <see cref="FTSQuery.NearestTo"/>.
        /// </para>
        /// </remarks>
        /// <param name="query">The structured full-text query to run.</param>
        /// <returns>A <see cref="FTSQuery"/> with full-text search applied.</returns>
        public FTSQuery NearestToText(FullTextQuery query)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }
            var ftsQuery = new FTSQuery(_tablePtr);
            ftsQuery._selectColumns = _selectColumns;
            ftsQuery._selectExpressions = _selectExpressions;
            ftsQuery._predicate = _predicate;
            ftsQuery._limit = _limit;
            ftsQuery._offset = _offset;
            ftsQuery._withRowId = _withRowId;
            ftsQuery._fastSearch = _fastSearch;
            ftsQuery._postfilter = _postfilter;
            ftsQuery._fullTextQueryJson = query.ToJson();
            ftsQuery._fullTextSearchQuery = query.RerankText;
            return ftsQuery;
        }
    }
}