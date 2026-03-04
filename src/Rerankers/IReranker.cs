namespace lancedb
{
    using System.Threading.Tasks;
    using Apache.Arrow;
    /// <summary>
    /// Interface for a reranker. A reranker is used to rerank the results from a
    /// vector and FTS search. This is useful for combining the results from both
    /// search methods into a single, relevance-ordered result set.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Implementors must provide <see cref="RerankHybrid"/> which receives
    /// individual results from vector and FTS searches and produces a combined,
    /// reranked result. The result must include a <c>_relevance_score</c> column
    /// of type <see cref="Apache.Arrow.Types.FloatType"/>.
    /// </para>
    /// <para>
    /// Optionally, implementors can override <see cref="RerankFts"/> and/or
    /// <see cref="RerankVector"/> to support single-side reranking of standalone
    /// FTS or vector query results (e.g., with a cross-encoder model).
    /// By default these throw <see cref="NotSupportedException"/>.
    /// </para>
    /// </remarks>
    public interface IReranker
    {
        /// <summary>
        /// Rerank function receives the individual results from the vector and FTS
        /// search results. You can choose to use any of the results to generate the
        /// final results, allowing maximum flexibility.
        /// </summary>
        /// <param name="query">The input query string.</param>
        /// <param name="vectorResults">The results from the vector search.</param>
        /// <param name="ftsResults">The results from the full-text search.</param>
        /// <returns>A <see cref="RecordBatch"/> sorted by relevance score descending,
        /// with a <c>_relevance_score</c> column appended.</returns>
        Task<RecordBatch> RerankHybrid(
            string query,
            RecordBatch vectorResults,
            RecordBatch ftsResults);

        /// <summary>
        /// Rerank function receives the result from the full-text search.
        /// This is not mandatory to implement.
        /// </summary>
        /// <remarks>
        /// Override this method to support reranking standalone FTS query results
        /// via <see cref="FTSQuery.Rerank"/>. Useful for cross-encoder or
        /// API-based rerankers that can re-score text search results.
        /// The default convention is to throw <see cref="NotSupportedException"/>
        /// if not supported.
        /// </remarks>
        /// <param name="query">The input query string.</param>
        /// <param name="ftsResults">The results from the FTS search.</param>
        /// <returns>A <see cref="RecordBatch"/> with reranked results including
        /// a <c>_relevance_score</c> column.</returns>
        Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults);

        /// <summary>
        /// Rerank function receives the result from the vector search.
        /// This is not mandatory to implement.
        /// </summary>
        /// <remarks>
        /// Override this method to support reranking standalone vector query results
        /// via <see cref="VectorQuery.Rerank"/>. Useful for cross-encoder or
        /// API-based rerankers that can re-score vector search results using
        /// the original query text.
        /// The default convention is to throw <see cref="NotSupportedException"/>
        /// if not supported.
        /// </remarks>
        /// <param name="query">The input query string.</param>
        /// <param name="vectorResults">The results from the vector search.</param>
        /// <returns>A <see cref="RecordBatch"/> with reranked results including
        /// a <c>_relevance_score</c> column.</returns>
        Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults);
    }
}
