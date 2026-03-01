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
    /// Implementors must provide <see cref="RerankHybridAsync"/> which receives
    /// individual results from vector and FTS searches and produces a combined,
    /// reranked result. The result must include a <c>_relevance_score</c> column
    /// of type <see cref="Apache.Arrow.Types.FloatType"/>.
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
    }
}
