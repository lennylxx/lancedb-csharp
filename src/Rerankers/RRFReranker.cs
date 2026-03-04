namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Arrow;
    /// <summary>
    /// Reranks the results using Reciprocal Rank Fusion (RRF) algorithm based
    /// on the positions of results from vector and FTS search.
    /// </summary>
    /// <remarks>
    /// RRF combines multiple ranked lists by assigning each result a score of
    /// <c>1 / (rank + k)</c> from each list, then summing them. Results appearing
    /// in both lists get higher combined scores. The constant <c>k</c> (default 60)
    /// dampens the effect of high rankings. See:
    /// https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
    /// </remarks>
    public class RRFReranker : IReranker
    {
        private readonly float _k;
        private readonly string _returnScore;

        /// <summary>
        /// Creates a new <see cref="RRFReranker"/> with the specified constant.
        /// </summary>
        /// <param name="k">A constant used in the RRF formula. Experiments indicate
        /// that k=60 is near-optimal. Must be greater than 0.</param>
        /// <param name="returnScore">
        /// Controls which score columns appear in the output.
        /// <c>"relevance"</c> (default) returns only <c>_relevance_score</c>.
        /// <c>"all"</c> also keeps <c>_distance</c> and <c>_score</c>.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="k"/> is less than or equal to 0.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="returnScore"/> is not <c>"relevance"</c> or <c>"all"</c>.
        /// </exception>
        public RRFReranker(float k = 60f, string returnScore = "relevance")
        {
            if (k <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(k), "k must be greater than 0.");
            }
            if (returnScore != "relevance" && returnScore != "all")
            {
                throw new ArgumentException(
                    "returnScore must be \"relevance\" or \"all\".",
                    nameof(returnScore));
            }
            _k = k;
            _returnScore = returnScore;
        }

        /// <inheritdoc />
        public Task<RecordBatch> RerankHybrid(
            string query,
            RecordBatch vectorResults,
            RecordBatch ftsResults)
        {
            var rrfScores = new Dictionary<ulong, float>();

            // Calculate RRF scores from vector results
            if (vectorResults.Length > 0)
            {
                var vecRowIdIndex = RerankerHelpers.GetColumnIndex(vectorResults.Schema, RerankerHelpers.RowIdColumn);
                var vecRowIds = (UInt64Array)vectorResults.Column(vecRowIdIndex);
                for (int i = 0; i < vecRowIds.Length; i++)
                {
                    var id = vecRowIds.GetValue(i);
                    if (id.HasValue)
                    {
                        float score = 1f / (i + 1 + _k);
                        if (rrfScores.ContainsKey(id.Value))
                        {
                            rrfScores[id.Value] += score;
                        }
                        else
                        {
                            rrfScores[id.Value] = score;
                        }
                    }
                }
            }

            // Calculate RRF scores from FTS results
            if (ftsResults.Length > 0)
            {
                var ftsRowIdIndex = RerankerHelpers.GetColumnIndex(ftsResults.Schema, RerankerHelpers.RowIdColumn);
                var ftsRowIds = (UInt64Array)ftsResults.Column(ftsRowIdIndex);
                for (int i = 0; i < ftsRowIds.Length; i++)
                {
                    var id = ftsRowIds.GetValue(i);
                    if (id.HasValue)
                    {
                        float score = 1f / (i + 1 + _k);
                        if (rrfScores.ContainsKey(id.Value))
                        {
                            rrfScores[id.Value] += score;
                        }
                        else
                        {
                            rrfScores[id.Value] = score;
                        }
                    }
                }
            }

            // Merge and deduplicate
            var combined = RerankerHelpers.MergeResults(vectorResults, ftsResults);

            // Build relevance scores array
            var rowIdIndex = RerankerHelpers.GetColumnIndex(combined.Schema, RerankerHelpers.RowIdColumn);
            var rowIds = (UInt64Array)combined.Column(rowIdIndex);
            var relevanceScores = new float[combined.Length];
            for (int i = 0; i < rowIds.Length; i++)
            {
                var id = rowIds.GetValue(i);
                if (id.HasValue)
                {
                    relevanceScores[i] = rrfScores[id.Value];
                }
            }

            // Append relevance score column and sort descending
            var result = RerankerHelpers.AppendColumn(combined, RerankerHelpers.RelevanceScoreColumn, relevanceScores);
            result = RerankerHelpers.SortByDescending(result, RerankerHelpers.RelevanceScoreColumn);
            result = RerankerHelpers.KeepRelevanceScore(result, _returnScore);

                        return Task.FromResult(result);
        }

        /// <inheritdoc/>
        public Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults)
        {
            throw new NotSupportedException(
                $"{nameof(RRFReranker)} does not support RerankFts.");
        }

        /// <inheritdoc/>
        public Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults)
        {
            throw new NotSupportedException(
                $"{nameof(RRFReranker)} does not support RerankVector.");
        }
    }
}