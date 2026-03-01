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

        /// <summary>
        /// Creates a new <see cref="RRFReranker"/> with the specified constant.
        /// </summary>
        /// <param name="k">A constant used in the RRF formula. Experiments indicate
        /// that k=60 is near-optimal. Must be greater than 0.</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="k"/> is less than or equal to 0.
        /// </exception>
        public RRFReranker(float k = 60f)
        {
            if (k <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(k), "k must be greater than 0.");
            }
            _k = k;
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
                        float score = 1f / (i + _k);
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
                        float score = 1f / (i + _k);
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

            return Task.FromResult(result);
        }
    }
}
