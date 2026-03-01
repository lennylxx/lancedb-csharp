namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Arrow;

    /// <summary>
    /// Reranks the results using Mean Reciprocal Rank (MRR) algorithm based
    /// on the scores of vector and FTS search.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Algorithm reference: https://en.wikipedia.org/wiki/Mean_reciprocal_rank
    /// </para>
    /// <para>
    /// MRR calculates the average of reciprocal ranks across different search results.
    /// For each document, it computes the reciprocal of its rank in each system,
    /// then takes the weighted mean of these reciprocal ranks as the final score.
    /// </para>
    /// <para>
    /// If a document does not appear in one of the result sets, its reciprocal rank
    /// for that set is 0.
    /// </para>
    /// </remarks>
    public class MRRReranker : IReranker
    {
        private readonly float _weightVector;
        private readonly float _weightFts;

        /// <summary>
        /// Creates a new <see cref="MRRReranker"/> with the specified weights.
        /// </summary>
        /// <param name="weightVector">
        /// Weight for vector search results (0.0 to 1.0). Default is 0.5.
        /// </param>
        /// <param name="weightFts">
        /// Weight for FTS search results (0.0 to 1.0). Default is 0.5.
        /// <c>weightVector + weightFts</c> must equal 1.0.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="weightVector"/> or <paramref name="weightFts"/>
        /// is outside the range [0.0, 1.0].
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="weightVector"/> + <paramref name="weightFts"/>
        /// does not equal 1.0.
        /// </exception>
        public MRRReranker(float weightVector = 0.5f, float weightFts = 0.5f)
        {
            if (weightVector < 0f || weightVector > 1f)
            {
                throw new ArgumentOutOfRangeException(nameof(weightVector),
                    "weightVector must be between 0.0 and 1.0.");
            }
            if (weightFts < 0f || weightFts > 1f)
            {
                throw new ArgumentOutOfRangeException(nameof(weightFts),
                    "weightFts must be between 0.0 and 1.0.");
            }
            if (Math.Abs(weightVector + weightFts - 1f) > 1e-6f)
            {
                throw new ArgumentException(
                    "weightVector + weightFts must equal 1.0.");
            }

            _weightVector = weightVector;
            _weightFts = weightFts;
        }

        /// <inheritdoc />
        public Task<RecordBatch> RerankHybrid(
            string query,
            RecordBatch vectorResults,
            RecordBatch ftsResults)
        {
            var mrrScores = new Dictionary<ulong, (float vectorRR, float ftsRR)>();

            // Calculate reciprocal ranks from vector results
            if (vectorResults.Length > 0)
            {
                var vecRowIdIndex = RerankerHelpers.GetColumnIndex(
                    vectorResults.Schema, RerankerHelpers.RowIdColumn);
                var vecRowIds = (UInt64Array)vectorResults.Column(vecRowIdIndex);
                for (int rank = 0; rank < vecRowIds.Length; rank++)
                {
                    var id = vecRowIds.GetValue(rank);
                    if (id.HasValue)
                    {
                        float reciprocalRank = 1f / (rank + 1);
                        mrrScores[id.Value] = (reciprocalRank, 0f);
                    }
                }
            }

            // Calculate reciprocal ranks from FTS results
            if (ftsResults.Length > 0)
            {
                var ftsRowIdIndex = RerankerHelpers.GetColumnIndex(
                    ftsResults.Schema, RerankerHelpers.RowIdColumn);
                var ftsRowIds = (UInt64Array)ftsResults.Column(ftsRowIdIndex);
                for (int rank = 0; rank < ftsRowIds.Length; rank++)
                {
                    var id = ftsRowIds.GetValue(rank);
                    if (id.HasValue)
                    {
                        float reciprocalRank = 1f / (rank + 1);
                        if (mrrScores.TryGetValue(id.Value, out var existing))
                        {
                            mrrScores[id.Value] = (existing.vectorRR, reciprocalRank);
                        }
                        else
                        {
                            mrrScores[id.Value] = (0f, reciprocalRank);
                        }
                    }
                }
            }

            // Compute weighted MRR scores
            var finalScores = new Dictionary<ulong, float>(mrrScores.Count);
            foreach (var kvp in mrrScores)
            {
                float weightedMrr = _weightVector * kvp.Value.vectorRR
                                  + _weightFts * kvp.Value.ftsRR;
                finalScores[kvp.Key] = weightedMrr;
            }

            // Merge and deduplicate
            var combined = RerankerHelpers.MergeResults(vectorResults, ftsResults);

            // Build relevance scores array
            var rowIdIndex = RerankerHelpers.GetColumnIndex(
                combined.Schema, RerankerHelpers.RowIdColumn);
            var rowIds = (UInt64Array)combined.Column(rowIdIndex);
            var relevanceScores = new float[combined.Length];
            for (int i = 0; i < rowIds.Length; i++)
            {
                var id = rowIds.GetValue(i);
                if (id.HasValue)
                {
                    relevanceScores[i] = finalScores[id.Value];
                }
            }

            // Append relevance score column and sort descending
            var result = RerankerHelpers.AppendColumn(
                combined, RerankerHelpers.RelevanceScoreColumn, relevanceScores);
            result = RerankerHelpers.SortByDescending(
                result, RerankerHelpers.RelevanceScoreColumn);

            return Task.FromResult(result);
        }
    }
}
