namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Arrow;
    /// <summary>
    /// Reranks the results using a linear combination of the scores from the
    /// vector and FTS search.
    /// </summary>
    /// <remarks>
    /// The relevance score is computed as:
    /// <c>1 - (weight * vectorScore + (1 - weight) * ftsScore)</c>
    /// where <c>vectorScore = 1 - distance</c> (inverted from distance to similarity).
    /// For results appearing in only one search, the missing score defaults to the
    /// <c>fill</c> value (treated as a penalty — higher fill means lower relevance).
    /// </remarks>
    public class LinearCombinationReranker : IReranker
    {
        private readonly float _weight;
        private readonly float _fill;
        private readonly string _returnScore;

        /// <summary>
        /// Creates a new <see cref="LinearCombinationReranker"/>.
        /// </summary>
        /// <param name="weight">The weight given to the vector score. Must be between 0 and 1.
        /// A value of 0.7 means 70% vector, 30% FTS.</param>
        /// <param name="fill">The penalty score assigned to results that appear in only one
        /// of the two result sets. Higher values produce lower relevance scores for
        /// single-source results. Default is 1.0.</param>
        /// <param name="returnScore">
        /// Controls which score columns appear in the output.
        /// <c>"relevance"</c> (default) returns only <c>_relevance_score</c>.
        /// <c>"all"</c> also keeps <c>_distance</c> and <c>_score</c>.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="weight"/> is not between 0 and 1.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="returnScore"/> is not <c>"relevance"</c> or <c>"all"</c>.
        /// </exception>
        public LinearCombinationReranker(float weight = 0.7f, float fill = 1.0f,
            string returnScore = "relevance")
        {
            if (weight < 0 || weight > 1)
            {
                throw new ArgumentOutOfRangeException(nameof(weight), "weight must be between 0 and 1.");
            }
            if (returnScore != "relevance" && returnScore != "all")
            {
                throw new ArgumentException(
                    "returnScore must be \"relevance\" or \"all\".",
                    nameof(returnScore));
            }
            _weight = weight;
            _fill = fill;
            _returnScore = returnScore;
        }

        /// <inheritdoc />
        public Task<RecordBatch> RerankHybrid(
            string query,
            RecordBatch vectorResults,
            RecordBatch ftsResults)
        {
            // When one side is empty, short-circuit: use the available score directly
            // as relevance (matching Python's LinearCombinationReranker.merge_results).
            if (vectorResults.Length == 0)
            {
                return Task.FromResult(HandleEmptyVector(ftsResults));
            }

            if (ftsResults.Length == 0)
            {
                return Task.FromResult(HandleEmptyFts(vectorResults));
            }

            // Both sides have results — use the full weighted formula.
            return Task.FromResult(CombineBothSides(vectorResults, ftsResults));
        }

        /// <summary>
        /// When vector results are empty, relevance = FTS score directly.
        /// </summary>
        private RecordBatch HandleEmptyVector(RecordBatch ftsResults)
        {
            var scoreIdx = RerankerHelpers.GetColumnIndex(ftsResults.Schema,
                RerankerHelpers.ScoreColumn);
            var scores = (FloatArray)ftsResults.Column(scoreIdx);
            var relevance = new float[ftsResults.Length];
            for (int i = 0; i < ftsResults.Length; i++)
            {
                relevance[i] = scores.GetValue(i) ?? 0f;
            }

            var result = RerankerHelpers.AppendColumn(ftsResults,
                RerankerHelpers.RelevanceScoreColumn, relevance);

            if (_returnScore == "all")
            {
                result = RerankerHelpers.AppendNanColumn(result,
                    RerankerHelpers.DistanceColumn);
            }
            else
            {
                result = RerankerHelpers.KeepRelevanceScore(result, _returnScore);
            }

            return result;
        }

        /// <summary>
        /// When FTS results are empty, relevance = inverted distance (1 - distance).
        /// </summary>
        private RecordBatch HandleEmptyFts(RecordBatch vectorResults)
        {
            var distIdx = RerankerHelpers.GetColumnIndex(vectorResults.Schema,
                RerankerHelpers.DistanceColumn);
            var distances = (FloatArray)vectorResults.Column(distIdx);
            var relevance = new float[vectorResults.Length];
            for (int i = 0; i < vectorResults.Length; i++)
            {
                relevance[i] = 1f - (distances.GetValue(i) ?? 0f);
            }

            var result = RerankerHelpers.AppendColumn(vectorResults,
                RerankerHelpers.RelevanceScoreColumn, relevance);

            if (_returnScore == "all")
            {
                result = RerankerHelpers.AppendNanColumn(result,
                    RerankerHelpers.ScoreColumn);
            }
            else
            {
                result = RerankerHelpers.KeepRelevanceScore(result, _returnScore);
            }

            return result;
        }

        /// <summary>
        /// Both sides have results — merge, deduplicate, compute weighted relevance.
        /// </summary>
        private RecordBatch CombineBothSides(RecordBatch vectorResults,
            RecordBatch ftsResults)
        {
            // Build lookup maps: rowId -> score for each result set
            var vectorScores = new Dictionary<ulong, float>();
            {
                var distIdx = RerankerHelpers.GetColumnIndex(vectorResults.Schema, RerankerHelpers.DistanceColumn);
                var rowIdIdx = RerankerHelpers.GetColumnIndex(vectorResults.Schema, RerankerHelpers.RowIdColumn);
                var distances = (FloatArray)vectorResults.Column(distIdx);
                var rowIds = (UInt64Array)vectorResults.Column(rowIdIdx);
                for (int i = 0; i < rowIds.Length; i++)
                {
                    var id = rowIds.GetValue(i);
                    var dist = distances.GetValue(i);
                    if (id.HasValue && dist.HasValue)
                    {
                        vectorScores[id.Value] = dist.Value;
                    }
                }
            }

            var ftsScores = new Dictionary<ulong, float>();
            {
                var scoreIdx = RerankerHelpers.GetColumnIndex(ftsResults.Schema, RerankerHelpers.ScoreColumn);
                var rowIdIdx = RerankerHelpers.GetColumnIndex(ftsResults.Schema, RerankerHelpers.RowIdColumn);
                var scores = (FloatArray)ftsResults.Column(scoreIdx);
                var rowIds = (UInt64Array)ftsResults.Column(rowIdIdx);
                for (int i = 0; i < rowIds.Length; i++)
                {
                    var id = rowIds.GetValue(i);
                    var score = scores.GetValue(i);
                    if (id.HasValue && score.HasValue)
                    {
                        ftsScores[id.Value] = score.Value;
                    }
                }
            }

            // Merge and deduplicate
            var combined = RerankerHelpers.MergeResults(vectorResults, ftsResults);

            // Compute relevance scores
            var rowIdIndex = RerankerHelpers.GetColumnIndex(combined.Schema, RerankerHelpers.RowIdColumn);
            var combinedRowIds = (UInt64Array)combined.Column(rowIdIndex);
            var relevanceScores = new float[combined.Length];

            for (int i = 0; i < combinedRowIds.Length; i++)
            {
                var id = combinedRowIds.GetValue(i);
                if (id.HasValue)
                {
                    float distance = vectorScores.TryGetValue(id.Value, out var d) ? d : _fill;
                    float ftsScore = ftsScores.TryGetValue(id.Value, out var s) ? s : _fill;

                    // Invert distance to similarity: vectorScore = 1 - distance
                    float vectorScore = 1f - distance;
                    relevanceScores[i] = 1f - (_weight * vectorScore + (1f - _weight) * ftsScore);
                }
            }

            // Append relevance score and sort
            var result = RerankerHelpers.AppendColumn(combined, RerankerHelpers.RelevanceScoreColumn, relevanceScores);
            result = RerankerHelpers.SortByDescending(result, RerankerHelpers.RelevanceScoreColumn);
            result = RerankerHelpers.KeepRelevanceScore(result, _returnScore);

            return result;
        }

        /// <inheritdoc/>
        public Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults)
        {
            throw new NotSupportedException(
                $"{nameof(LinearCombinationReranker)} does not support RerankFts.");
        }

        /// <inheritdoc/>
        public Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults)
        {
            throw new NotSupportedException(
                $"{nameof(LinearCombinationReranker)} does not support RerankVector.");
        }
    }
}