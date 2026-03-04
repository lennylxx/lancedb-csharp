namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Arrow;

    /// <summary>
    /// A builder for hybrid queries that combine vector search and full-text search.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A hybrid query executes both a vector search and a full-text search independently,
    /// then merges the results using a reranker. By default, the
    /// <see cref="RRFReranker"/> (Reciprocal Rank Fusion) is used.
    /// </para>
    /// <para>
    /// This class is created via:
    /// <list type="bullet">
    /// <item><description><see cref="FTSQuery.NearestTo"/> — from an FTS query</description></item>
    /// <item><description><see cref="VectorQuery.NearestToText"/> — from a vector query</description></item>
    /// <item><description><see cref="Table.HybridSearch"/> — directly from a table</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public class HybridQuery
    {
        private readonly IntPtr _tablePtr;
        private readonly string _ftsQuery;
        private string[]? _ftsColumns;
        private readonly double[] _vector;
        private IReranker _reranker = new RRFReranker();
        private string _normalize = "score";

        // Shared config (applied to both sub-queries)
        private string? _predicate;
        private bool _fastSearch;
        private bool _postfilter;

        // Final result config (applied after reranking)
        private IReadOnlyList<string>? _selectColumns;
        private int? _limit;
        private int? _offset;

        // Vector-specific config
        private string? _vectorColumn;
        private int? _distanceType;
        private int? _nprobes;
        private int? _refineFactor;
        private bool _bypassVectorIndex;
        private int? _ef;
        private int? _minimumNprobes;
        private int? _maximumNprobes;

        /// <summary>
        /// Creates a HybridQuery from an FTSQuery and a vector.
        /// </summary>
        internal HybridQuery(FTSQuery source, double[] vector)
        {
            _tablePtr = source._tablePtr;
            _ftsQuery = source._fullTextSearchQuery!;
            _ftsColumns = source._fullTextSearchColumns;
            _vector = vector;
            _predicate = source._predicate;
            _selectColumns = source._selectColumns;
            if (_selectColumns == null && source._selectExpressions != null)
            {
                _selectColumns = new List<string>(source._selectExpressions.Keys);
            }
            _limit = source._limit;
            _offset = source._offset;
            _fastSearch = source._fastSearch;
            _postfilter = source._postfilter;
        }

        /// <summary>
        /// Creates a HybridQuery from a VectorQuery and FTS text.
        /// </summary>
        internal HybridQuery(VectorQuery source, string ftsQuery, string[]? ftsColumns)
        {
            _tablePtr = source._tablePtr;
            _ftsQuery = ftsQuery;
            _ftsColumns = ftsColumns;
            _vector = source._vector;
            _predicate = source._predicate;
            _selectColumns = source._selectColumns;
            if (_selectColumns == null && source._selectExpressions != null)
            {
                _selectColumns = new List<string>(source._selectExpressions.Keys);
            }
            _limit = source._limit;
            _offset = source._offset;
            _fastSearch = source._fastSearch;
            _postfilter = source._postfilter;
            CopyVectorParams(source);
        }

        /// <summary>
        /// Creates a HybridQuery directly from a table handle.
        /// </summary>
        internal HybridQuery(IntPtr tablePtr, string ftsQuery, double[] vector,
            string[]? ftsColumns = null)
        {
            _tablePtr = tablePtr;
            _ftsQuery = ftsQuery;
            _ftsColumns = ftsColumns;
            _vector = vector;
        }

        private void CopyVectorParams(VectorQuery source)
        {
            _vectorColumn = source._column;
            _distanceType = source._distanceType;
            _nprobes = source._nprobes;
            _refineFactor = source._refineFactor;
            _bypassVectorIndex = source._bypassVectorIndex;
            _ef = source._ef;
            _minimumNprobes = source._minimumNprobes;
            _maximumNprobes = source._maximumNprobes;
        }

        /// <summary>
        /// Set the reranker to use for merging vector and FTS results.
        /// </summary>
        /// <remarks>
        /// The default reranker is <see cref="RRFReranker"/> with k=60.
        /// </remarks>
        /// <param name="reranker">The reranker to use.</param>
        /// <param name="normalize">The method to normalize the scores. Can be "score" or "rank".
        /// If "score" (the default), the raw scores are normalized directly via min-max scaling.
        /// If "rank", the scores are first converted to ordinal ranks and then normalized.
        /// Rank-based normalization is more robust to outliers in the score distribution.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="normalize"/> is not "score" or "rank".
        /// </exception>
        public HybridQuery Rerank(IReranker reranker, string normalize = "score")
        {
            if (normalize != "score" && normalize != "rank")
            {
                throw new ArgumentException("normalize must be 'score' or 'rank'.", nameof(normalize));
            }
            _reranker = reranker;
            _normalize = normalize;
            return this;
        }

        /// <summary>
        /// Only return rows which match the given filter.
        /// </summary>
        /// <param name="predicate">A SQL filter expression.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Where(string predicate)
        {
            _predicate = predicate;
            return this;
        }

        /// <summary>
        /// Set the columns to return.
        /// </summary>
        /// <param name="columns">A list of column names to return.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Select(IReadOnlyList<string> columns)
        {
            _selectColumns = columns;
            return this;
        }

        /// <summary>
        /// Set the columns to return, with SQL expression transformations.
        /// </summary>
        /// <param name="columns">A dictionary mapping output column names to SQL expressions.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Select(Dictionary<string, string> columns)
        {
            _selectColumns = new List<string>(columns.Keys);
            return this;
        }

        /// <summary>
        /// Set the maximum number of results to return.
        /// </summary>
        /// <param name="limit">The maximum number of results to return.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Limit(int limit)
        {
            _limit = limit;
            return this;
        }

        /// <summary>
        /// Set the offset for the results.
        /// </summary>
        /// <param name="offset">The number of results to skip.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Offset(int offset)
        {
            _offset = offset;
            return this;
        }

        /// <summary>
        /// Skip searching unindexed data.
        /// </summary>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery FastSearch()
        {
            _fastSearch = true;
            return this;
        }

        /// <summary>
        /// Apply filtering after the search instead of before.
        /// </summary>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Postfilter()
        {
            _postfilter = true;
            return this;
        }

        /// <summary>
        /// Set the vector column to query.
        /// </summary>
        /// <param name="column">The name of the vector column.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Column(string column)
        {
            _vectorColumn = column;
            return this;
        }

        /// <summary>
        /// Set the distance metric to use for the vector search.
        /// </summary>
        /// <param name="distanceType">The distance metric to use.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery DistanceType(DistanceType distanceType)
        {
            _distanceType = (int)distanceType;
            return this;
        }

        /// <summary>
        /// Set the number of probes to use for an IVF index search.
        /// </summary>
        /// <param name="nprobes">The number of probes to use.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Nprobes(int nprobes)
        {
            _nprobes = nprobes;
            return this;
        }

        /// <summary>
        /// Set a refine factor to use for vector search.
        /// </summary>
        /// <param name="refineFactor">The refine factor to use.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery RefineFactor(int refineFactor)
        {
            _refineFactor = refineFactor;
            return this;
        }

        /// <summary>
        /// Skip the vector index and perform a brute-force search.
        /// </summary>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery BypassVectorIndex()
        {
            _bypassVectorIndex = true;
            return this;
        }

        /// <summary>
        /// Set the HNSW ef search parameter.
        /// </summary>
        /// <param name="ef">The ef search parameter.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Ef(int ef)
        {
            _ef = ef;
            return this;
        }

        /// <summary>
        /// Set the minimum number of probes for an IVF index search.
        /// </summary>
        /// <param name="n">The minimum number of probes.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery MinimumNprobes(int n)
        {
            _minimumNprobes = n;
            return this;
        }

        /// <summary>
        /// Set the maximum number of probes for an IVF index search.
        /// </summary>
        /// <param name="n">The maximum number of probes, or 0 for unlimited.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery MaximumNprobes(int n)
        {
            _maximumNprobes = n;
            return this;
        }

        /// <summary>
        /// Set the FTS columns to search.
        /// </summary>
        /// <param name="columns">The column names to search.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery FtsColumns(string[] columns)
        {
            _ftsColumns = columns;
            return this;
        }

        /// <summary>
        /// Execute the hybrid query and return the results as an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This executes both a vector search and a full-text search independently,
        /// then merges the results using the configured reranker. The merged results
        /// include a <c>_relevance_score</c> column from the reranker.
        /// </para>
        /// <para>
        /// Before reranking, <c>_distance</c> and <c>_score</c> columns are normalized
        /// to the [0, 1] range using min-max normalization, matching the Python SDK behavior.
        /// Original values are restored after reranking.
        /// </para>
        /// </remarks>
        /// <param name="timeout">Optional maximum time for each sub-query to run.</param>
        /// <returns>The merged and reranked results as a RecordBatch.</returns>
        public async Task<RecordBatch> ToArrow(TimeSpan? timeout = null)
        {
            // Execute FTS sub-query
            using var ftsSubQuery = new Query(_tablePtr);
            ftsSubQuery._fullTextSearchQuery = _ftsQuery;
            ftsSubQuery._fullTextSearchColumns = _ftsColumns;
            ftsSubQuery.WithRowId();
            ApplySharedConfig(ftsSubQuery);
            var ftsResults = await ftsSubQuery.ToArrow(timeout).ConfigureAwait(false);

            // Execute vector sub-query
            using var vecBaseQuery = new Query(_tablePtr);
            ApplySharedConfig(vecBaseQuery);
            var vecSubQuery = vecBaseQuery.NearestTo(_vector);
            vecSubQuery.WithRowId();
            ApplyVectorConfig(vecSubQuery);
            var vecResults = await vecSubQuery.ToArrow(timeout).ConfigureAwait(false);

            // If normalize="rank", convert scores to ordinal ranks first.
            if (_normalize == "rank")
            {
                vecResults = RankColumn(vecResults, "_distance", ascending: true);
                ftsResults = RankColumn(ftsResults, "_score", ascending: true);
            }

            // Normalize scores to [0,1] before reranking (matching Python behavior).
            // Save originals so we can restore them after reranking.
            FloatArray? originalDistances = null;
            UInt64Array? originalDistanceRowIds = null;
            FloatArray? originalScores = null;
            UInt64Array? originalScoreRowIds = null;

            if (vecResults.Length > 0)
            {
                var distIdx = vecResults.Schema.GetFieldIndex("_distance");
                if (distIdx >= 0)
                {
                    originalDistances = (FloatArray)vecResults.Column(distIdx);
                    originalDistanceRowIds = (UInt64Array)vecResults.Column(
                        vecResults.Schema.GetFieldIndex("_rowid"));
                    vecResults = ReplaceColumn(vecResults, distIdx,
                        NormalizeScores(originalDistances));
                }
            }

            if (ftsResults.Length > 0)
            {
                var scoreIdx = ftsResults.Schema.GetFieldIndex("_score");
                if (scoreIdx >= 0)
                {
                    originalScores = (FloatArray)ftsResults.Column(scoreIdx);
                    originalScoreRowIds = (UInt64Array)ftsResults.Column(
                        ftsResults.Schema.GetFieldIndex("_rowid"));
                    ftsResults = ReplaceColumn(ftsResults, scoreIdx,
                        NormalizeScores(originalScores));
                }
            }

            // Merge with reranker
            var merged = await _reranker.RerankHybrid(_ftsQuery, vecResults, ftsResults)
                .ConfigureAwait(false);

            // Restore original _distance and _score values (reranker saw normalized copies)
            merged = RestoreColumn(merged, "_distance", originalDistances, originalDistanceRowIds);
            merged = RestoreColumn(merged, "_score", originalScores, originalScoreRowIds);

            // Apply final offset (before limit — SQL semantics)
            if (_offset.HasValue && _offset.Value > 0 && merged.Length > _offset.Value)
            {
                merged = SliceBatch(merged, _offset.Value, merged.Length - _offset.Value);
            }

            // Apply final limit
            if (_limit.HasValue && merged.Length > _limit.Value)
            {
                merged = SliceBatch(merged, 0, _limit.Value);
            }

            // Apply column projection
            if (_selectColumns != null)
            {
                merged = ApplySelect(merged, _selectColumns);
            }

            return merged;
        }

        /// <summary>
        /// Execute the hybrid query and return the results as a list of dictionaries.
        /// </summary>
        /// <param name="timeout">Optional maximum time for each sub-query to run.</param>
        /// <returns>A list of dictionaries, one per row.</returns>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ToList(TimeSpan? timeout = null)
        {
            var batch = await ToArrow(timeout).ConfigureAwait(false);
            var result = new List<Dictionary<string, object?>>(batch.Length);
            for (int row = 0; row < batch.Length; row++)
            {
                var dict = new Dictionary<string, object?>(batch.ColumnCount);
                for (int col = 0; col < batch.ColumnCount; col++)
                {
                    string name = batch.Schema.FieldsList[col].Name;
                    dict[name] = GetArrowValue(batch.Column(col), row);
                }
                result.Add(dict);
            }
            return result;
        }

        private void ApplySharedConfig<T>(QueryBase<T> query) where T : QueryBase<T>
        {
            if (_predicate != null)
            {
                query.Where(_predicate);
            }
            if (_fastSearch)
            {
                query.FastSearch();
            }
            if (_postfilter)
            {
                query.Postfilter();
            }
        }

        private void ApplyVectorConfig(VectorQuery query)
        {
            if (_vectorColumn != null)
            {
                query.Column(_vectorColumn);
            }
            if (_distanceType.HasValue)
            {
                query.DistanceType((DistanceType)_distanceType.Value);
            }
            if (_nprobes.HasValue)
            {
                query.Nprobes(_nprobes.Value);
            }
            if (_refineFactor.HasValue)
            {
                query.RefineFactor(_refineFactor.Value);
            }
            if (_bypassVectorIndex)
            {
                query.BypassVectorIndex();
            }
            if (_ef.HasValue)
            {
                query.Ef(_ef.Value);
            }
            if (_minimumNprobes.HasValue)
            {
                query.MinimumNprobes(_minimumNprobes.Value);
            }
            if (_maximumNprobes.HasValue)
            {
                query.MaximumNprobes(_maximumNprobes.Value);
            }
        }

        private static RecordBatch ApplySelect(RecordBatch batch, IReadOnlyList<string> columns)
        {
            var keep = new HashSet<string>(columns);
            // Always include _relevance_score from reranker
            keep.Add("_relevance_score");

            var fields = new List<Field>();
            var arrays = new List<IArrowArray>();
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                if (keep.Contains(batch.Schema.FieldsList[i].Name))
                {
                    fields.Add(batch.Schema.FieldsList[i]);
                    arrays.Add(batch.Column(i));
                }
            }
            var schema = new Schema(fields, batch.Schema.Metadata);
            return new RecordBatch(schema, arrays, batch.Length);
        }

        private static RecordBatch SliceBatch(RecordBatch batch, int offset, int length)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = ((Apache.Arrow.Array)batch.Column(i)).Slice(offset, length);
            }
            return new RecordBatch(batch.Schema, arrays, length);
        }

        /// <summary>
        /// Replaces a float column with ordinal ranks (1-based).
        /// Matches Python's <c>_rank</c>: sorts by the column, assigns ranks 1,2,3...
        /// </summary>
        private static RecordBatch RankColumn(RecordBatch batch, string columnName, bool ascending)
        {
            if (batch.Length == 0)
            {
                return batch;
            }

            var colIdx = batch.Schema.GetFieldIndex(columnName);
            if (colIdx < 0)
            {
                return batch;
            }

            var values = (FloatArray)batch.Column(colIdx);
            int n = values.Length;

            // Build sort indices (argsort)
            var indices = new int[n];
            for (int i = 0; i < n; i++)
            {
                indices[i] = i;
            }
            System.Array.Sort(indices, (a, b) =>
            {
                var va = values.GetValue(a) ?? float.MaxValue;
                var vb = values.GetValue(b) ?? float.MaxValue;
                return ascending ? va.CompareTo(vb) : vb.CompareTo(va);
            });

            // Assign ranks: ranks[sortedIndex] = rank (1-based)
            var ranks = new float[n];
            for (int rank = 0; rank < n; rank++)
            {
                ranks[indices[rank]] = rank + 1;
            }

            var builder = new FloatArray.Builder();
            for (int i = 0; i < n; i++)
            {
                builder.Append(ranks[i]);
            }
            return ReplaceColumn(batch, colIdx, builder.Build());
        }

        /// <summary>
        /// Min-max normalizes float scores to [0, 1].
        /// Matches Python's <c>_normalize_scores</c>: if range is 0 and max != 0,
        /// all values become 0; if range is 0 and max == 0, values are unchanged.
        /// </summary>
        private static FloatArray NormalizeScores(FloatArray scores)
        {
            if (scores.Length == 0)
            {
                return scores;
            }

            float min = float.MaxValue, max = float.MinValue;
            for (int i = 0; i < scores.Length; i++)
            {
                var val = scores.GetValue(i);
                if (val.HasValue)
                {
                    if (val.Value < min) { min = val.Value; }
                    if (val.Value > max) { max = val.Value; }
                }
            }

            float range = max - min;
            var builder = new FloatArray.Builder();
            for (int i = 0; i < scores.Length; i++)
            {
                var val = scores.GetValue(i);
                if (!val.HasValue)
                {
                    builder.AppendNull();
                }
                else if (range != 0f)
                {
                    builder.Append((val.Value - min) / range);
                }
                else if (max != 0f)
                {
                    builder.Append(val.Value - min);
                }
                else
                {
                    builder.Append(val.Value);
                }
            }
            return builder.Build();
        }

        private static RecordBatch ReplaceColumn(RecordBatch batch, int columnIndex, IArrowArray newColumn)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = i == columnIndex ? newColumn : batch.Column(i);
            }
            return new RecordBatch(batch.Schema, arrays, batch.Length);
        }

        /// <summary>
        /// Restores original pre-normalization values in a column by matching _rowid.
        /// Equivalent to Python's <c>pc.index_in</c> + <c>pc.take</c> + <c>set_column</c>.
        /// </summary>
        private static RecordBatch RestoreColumn(RecordBatch merged, string columnName,
            FloatArray? originalValues, UInt64Array? originalRowIds)
        {
            if (originalValues == null || originalRowIds == null)
            {
                return merged;
            }

            var colIdx = merged.Schema.GetFieldIndex(columnName);
            if (colIdx < 0)
            {
                return merged;
            }

            var mergedRowIdIdx = merged.Schema.GetFieldIndex("_rowid");
            if (mergedRowIdIdx < 0)
            {
                return merged;
            }

            // Build rowid → original value lookup
            var lookup = new Dictionary<ulong, float>(originalRowIds.Length);
            for (int i = 0; i < originalRowIds.Length; i++)
            {
                var id = originalRowIds.GetValue(i);
                var val = originalValues.GetValue(i);
                if (id.HasValue && val.HasValue)
                {
                    lookup[id.Value] = val.Value;
                }
            }

            var mergedRowIds = (UInt64Array)merged.Column(mergedRowIdIdx);
            var currentValues = (FloatArray)merged.Column(colIdx);
            var builder = new FloatArray.Builder();
            for (int i = 0; i < merged.Length; i++)
            {
                var rowId = mergedRowIds.GetValue(i);
                if (rowId.HasValue && lookup.TryGetValue(rowId.Value, out var original))
                {
                    builder.Append(original);
                }
                else
                {
                    var val = currentValues.GetValue(i);
                    if (val.HasValue)
                    {
                        builder.Append(val.Value);
                    }
                    else
                    {
                        builder.AppendNull();
                    }
                }
            }

            return ReplaceColumn(merged, colIdx, builder.Build());
        }

        private static object? GetArrowValue(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return null;
            }
            return array switch
            {
                Int8Array a => a.GetValue(index),
                Int16Array a => a.GetValue(index),
                Int32Array a => a.GetValue(index),
                Int64Array a => a.GetValue(index),
                UInt8Array a => a.GetValue(index),
                UInt16Array a => a.GetValue(index),
                UInt32Array a => a.GetValue(index),
                UInt64Array a => a.GetValue(index),
                FloatArray a => a.GetValue(index),
                DoubleArray a => a.GetValue(index),
                StringArray a => a.GetString(index),
                BooleanArray a => a.GetValue(index),
                _ => array.GetType().Name,
            };
        }
    }
}
