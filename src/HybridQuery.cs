namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
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

        // Shared config (applied to both sub-queries)
        private string? _predicate;
        private bool _fastSearch;
        private bool _postfilter;

        // Final result config (applied after reranking)
        private string? _selectJson;
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
            _selectJson = source._selectJson;
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
            _selectJson = source._selectJson;
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
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Rerank(IReranker reranker)
        {
            _reranker = reranker;
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
            _selectJson = JsonSerializer.Serialize(columns);
            return this;
        }

        /// <summary>
        /// Set the columns to return, with SQL expression transformations.
        /// </summary>
        /// <param name="columns">A dictionary mapping output column names to SQL expressions.</param>
        /// <returns>This <see cref="HybridQuery"/> instance for method chaining.</returns>
        public HybridQuery Select(Dictionary<string, string> columns)
        {
            _selectJson = JsonSerializer.Serialize(columns);
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
        /// </remarks>
        /// <param name="timeout">Optional maximum time for each sub-query to run.</param>
        /// <returns>The merged and reranked results as a RecordBatch.</returns>
        public async Task<RecordBatch> ToArrow(TimeSpan? timeout = null)
        {
            // Execute FTS sub-query
            using var ftsSubQuery = new Query(_tablePtr);
            ftsSubQuery.FullTextSearch(_ftsQuery, _ftsColumns);
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

            // Merge with reranker
            var merged = await _reranker.RerankHybrid(_ftsQuery, vecResults, ftsResults)
                .ConfigureAwait(false);

            // Apply final limit
            if (_limit.HasValue && merged.Length > _limit.Value)
            {
                merged = SliceBatch(merged, 0, _limit.Value);
            }

            // Apply final offset
            if (_offset.HasValue && _offset.Value > 0 && merged.Length > _offset.Value)
            {
                merged = SliceBatch(merged, _offset.Value, merged.Length - _offset.Value);
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

        private static RecordBatch SliceBatch(RecordBatch batch, int offset, int length)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = ((Apache.Arrow.Array)batch.Column(i)).Slice(offset, length);
            }
            return new RecordBatch(batch.Schema, arrays, length);
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
