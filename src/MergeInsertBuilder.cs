namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Arrow;

    /// <summary>
    /// A builder used to create and run a merge insert operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A merge insert operation allows rows to be inserted, updated, or deleted
    /// based on matching criteria. This can be used to implement upsert, sync,
    /// or incremental update patterns.
    /// </para>
    /// <para>
    /// Use the builder methods to configure the behavior:
    /// - <see cref="WhenMatchedUpdateAll"/> — Update matched rows with source data.
    /// - <see cref="WhenNotMatchedInsertAll"/> — Insert source rows that don't match.
    /// - <see cref="WhenNotMatchedBySourceDelete"/> — Delete target rows not in source.
    /// </para>
    /// <para>
    /// At least one of these behaviors must be configured before calling
    /// <see cref="Execute"/>.
    /// </para>
    /// </remarks>
    public class MergeInsertBuilder
    {
        private readonly Table _table;
        private readonly IReadOnlyList<string> _onColumns;
        private bool _whenMatchedUpdateAll;
        private string? _whenMatchedUpdateAllFilter;
        private bool _whenNotMatchedInsertAll;
        private bool _whenNotMatchedBySourceDelete;
        private string? _whenNotMatchedBySourceDeleteFilter;
        private bool _useIndex = true;
        private TimeSpan? _timeout;

        internal MergeInsertBuilder(Table table, IReadOnlyList<string> onColumns)
        {
            _table = table;
            _onColumns = onColumns;
        }

        /// <summary>
        /// Rows that exist in both the source table (new data) and
        /// the target table (old data) will be updated, replacing
        /// the old row with the corresponding matching row.
        /// </summary>
        /// <remarks>
        /// If there are multiple matches then the behavior is undefined.
        /// Currently this causes multiple copies of the row to be created
        /// but that behavior is subject to change.
        /// </remarks>
        /// <param name="condition">
        /// An optional SQL filter condition. Only matched rows that satisfy
        /// the condition will be updated. Use the prefix <c>target.</c> to refer to
        /// rows in the target table and <c>source.</c> for the source table.
        /// For example: <c>"target.last_update &lt; source.last_update"</c>.
        /// </param>
        /// <returns>This builder for chaining.</returns>
        public MergeInsertBuilder WhenMatchedUpdateAll(string? condition = null)
        {
            _whenMatchedUpdateAll = true;
            _whenMatchedUpdateAllFilter = condition;
            return this;
        }

        /// <summary>
        /// Rows that exist only in the source table (new data) will be
        /// inserted into the target table.
        /// </summary>
        /// <returns>This builder for chaining.</returns>
        public MergeInsertBuilder WhenNotMatchedInsertAll()
        {
            _whenNotMatchedInsertAll = true;
            return this;
        }

        /// <summary>
        /// Rows that exist only in the target table (old data) will be
        /// deleted. An optional condition can limit what data is deleted.
        /// </summary>
        /// <param name="condition">
        /// An optional SQL filter condition. If <c>null</c>, all unmatched
        /// target rows are deleted.
        /// </param>
        /// <returns>This builder for chaining.</returns>
        public MergeInsertBuilder WhenNotMatchedBySourceDelete(string? condition = null)
        {
            _whenNotMatchedBySourceDelete = true;
            _whenNotMatchedBySourceDeleteFilter = condition;
            return this;
        }

        /// <summary>
        /// Controls whether to use indexes for the merge operation.
        /// </summary>
        /// <remarks>
        /// When set to <c>true</c> (the default), the operation will use an index if
        /// available on the join key for improved performance. When set to <c>false</c>,
        /// it forces a full table scan even if an index exists. This can be useful for
        /// benchmarking or when the query optimizer chooses a suboptimal path.
        /// </remarks>
        /// <param name="useIndex">Whether to use indices for the merge operation.</param>
        /// <returns>This builder for chaining.</returns>
        public MergeInsertBuilder UseIndex(bool useIndex)
        {
            _useIndex = useIndex;
            return this;
        }

        /// <summary>
        /// Set a timeout for the merge operation.
        /// </summary>
        /// <remarks>
        /// If there is a concurrent write that conflicts with the merge, it will be
        /// retried. The timeout is applied across all retry attempts. If the timeout
        /// is reached, the operation will fail.
        /// </remarks>
        /// <param name="timeout">The maximum time to allow for the merge operation.</param>
        /// <returns>This builder for chaining.</returns>
        public MergeInsertBuilder Timeout(TimeSpan timeout)
        {
            _timeout = timeout;
            return this;
        }

        /// <summary>
        /// Execute the merge insert operation with the provided data.
        /// </summary>
        /// <param name="data">
        /// The new data to merge. The schema must match the target table.
        /// </param>
        public async Task Execute(IReadOnlyList<RecordBatch> data)
        {
            await _table.ExecuteMergeInsert(
                _onColumns,
                _whenMatchedUpdateAll, _whenMatchedUpdateAllFilter,
                _whenNotMatchedInsertAll,
                _whenNotMatchedBySourceDelete, _whenNotMatchedBySourceDeleteFilter,
                data, _useIndex, _timeout).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute the merge insert operation with a single RecordBatch.
        /// </summary>
        /// <param name="data">The new data to merge.</param>
        public Task Execute(RecordBatch data)
        {
            return Execute(new[] { data });
        }
    }
}
