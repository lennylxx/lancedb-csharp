namespace lancedb
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Arrow;

    /// <summary>
    /// A builder for take operations, created by <see cref="Table.TakeOffsets"/>
    /// or <see cref="Table.TakeRowIds"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A take query retrieves specific rows from the table by offset position or
    /// row ID. Use builder methods to configure which columns to return and whether
    /// to include the internal row ID.
    /// </para>
    /// <para>
    /// Call <see cref="ToArrow"/> to execute the take and return the results.
    /// </para>
    /// </remarks>
    public class TakeQuery
    {
        private readonly Table _table;
        private readonly ulong[] _ids;
        private readonly bool _isByRowId;
        private IReadOnlyList<string>? _columns;
        private bool _withRowId;

        internal TakeQuery(Table table, IReadOnlyList<ulong> ids, bool isByRowId)
        {
            _table = table;
            _ids = ids is ulong[] arr ? arr : ids.ToArray();
            _isByRowId = isByRowId;
        }

        /// <summary>
        /// Set the columns to return.
        /// </summary>
        /// <remarks>
        /// If this is not called then every column will be returned.
        /// </remarks>
        /// <param name="columns">A list of column names to return.</param>
        /// <returns>This <see cref="TakeQuery"/> instance for method chaining.</returns>
        public TakeQuery Select(IReadOnlyList<string> columns)
        {
            _columns = columns;
            return this;
        }

        /// <summary>
        /// Return the internal row ID in the results.
        /// </summary>
        /// <returns>This <see cref="TakeQuery"/> instance for method chaining.</returns>
        public TakeQuery WithRowId()
        {
            _withRowId = true;
            return this;
        }

        /// <summary>
        /// Execute the take and return the results as an Arrow <see cref="RecordBatch"/>.
        /// </summary>
        /// <returns>A <see cref="RecordBatch"/> containing the requested rows.</returns>
        public Task<RecordBatch> ToArrow()
        {
            return _table.ExecuteTake(_ids, _isByRowId, _columns, _withRowId);
        }
    }
}
