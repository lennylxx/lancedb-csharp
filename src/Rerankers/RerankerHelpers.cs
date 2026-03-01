namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Arrow;
    using Apache.Arrow.Types;
    /// <summary>
    /// Helper utilities for reranker implementations.
    /// </summary>
    public static class RerankerHelpers
    {
        /// <summary>
        /// The column name used for row IDs in LanceDB query results.
        /// </summary>
        public const string RowIdColumn = "_rowid";

        /// <summary>
        /// The column name used for relevance scores produced by rerankers.
        /// </summary>
        public const string RelevanceScoreColumn = "_relevance_score";

        /// <summary>
        /// The column name used for vector distance scores.
        /// </summary>
        public const string DistanceColumn = "_distance";

        /// <summary>
        /// The column name used for FTS scores.
        /// </summary>
        public const string ScoreColumn = "_score";

        /// <summary>
        /// Merges results from vector and FTS searches, deduplicating by the
        /// <c>_rowid</c> column. The first occurrence of each row ID is kept.
        /// </summary>
        /// <param name="vectorResults">The results from the vector search.</param>
        /// <param name="ftsResults">The results from the full-text search.</param>
        /// <returns>A merged <see cref="RecordBatch"/> with duplicates removed.</returns>
        /// <exception cref="ArgumentException">
        /// Thrown if the <c>_rowid</c> column is not found in the results.
        /// </exception>
        public static RecordBatch MergeResults(RecordBatch vectorResults, RecordBatch ftsResults)
        {
            if (vectorResults.Length == 0)
            {
                return ftsResults;
            }
            if (ftsResults.Length == 0)
            {
                return vectorResults;
            }

            // Build a common schema from columns present in both result sets
            var commonFields = new List<Field>();
            foreach (var field in vectorResults.Schema.FieldsList)
            {
                if (ftsResults.Schema.GetFieldIndex(field.Name) >= 0)
                {
                    commonFields.Add(field);
                }
            }
            var commonSchema = new Schema(commonFields, null);

            var rowIdIndex = GetColumnIndex(commonSchema, RowIdColumn);
            var vecRowIdColIndex = GetColumnIndex(vectorResults.Schema, RowIdColumn);
            var ftsRowIdColIndex = GetColumnIndex(ftsResults.Schema, RowIdColumn);

            var vectorRowIds = (UInt64Array)vectorResults.Column(vecRowIdColIndex);
            var ftsRowIds = (UInt64Array)ftsResults.Column(ftsRowIdColIndex);

            var seenIds = new HashSet<ulong>();
            var keepIndices = new List<int>();

            // Keep all vector results, tracking their row IDs
            for (int i = 0; i < vectorRowIds.Length; i++)
            {
                var id = vectorRowIds.GetValue(i);
                if (id.HasValue)
                {
                    seenIds.Add(id.Value);
                }
                keepIndices.Add(i);
            }

            // Track which FTS rows to add (not already seen)
            var ftsKeepIndices = new List<int>();
            for (int i = 0; i < ftsRowIds.Length; i++)
            {
                var id = ftsRowIds.GetValue(i);
                if (id.HasValue && seenIds.Add(id.Value))
                {
                    ftsKeepIndices.Add(i);
                }
            }

            // Build combined columns using only common fields
            int totalRows = keepIndices.Count + ftsKeepIndices.Count;
            var builders = new List<IArrowArray>();

            foreach (var field in commonFields)
            {
                var vectorCol = vectorResults.Column(GetColumnIndex(vectorResults.Schema, field.Name));
                var ftsCol = ftsResults.Column(GetColumnIndex(ftsResults.Schema, field.Name));

                builders.Add(ConcatFilteredColumns(vectorCol, keepIndices, ftsCol, ftsKeepIndices, field.DataType, totalRows));
            }

            return new RecordBatch(commonSchema, builders, totalRows);
        }

        /// <summary>
        /// Sorts a <see cref="RecordBatch"/> by the specified float column in descending order.
        /// </summary>
        internal static RecordBatch SortByDescending(RecordBatch batch, string columnName)
        {
            var colIndex = GetColumnIndex(batch.Schema, columnName);
            var scores = (FloatArray)batch.Column(colIndex);

            // Create index array and sort by score descending
            var indices = Enumerable.Range(0, batch.Length)
                .OrderByDescending(i => scores.GetValue(i) ?? float.MinValue)
                .ToArray();

            return ReorderBatch(batch, indices);
        }

        /// <summary>
        /// Reorders a <see cref="RecordBatch"/> by the given index permutation.
        /// </summary>
        internal static RecordBatch ReorderBatch(RecordBatch batch, int[] indices)
        {
            var columns = new List<IArrowArray>();
            for (int col = 0; col < batch.Schema.FieldsList.Count; col++)
            {
                columns.Add(TakeIndices(batch.Column(col), indices, batch.Schema.FieldsList[col].DataType));
            }
            return new RecordBatch(batch.Schema, columns, indices.Length);
        }

        internal static int GetColumnIndex(Schema schema, string columnName)
        {
            var index = schema.GetFieldIndex(columnName);
            if (index < 0)
            {
                throw new ArgumentException(
                    $"Expected column '{columnName}' not found. " +
                    $"Found columns: [{string.Join(", ", schema.FieldsList.Select(f => f.Name))}]");
            }
            return index;
        }

        private static IArrowArray TakeIndices(IArrowArray array, int[] indices, IArrowType dataType)
        {
            switch (dataType)
            {
                case UInt64Type:
                {
                    var src = (UInt64Array)array;
                    var builder = new UInt64Array.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case FloatType:
                {
                    var src = (FloatArray)array;
                    var builder = new FloatArray.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case StringType:
                {
                    var src = (StringArray)array;
                    var builder = new StringArray.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetString(i);
                        if (val != null)
                        {
                            builder.Append(val);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case Int32Type:
                {
                    var src = (Int32Array)array;
                    var builder = new Int32Array.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case Int64Type:
                {
                    var src = (Int64Array)array;
                    var builder = new Int64Array.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case DoubleType:
                {
                    var src = (DoubleArray)array;
                    var builder = new DoubleArray.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                case BooleanType:
                {
                    var src = (BooleanArray)array;
                    var builder = new BooleanArray.Builder();
                    foreach (var i in indices)
                    {
                        var val = src.GetValue(i);
                        if (val.HasValue)
                        {
                            builder.Append(val.Value);
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    return builder.Build();
                }
                default:
                    throw new NotSupportedException(
                        $"Reranker does not support Arrow type '{dataType.Name}'. " +
                        $"Supported types: UInt64, Float, String, Int32, Int64, Double, Boolean.");
            }
        }

        private static IArrowArray ConcatFilteredColumns(
            IArrowArray first, List<int> firstIndices,
            IArrowArray second, List<int> secondIndices,
            IArrowType dataType, int totalRows)
        {
            switch (dataType)
            {
                case UInt64Type:
                {
                    var src1 = (UInt64Array)first;
                    var src2 = (UInt64Array)second;
                    var builder = new UInt64Array.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case FloatType:
                {
                    var src1 = (FloatArray)first;
                    var src2 = (FloatArray)second;
                    var builder = new FloatArray.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case StringType:
                {
                    var src1 = (StringArray)first;
                    var src2 = (StringArray)second;
                    var builder = new StringArray.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetString(idx);
                        if (val != null) { builder.Append(val); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetString(idx);
                        if (val != null) { builder.Append(val); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case Int32Type:
                {
                    var src1 = (Int32Array)first;
                    var src2 = (Int32Array)second;
                    var builder = new Int32Array.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case Int64Type:
                {
                    var src1 = (Int64Array)first;
                    var src2 = (Int64Array)second;
                    var builder = new Int64Array.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case DoubleType:
                {
                    var src1 = (DoubleArray)first;
                    var src2 = (DoubleArray)second;
                    var builder = new DoubleArray.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case BooleanType:
                {
                    var src1 = (BooleanArray)first;
                    var src2 = (BooleanArray)second;
                    var builder = new BooleanArray.Builder();
                    foreach (var idx in firstIndices)
                    {
                        var val = src1.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    foreach (var idx in secondIndices)
                    {
                        var val = src2.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                default:
                    throw new NotSupportedException(
                        $"Reranker does not support Arrow type '{dataType.Name}'. " +
                        $"Supported types: UInt64, Float, String, Int32, Int64, Double, Boolean.");
            }
        }

        /// <summary>
        /// Appends a float column to a <see cref="RecordBatch"/>.
        /// </summary>
        internal static RecordBatch AppendColumn(RecordBatch batch, string columnName, float[] values)
        {
            var fields = batch.Schema.FieldsList.ToList();
            fields.Add(new Field(columnName, FloatType.Default, false));
            var schema = new Schema(fields, batch.Schema.Metadata);

            var columns = new List<IArrowArray>();
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                columns.Add(batch.Column(i));
            }
            columns.Add(new FloatArray.Builder().AppendRange(values).Build());

            return new RecordBatch(schema, columns, batch.Length);
        }
    }
}
