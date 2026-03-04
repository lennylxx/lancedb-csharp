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

            // Build a unified schema from columns in either result set (union, not intersection).
            // This matches Python's pa.concat_tables(promote_options="default").
            var unifiedFields = new List<Field>();
            foreach (var field in vectorResults.Schema.FieldsList)
            {
                unifiedFields.Add(field);
            }
            foreach (var field in ftsResults.Schema.FieldsList)
            {
                if (vectorResults.Schema.GetFieldIndex(field.Name) < 0)
                {
                    unifiedFields.Add(field);
                }
            }
            var unifiedSchema = new Schema(unifiedFields, null);

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

            // Build combined columns using the unified schema.
            // Columns missing from one side are null-filled.
            int totalRows = keepIndices.Count + ftsKeepIndices.Count;
            var builders = new List<IArrowArray>();

            foreach (var field in unifiedFields)
            {
                var vecIdx = vectorResults.Schema.GetFieldIndex(field.Name);
                var ftsIdx = ftsResults.Schema.GetFieldIndex(field.Name);

                if (vecIdx >= 0 && ftsIdx >= 0)
                {
                    builders.Add(ConcatFilteredColumns(
                        vectorResults.Column(vecIdx), keepIndices,
                        ftsResults.Column(ftsIdx), ftsKeepIndices,
                        field.DataType, totalRows));
                }
                else if (vecIdx >= 0)
                {
                    builders.Add(ConcatFilteredColumnsWithNulls(
                        vectorResults.Column(vecIdx), keepIndices,
                        ftsKeepIndices.Count, field.DataType, totalRows));
                }
                else
                {
                    builders.Add(ConcatFilteredColumnsWithNulls(
                        ftsResults.Column(ftsIdx), ftsKeepIndices,
                        keepIndices.Count, field.DataType, totalRows,
                        nullsFirst: true));
                }
            }

            return new RecordBatch(unifiedSchema, builders, totalRows);
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
                case FixedSizeListType fslType:
                {
                    var src = (FixedSizeListArray)array;
                    int listSize = fslType.ListSize;
                    var valueField = fslType.ValueField;
                    var builder = new FixedSizeListArray.Builder(valueField, listSize);
                    var indexList = new List<int>(indices);
                    AppendFixedListEntries(builder, src, indexList, listSize, valueField.DataType);
                    return builder.Build();
                }
                default:
                    throw new NotSupportedException(
                        $"Reranker does not support Arrow type '{dataType.Name}'. " +
                        $"Supported types: UInt64, Float, String, Int32, Int64, Double, Boolean, FixedSizeList.");
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
                case FixedSizeListType fslType:
                {
                    return ConcatFixedSizeList(
                        (FixedSizeListArray)first, firstIndices,
                        (FixedSizeListArray)second, secondIndices,
                        fslType);
                }
                default:
                    throw new NotSupportedException(
                        $"Reranker does not support Arrow type '{dataType.Name}'. " +
                        $"Supported types: UInt64, Float, String, Int32, Int64, Double, Boolean, FixedSizeList.");
            }
        }

        /// <summary>
        /// Concatenates values from one source with null padding for the other side.
        /// Used for columns that exist in only one of the two result sets.
        /// </summary>
        /// <param name="source">The array containing actual values.</param>
        /// <param name="sourceIndices">Indices to take from source.</param>
        /// <param name="nullCount">Number of null values for the other side.</param>
        /// <param name="dataType">The Arrow data type.</param>
        /// <param name="totalRows">Expected total row count.</param>
        /// <param name="nullsFirst">If true, nulls come before source values.</param>
        private static IArrowArray ConcatFilteredColumnsWithNulls(
            IArrowArray source, List<int> sourceIndices,
            int nullCount, IArrowType dataType, int totalRows,
            bool nullsFirst = false)
        {
            switch (dataType)
            {
                case FloatType:
                {
                    var src = (FloatArray)source;
                    var builder = new FloatArray.Builder();
                    if (nullsFirst)
                    {
                        for (int i = 0; i < nullCount; i++) { builder.AppendNull(); }
                    }
                    foreach (var idx in sourceIndices)
                    {
                        var val = src.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    if (!nullsFirst)
                    {
                        for (int i = 0; i < nullCount; i++) { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                case UInt64Type:
                {
                    var src = (UInt64Array)source;
                    var builder = new UInt64Array.Builder();
                    if (nullsFirst)
                    {
                        for (int i = 0; i < nullCount; i++) { builder.AppendNull(); }
                    }
                    foreach (var idx in sourceIndices)
                    {
                        var val = src.GetValue(idx);
                        if (val.HasValue) { builder.Append(val.Value); } else { builder.AppendNull(); }
                    }
                    if (!nullsFirst)
                    {
                        for (int i = 0; i < nullCount; i++) { builder.AppendNull(); }
                    }
                    return builder.Build();
                }
                default:
                {
                    // For other types, build a fully-null array of the correct length
                    return BuildNullArray(dataType, totalRows);
                }
            }
        }

        private static IArrowArray BuildNullArray(IArrowType dataType, int length)
        {
            switch (dataType)
            {
                case FloatType:
                {
                    var b = new FloatArray.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case UInt64Type:
                {
                    var b = new UInt64Array.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case StringType:
                {
                    var b = new StringArray.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case Int32Type:
                {
                    var b = new Int32Array.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case Int64Type:
                {
                    var b = new Int64Array.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case DoubleType:
                {
                    var b = new DoubleArray.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                case BooleanType:
                {
                    var b = new BooleanArray.Builder();
                    for (int i = 0; i < length; i++) { b.AppendNull(); }
                    return b.Build();
                }
                default:
                    throw new NotSupportedException(
                        $"Reranker does not support null-fill for Arrow type '{dataType.Name}'.");
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

        /// <summary>
        /// Appends a float column filled with NaN values to a <see cref="RecordBatch"/>.
        /// Used when one sub-query returns no results and return_score="all" needs a
        /// placeholder column for the missing side.
        /// </summary>
        internal static RecordBatch AppendNanColumn(RecordBatch batch, string columnName)
        {
            var nanValues = new float[batch.Length];
            for (int i = 0; i < nanValues.Length; i++)
            {
                nanValues[i] = float.NaN;
            }
            return AppendColumn(batch, columnName, nanValues);
        }

        private static IArrowArray ConcatFixedSizeList(
            FixedSizeListArray first, List<int> firstIndices,
            FixedSizeListArray second, List<int> secondIndices,
            FixedSizeListType fslType)
        {
            int listSize = fslType.ListSize;
            var valueField = fslType.ValueField;
            var builder = new FixedSizeListArray.Builder(valueField, listSize);

            AppendFixedListEntries(builder, first, firstIndices, listSize, valueField.DataType);
            AppendFixedListEntries(builder, second, secondIndices, listSize, valueField.DataType);
            return builder.Build();
        }

        private static void AppendFixedListEntries(
            FixedSizeListArray.Builder builder, FixedSizeListArray source,
            List<int> indices, int listSize, IArrowType valueType)
        {
            foreach (var idx in indices)
            {
                if (source.IsNull(idx))
                {
                    builder.AppendNull();
                    continue;
                }
                builder.Append();
                int offset = idx * listSize;
                switch (valueType)
                {
                    case FloatType _:
                    {
                        var vb = (FloatArray.Builder)builder.ValueBuilder;
                        var vals = (FloatArray)source.Values;
                        for (int k = 0; k < listSize; k++)
                        {
                            vb.Append(vals.GetValue(offset + k)!.Value);
                        }
                        break;
                    }
                    case DoubleType _:
                    {
                        var vb = (DoubleArray.Builder)builder.ValueBuilder;
                        var vals = (DoubleArray)source.Values;
                        for (int k = 0; k < listSize; k++)
                        {
                            vb.Append(vals.GetValue(offset + k)!.Value);
                        }
                        break;
                    }
                    default:
                        throw new NotSupportedException(
                            $"FixedSizeList with value type '{valueType.Name}' is not supported.");
                }
            }
        }

        /// <summary>
        /// Strips <c>_distance</c> and <c>_score</c> columns from the result when
        /// <paramref name="returnScore"/> is <c>"relevance"</c>. When <c>"all"</c>,
        /// returns the batch unchanged.
        /// </summary>
        /// <remarks>
        /// Matches Python's <c>Reranker._keep_relevance_score</c>.
        /// </remarks>
        internal static RecordBatch KeepRelevanceScore(RecordBatch batch, string returnScore)
        {
            if (returnScore != "relevance")
            {
                return batch;
            }

            batch = DropColumnIfPresent(batch, ScoreColumn);
            batch = DropColumnIfPresent(batch, DistanceColumn);
            return batch;
        }

        private static RecordBatch DropColumnIfPresent(RecordBatch batch, string columnName)
        {
            var idx = batch.Schema.GetFieldIndex(columnName);
            if (idx < 0)
            {
                return batch;
            }

            var fields = new List<Field>(batch.ColumnCount - 1);
            var arrays = new List<IArrowArray>(batch.ColumnCount - 1);
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                if (i != idx)
                {
                    fields.Add(batch.Schema.GetFieldByIndex(i));
                    arrays.Add(batch.Column(i));
                }
            }
            return new RecordBatch(new Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
        }
    }
}
