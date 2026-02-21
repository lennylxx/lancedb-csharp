namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using Apache.Arrow;
    using Apache.Arrow.Types;

    /// <summary>
    /// Validates and repairs vector columns in Arrow RecordBatches before insertion.
    /// </summary>
    internal static class VectorValidator
    {
        /// <summary>
        /// Processes a RecordBatch according to the bad vector handling strategy.
        /// Returns the original batch if no vector columns exist or no bad vectors found.
        /// </summary>
        internal static RecordBatch HandleBadVectors(
            RecordBatch batch, BadVectorHandling handling, float fillValue)
        {
            if (handling == BadVectorHandling.Error)
            {
                ValidateNoNaN(batch);
                return batch;
            }

            var vectorColumns = FindVectorColumns(batch.Schema);
            if (vectorColumns.Count == 0)
            {
                return batch;
            }

            return handling switch
            {
                BadVectorHandling.Drop => DropBadRows(batch, vectorColumns),
                BadVectorHandling.Fill => FillBadVectors(batch, vectorColumns, fillValue),
                BadVectorHandling.Null => NullBadVectors(batch, vectorColumns),
                _ => throw new ArgumentException($"Invalid BadVectorHandling: {handling}"),
            };
        }

        private static void ValidateNoNaN(RecordBatch batch)
        {
            var vectorColumns = FindVectorColumns(batch.Schema);
            foreach (int colIdx in vectorColumns)
            {
                var fslArray = (FixedSizeListArray)batch.Column(colIdx);
                if (HasAnyNaN(fslArray))
                {
                    throw new ArgumentException(
                        $"Vector column '{batch.Schema.FieldsList[colIdx].Name}' contains NaN values. " +
                        "Use AddOptions.OnBadVectors = BadVectorHandling.Drop to remove them, " +
                        "BadVectorHandling.Fill to replace them, or BadVectorHandling.Null to null them.");
                }
            }
        }

        private static List<int> FindVectorColumns(Schema schema)
        {
            var result = new List<int>();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                var field = schema.FieldsList[i];
                if (field.DataType is FixedSizeListType fslType &&
                    (fslType.ValueDataType is FloatType || fslType.ValueDataType is DoubleType))
                {
                    result.Add(i);
                }
            }

            return result;
        }

        private static bool HasAnyNaN(FixedSizeListArray fslArray)
        {
            var values = fslArray.Values;
            if (values is FloatArray floatArr)
            {
                for (int i = 0; i < floatArr.Length; i++)
                {
                    if (floatArr.GetValue(i) is float v && float.IsNaN(v))
                    {
                        return true;
                    }
                }
            }
            else if (values is DoubleArray doubleArr)
            {
                for (int i = 0; i < doubleArr.Length; i++)
                {
                    if (doubleArr.GetValue(i) is double v && double.IsNaN(v))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private static bool RowHasNaN(FixedSizeListArray fslArray, int row)
        {
            int listSize = ((FixedSizeListType)fslArray.Data.DataType).ListSize;
            int offset = row * listSize;
            var values = fslArray.Values;

            if (values is FloatArray floatArr)
            {
                for (int j = 0; j < listSize; j++)
                {
                    if (floatArr.GetValue(offset + j) is float v && float.IsNaN(v))
                    {
                        return true;
                    }
                }
            }
            else if (values is DoubleArray doubleArr)
            {
                for (int j = 0; j < listSize; j++)
                {
                    if (doubleArr.GetValue(offset + j) is double v && double.IsNaN(v))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private static RecordBatch DropBadRows(RecordBatch batch, List<int> vectorColumns)
        {
            var goodRows = new List<int>();
            for (int row = 0; row < batch.Length; row++)
            {
                bool bad = false;
                foreach (int colIdx in vectorColumns)
                {
                    if (RowHasNaN((FixedSizeListArray)batch.Column(colIdx), row))
                    {
                        bad = true;
                        break;
                    }
                }

                if (!bad)
                {
                    goodRows.Add(row);
                }
            }

            if (goodRows.Count == batch.Length)
            {
                return batch;
            }

            return SliceRows(batch, goodRows);
        }

        private static RecordBatch FillBadVectors(
            RecordBatch batch, List<int> vectorColumns, float fillValue)
        {
            var columns = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                columns[i] = batch.Column(i);
            }

            foreach (int colIdx in vectorColumns)
            {
                columns[colIdx] = FillColumn(
                    (FixedSizeListArray)batch.Column(colIdx), fillValue);
            }

            return new RecordBatch(batch.Schema, columns, batch.Length);
        }

        private static RecordBatch NullBadVectors(
            RecordBatch batch, List<int> vectorColumns)
        {
            var columns = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                columns[i] = batch.Column(i);
            }

            bool schemaChanged = false;
            var fields = new List<Field>(batch.Schema.FieldsList);

            foreach (int colIdx in vectorColumns)
            {
                columns[colIdx] = NullifyColumn(
                    (FixedSizeListArray)batch.Column(colIdx));

                if (!fields[colIdx].IsNullable)
                {
                    var f = fields[colIdx];
                    fields[colIdx] = new Field(f.Name, f.DataType, nullable: true);
                    schemaChanged = true;
                }
            }

            var schema = batch.Schema;
            if (schemaChanged)
            {
                var builder = new Schema.Builder();
                foreach (var f in fields)
                {
                    builder.Field(f);
                }
                schema = builder.Build();
            }

            return new RecordBatch(schema, columns, batch.Length);
        }

        private static FixedSizeListArray FillColumn(FixedSizeListArray original, float fillValue)
        {
            var fslType = (FixedSizeListType)original.Data.DataType;
            int listSize = fslType.ListSize;
            int numRows = original.Length;

            var valueField = fslType.ValueField;
            var builder = new FixedSizeListArray.Builder(valueField, listSize);

            if (original.Values is FloatArray floatValues)
            {
                var valueBuilder = (FloatArray.Builder)builder.ValueBuilder;
                for (int row = 0; row < numRows; row++)
                {
                    builder.Append();
                    if (RowHasNaN(original, row))
                    {
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(fillValue);
                        }
                    }
                    else
                    {
                        int offset = row * listSize;
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(floatValues.GetValue(offset + j)!.Value);
                        }
                    }
                }
            }
            else if (original.Values is DoubleArray doubleValues)
            {
                var valueBuilder = (DoubleArray.Builder)builder.ValueBuilder;
                for (int row = 0; row < numRows; row++)
                {
                    builder.Append();
                    if (RowHasNaN(original, row))
                    {
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(fillValue);
                        }
                    }
                    else
                    {
                        int offset = row * listSize;
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(doubleValues.GetValue(offset + j)!.Value);
                        }
                    }
                }
            }

            return builder.Build();
        }

        private static FixedSizeListArray NullifyColumn(FixedSizeListArray original)
        {
            var fslType = (FixedSizeListType)original.Data.DataType;
            int listSize = fslType.ListSize;
            int numRows = original.Length;

            var valueField = fslType.ValueField;
            var builder = new FixedSizeListArray.Builder(valueField, listSize);

            if (original.Values is FloatArray floatValues)
            {
                var valueBuilder = (FloatArray.Builder)builder.ValueBuilder;
                for (int row = 0; row < numRows; row++)
                {
                    if (RowHasNaN(original, row))
                    {
                        builder.AppendNull();
                    }
                    else
                    {
                        builder.Append();
                        int offset = row * listSize;
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(floatValues.GetValue(offset + j)!.Value);
                        }
                    }
                }
            }
            else if (original.Values is DoubleArray doubleValues)
            {
                var valueBuilder = (DoubleArray.Builder)builder.ValueBuilder;
                for (int row = 0; row < numRows; row++)
                {
                    if (RowHasNaN(original, row))
                    {
                        builder.AppendNull();
                    }
                    else
                    {
                        builder.Append();
                        int offset = row * listSize;
                        for (int j = 0; j < listSize; j++)
                        {
                            valueBuilder.Append(doubleValues.GetValue(offset + j)!.Value);
                        }
                    }
                }
            }

            return builder.Build();
        }

        private static RecordBatch SliceRows(RecordBatch batch, List<int> rowIndices)
        {
            if (rowIndices.Count == 0)
            {
                return new RecordBatch(batch.Schema, BuildEmptyColumns(batch.Schema), 0);
            }

            var columns = new IArrowArray[batch.ColumnCount];
            for (int colIdx = 0; colIdx < batch.ColumnCount; colIdx++)
            {
                columns[colIdx] = SliceColumn(batch.Column(colIdx), batch.Schema.FieldsList[colIdx], rowIndices);
            }

            return new RecordBatch(batch.Schema, columns, rowIndices.Count);
        }

        private static IArrowArray SliceColumn(IArrowArray column, Field field, List<int> rowIndices)
        {
            switch (column)
            {
                case Int32Array int32:
                {
                    var b = new Int32Array.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (int32.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(int32.GetValue(row)!.Value); }
                    }
                    return b.Build();
                }
                case Int64Array int64:
                {
                    var b = new Int64Array.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (int64.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(int64.GetValue(row)!.Value); }
                    }
                    return b.Build();
                }
                case FloatArray floats:
                {
                    var b = new FloatArray.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (floats.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(floats.GetValue(row)!.Value); }
                    }
                    return b.Build();
                }
                case DoubleArray doubles:
                {
                    var b = new DoubleArray.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (doubles.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(doubles.GetValue(row)!.Value); }
                    }
                    return b.Build();
                }
                case StringArray strings:
                {
                    var b = new StringArray.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (strings.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(strings.GetString(row)); }
                    }
                    return b.Build();
                }
                case BooleanArray bools:
                {
                    var b = new BooleanArray.Builder();
                    foreach (int row in rowIndices)
                    {
                        if (bools.IsNull(row)) { b.AppendNull(); }
                        else { b.Append(bools.GetValue(row)!.Value); }
                    }
                    return b.Build();
                }
                case FixedSizeListArray fsl:
                {
                    var fslType = (FixedSizeListType)fsl.Data.DataType;
                    int listSize = fslType.ListSize;
                    var valueField = fslType.ValueField;
                    var builder = new FixedSizeListArray.Builder(valueField, listSize);

                    if (fsl.Values is FloatArray fv)
                    {
                        var vb = (FloatArray.Builder)builder.ValueBuilder;
                        foreach (int row in rowIndices)
                        {
                            if (fsl.IsNull(row))
                            {
                                builder.AppendNull();
                            }
                            else
                            {
                                builder.Append();
                                int offset = row * listSize;
                                for (int j = 0; j < listSize; j++)
                                {
                                    vb.Append(fv.GetValue(offset + j)!.Value);
                                }
                            }
                        }
                    }
                    else if (fsl.Values is DoubleArray dv)
                    {
                        var vb = (DoubleArray.Builder)builder.ValueBuilder;
                        foreach (int row in rowIndices)
                        {
                            if (fsl.IsNull(row))
                            {
                                builder.AppendNull();
                            }
                            else
                            {
                                builder.Append();
                                int offset = row * listSize;
                                for (int j = 0; j < listSize; j++)
                                {
                                    vb.Append(dv.GetValue(offset + j)!.Value);
                                }
                            }
                        }
                    }

                    return builder.Build();
                }
                default:
                    throw new NotSupportedException(
                        $"VectorValidator.SliceColumn does not support {column.GetType().Name} " +
                        $"(Arrow type: {field.DataType})");
            }
        }

        private static IArrowArray[] BuildEmptyColumns(Schema schema)
        {
            var columns = new IArrowArray[schema.FieldsList.Count];
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                var field = schema.FieldsList[i];
                columns[i] = field.DataType switch
                {
                    Int32Type => new Int32Array.Builder().Build(),
                    Int64Type => new Int64Array.Builder().Build(),
                    FloatType => new FloatArray.Builder().Build(),
                    DoubleType => new DoubleArray.Builder().Build(),
                    StringType => new StringArray.Builder().Build(),
                    BooleanType => new BooleanArray.Builder().Build(),
                    FixedSizeListType fslType => new FixedSizeListArray.Builder(
                        fslType.ValueField, fslType.ListSize).Build(),
                    _ => throw new NotSupportedException(
                        $"VectorValidator.BuildEmptyColumns does not support {field.DataType}"),
                };
            }

            return columns;
        }
    }
}
