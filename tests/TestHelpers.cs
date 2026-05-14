namespace lancedb.tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Types;

    /// <summary>
    /// Shared test data factories and fixture builders used across the C# test suite.
    /// Registered as a project-wide <c>using static</c> in tests.csproj, so members are
    /// referenced unqualified (e.g. <c>CreateTestBatch(5)</c>).
    /// </summary>
    internal static class TestHelpers
    {
        // -----------------------------------------------------------------------
        // RecordBatch factories
        // -----------------------------------------------------------------------

        /// <summary>
        /// Returns a single-column ("id": int32) batch with IDs
        /// <c>startId .. startId + numRows - 1</c>.
        /// </summary>
        public static RecordBatch CreateTestBatch(int numRows, int startId = 0)
        {
            var idArray = new Int32Array.Builder();
            for (int i = startId; i < startId + numRows; i++)
            {
                idArray.Append(i);
            }

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idArray.Build() }, numRows);
        }

        /// <summary>
        /// Returns a batch with columns ("id": int32, "content": string) and one row per input text.
        /// </summary>
        public static RecordBatch CreateTextBatch(string[] texts)
        {
            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();
            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i);
                contentBuilder.Append(texts[i]);
            }

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build() }, texts.Length);
        }

        /// <summary>
        /// Returns a batch with columns ("id": int32, "name": string), names of the form <c>name_{i}</c>.
        /// </summary>
        public static RecordBatch CreateTwoColumnBatch(int numRows)
        {
            var idBuilder = new Int32Array.Builder();
            var nameBuilder = new StringArray.Builder();
            for (int i = 0; i < numRows; i++)
            {
                idBuilder.Append(i);
                nameBuilder.Append($"name_{i}");
            }

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("name", StringType.Default, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, numRows);
        }

        /// <summary>
        /// Returns a 3-row batch with columns ("id": int32, "content": string, "vector": fixed_size_list&lt;float, 3&gt;).
        /// </summary>
        public static RecordBatch CreateVectorTextBatch()
        {
            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();

            string[] texts = new[] { "apple banana", "cherry date", "elderberry fig" };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f, 0.0f },
                new float[] { 0.0f, 1.0f, 0.0f },
                new float[] { 0.0f, 0.0f, 1.0f },
            };

            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, 3);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i);
                contentBuilder.Append(texts[i]);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, 3);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
        }

        // Helper to create a batch with id and value columns for MergeInsert tests
        /// <summary>
        /// Returns a batch with columns ("id": int32, "value": string nullable). Lengths must match.
        /// </summary>
        public static RecordBatch CreateIdValueBatch(int[] ids, string[] values)
        {
            var idBuilder = new Int32Array.Builder();
            var valueBuilder = new StringArray.Builder();
            foreach (var id in ids) { idBuilder.Append(id); }
            foreach (var val in values) { valueBuilder.Append(val); }

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("value", StringType.Default, nullable: true))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), valueBuilder.Build() },
                ids.Length);
        }

        /// <summary>
        /// Returns a batch with the supplied vectors and an auto-generated id column. All vectors
        /// must have the same dimension; that dimension is used for the fixed_size_list field.
        /// </summary>
        public static RecordBatch CreateVectorBatch(float[][] vectors)
        {
            var idBuilder = new Int32Array.Builder();
            var valueField = new Field("item", FloatType.Default, nullable: false);
            int dim = vectors[0].Length;
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, dim);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < vectors.Length; i++)
            {
                idBuilder.Append(i);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, dim);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), vectorBuilder.Build() },
                vectors.Length);
        }

        /// <summary>
        /// Returns a batch of <paramref name="numRows"/> rows with id + a deterministic random vector
        /// of the requested dimension (seeded so the data is reproducible).
        /// </summary>
        public static RecordBatch CreateVectorBatch(int numRows, int dimension = 8)
        {
            var idBuilder = new Int32Array.Builder();
            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, dimension);
            var floatBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;
            var rng = new Random(42);

            for (int i = 0; i < numRows; i++)
            {
                idBuilder.Append(i);
                vectorBuilder.Append();
                for (int d = 0; d < dimension; d++)
                {
                    floatBuilder.Append((float)rng.NextDouble());
                }
            }

            var vectorType = new FixedSizeListType(valueField, dimension);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), vectorBuilder.Build() }, numRows);
        }

        /// <summary>
        /// Returns a batch with an id column and a "tags" column of type <c>List&lt;Int32&gt;</c>.
        /// Each row has three tag values <c>[i, i+1, i+2]</c>. Used to exercise
        /// <see cref="LabelListIndex"/> which requires a list-typed column.
        /// </summary>
        public static RecordBatch CreateLabelBatch(int numRows)
        {
            var idBuilder = new Int32Array.Builder();
            var valueField = new Field("item", Int32Type.Default, nullable: true);
            var listType = new ListType(valueField);

            var valueBuilder = new Int32Array.Builder();
            var offsetBuilder = new ArrowBuffer.Builder<int>(numRows + 1);
            var validityBuilder = new ArrowBuffer.BitmapBuilder(numRows);

            int offset = 0;
            offsetBuilder.Append(offset);
            for (int i = 0; i < numRows; i++)
            {
                idBuilder.Append(i);
                valueBuilder.Append(i);
                valueBuilder.Append(i + 1);
                valueBuilder.Append(i + 2);
                offset += 3;
                offsetBuilder.Append(offset);
                validityBuilder.Append(true);
            }

            var valuesArray = valueBuilder.Build();
            var listData = new ArrayData(
                listType, numRows, 0, 0,
                new[] { validityBuilder.Build(), offsetBuilder.Build() },
                new[] { valuesArray.Data });
            var listArray = new ListArray(listData);

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("tags", listType, nullable: false))
                .Build();

            return new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), listArray }, numRows);
        }

        // -----------------------------------------------------------------------
        // Reranker-test schema and result factories
        // -----------------------------------------------------------------------

        /// <summary>
        /// Builds a schema from the listed column names. Reserved names get their canonical type
        /// (<c>_distance</c>/<c>_score</c> → float, <c>_rowid</c> → uint64); everything else is a string.
        /// </summary>
        public static Schema CreateSchema(params string[] extraColumns)
        {
            var fields = new List<Field>();
            foreach (var col in extraColumns)
            {
                if (col == "_distance")
                {
                    fields.Add(new Field(col, FloatType.Default, true));
                }
                else if (col == "_score")
                {
                    fields.Add(new Field(col, FloatType.Default, true));
                }
                else if (col == "_rowid")
                {
                    fields.Add(new Field(col, UInt64Type.Default, false));
                }
                else
                {
                    fields.Add(new Field(col, StringType.Default, false));
                }
            }
            return new Schema(fields, null);
        }

        public static RecordBatch CreateVectorResults(string[] names, ulong[] rowIds, float[] distances)
        {
            var schema = CreateSchema("name", "_rowid", "_distance");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(names).Build(),
                new UInt64Array.Builder().AppendRange(rowIds).Build(),
                new FloatArray.Builder().AppendRange(distances).Build(),
            }, names.Length);
        }

        public static RecordBatch CreateFtsResults(string[] names, ulong[] rowIds, float[] scores)
        {
            var schema = CreateSchema("name", "_rowid", "_score");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(names).Build(),
                new UInt64Array.Builder().AppendRange(rowIds).Build(),
                new FloatArray.Builder().AppendRange(scores).Build(),
            }, names.Length);
        }

        public static RecordBatch CreateEmptyVectorResults()
        {
            var schema = CreateSchema("name", "_rowid", "_distance");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new UInt64Array.Builder().Build(),
                new FloatArray.Builder().Build(),
            }, 0);
        }

        public static RecordBatch CreateEmptyFtsResults()
        {
            var schema = CreateSchema("name", "_rowid", "_score");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new UInt64Array.Builder().Build(),
                new FloatArray.Builder().Build(),
            }, 0);
        }

        // -----------------------------------------------------------------------
        // Mock-reranker helper
        // -----------------------------------------------------------------------

        /// <summary>
        /// Appends a <c>_relevance_score</c> float column to <paramref name="batch"/>, with the value
        /// <c>1 / (i + 1)</c> at row <c>i</c>. Used by mock IReranker implementations in tests.
        /// </summary>
        public static RecordBatch AddRelevanceScore(RecordBatch batch)
        {
            var scoreBuilder = new FloatArray.Builder();
            for (int i = 0; i < batch.Length; i++)
            {
                scoreBuilder.Append(1.0f / (i + 1));
            }
            var fields = new List<Field>(batch.Schema.FieldsList);
            fields.Add(new Field("_relevance_score", FloatType.Default, false));
            var arrays = new List<IArrowArray>();
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays.Add(batch.Column(i));
            }
            arrays.Add(scoreBuilder.Build());
            return new RecordBatch(new Schema(fields, null), arrays, batch.Length);
        }
    }
}
