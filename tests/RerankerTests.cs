namespace lancedb.tests
{
    using System;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using Xunit;

    public class RerankerTests
    {
        private static Schema CreateSchema(params string[] extraColumns)
        {
            var fields = new System.Collections.Generic.List<Field>();
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

        private static RecordBatch CreateVectorResults(string[] names, ulong[] rowIds, float[] distances)
        {
            var schema = CreateSchema("name", "_rowid", "_distance");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(names).Build(),
                new UInt64Array.Builder().AppendRange(rowIds).Build(),
                new FloatArray.Builder().AppendRange(distances).Build(),
            }, names.Length);
        }

        private static RecordBatch CreateFtsResults(string[] names, ulong[] rowIds, float[] scores)
        {
            var schema = CreateSchema("name", "_rowid", "_score");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(names).Build(),
                new UInt64Array.Builder().AppendRange(rowIds).Build(),
                new FloatArray.Builder().AppendRange(scores).Build(),
            }, names.Length);
        }

        private static RecordBatch CreateEmptyVectorResults()
        {
            var schema = CreateSchema("name", "_rowid", "_distance");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new UInt64Array.Builder().Build(),
                new FloatArray.Builder().Build(),
            }, 0);
        }

        private static RecordBatch CreateEmptyFtsResults()
        {
            var schema = CreateSchema("name", "_rowid", "_score");
            return new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new UInt64Array.Builder().Build(),
                new FloatArray.Builder().Build(),
            }, 0);
        }

        // ===== RRF Reranker Tests =====

        [Fact]
        public async Task RRFReranker_HybridResults_RankedByRRFScore()
        {
            // Ported from Rust test in rrf.rs with k=1
            var vecResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "foo", "bar", "baz", "bean", "dog" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 1, 4, 2, 5, 3 }).Build(),
                }, 5);

            var ftsResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "bar", "bean", "dog" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 4, 5, 3 }).Build(),
                }, 3);

            var reranker = new RRFReranker(k: 1f);
            var result = await reranker.RerankHybrid("", vecResults, ftsResults);

            Assert.Equal(5, result.Length);
            Assert.Equal(3, result.Schema.FieldsList.Count);

            var names = (StringArray)result.Column(0);
            Assert.Equal("bar", names.GetString(0));   // 1/2 + 1/1 = 1.5
            Assert.Equal("foo", names.GetString(1));    // 1/1 = 1.0
            Assert.Equal("bean", names.GetString(2));   // 1/4 + 1/2 = 0.75
            Assert.Equal("dog", names.GetString(3));    // 1/5 + 1/3 ≈ 0.533
            Assert.Equal("baz", names.GetString(4));    // 1/3 ≈ 0.333

            var scores = (FloatArray)result.Column(result.Schema.GetFieldIndex("_relevance_score"));
            Assert.Equal(1.5f, scores.GetValue(0));
            Assert.Equal(1.0f, scores.GetValue(1));
            Assert.Equal(0.75f, scores.GetValue(2));
        }

        [Fact]
        public async Task RRFReranker_DefaultK_ProducesResults()
        {
            var vecResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "a", "b" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 1, 2 }).Build(),
                }, 2);

            var ftsResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "b", "c" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 2, 3 }).Build(),
                }, 2);

            var reranker = new RRFReranker(); // k=60
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(3, result.Length);
            // "b" appears in both, should have highest score
            var names = (StringArray)result.Column(0);
            Assert.Equal("b", names.GetString(0));
        }

        [Fact]
        public async Task RRFReranker_EmptyVectorResults_ReturnsFtsOnly()
        {
            var vecResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().Build(),
                    new UInt64Array.Builder().Build(),
                }, 0);

            var ftsResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "a", "b" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 1, 2 }).Build(),
                }, 2);

            var reranker = new RRFReranker(k: 1f);
            var result = await reranker.RerankHybrid("", vecResults, ftsResults);

            Assert.Equal(2, result.Length);
        }

        [Fact]
        public async Task RRFReranker_EmptyFtsResults_ReturnsVectorOnly()
        {
            var vecResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().AppendRange(new[] { "x", "y" }).Build(),
                    new UInt64Array.Builder().AppendRange(new ulong[] { 10, 20 }).Build(),
                }, 2);

            var ftsResults = new RecordBatch(
                CreateSchema("name", "_rowid"),
                new IArrowArray[]
                {
                    new StringArray.Builder().Build(),
                    new UInt64Array.Builder().Build(),
                }, 0);

            var reranker = new RRFReranker(k: 1f);
            var result = await reranker.RerankHybrid("", vecResults, ftsResults);

            Assert.Equal(2, result.Length);
        }

        [Fact]
        public void RRFReranker_InvalidK_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new RRFReranker(k: 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new RRFReranker(k: -1));
        }

        // ===== LinearCombinationReranker Tests =====

        [Fact]
        public async Task LinearCombination_HybridResults_CombinesScores()
        {
            var vecResults = CreateVectorResults(
                new[] { "a", "b", "c" },
                new ulong[] { 1, 2, 3 },
                new float[] { 0.1f, 0.5f, 0.9f });

            var ftsResults = CreateFtsResults(
                new[] { "b", "d" },
                new ulong[] { 2, 4 },
                new float[] { 0.8f, 0.3f });

            var reranker = new LinearCombinationReranker(weight: 0.7f, fill: 1.0f);
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(4, result.Length);
            var scoreIdx = result.Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx >= 0);

            // Verify sorted descending
            var scores = (FloatArray)result.Column(scoreIdx);
            for (int i = 0; i < result.Length - 1; i++)
            {
                Assert.True(scores.GetValue(i) >= scores.GetValue(i + 1),
                    $"Results not sorted descending at index {i}");
            }
        }

        [Fact]
        public async Task LinearCombination_OverlappingResults_HigherRelevance()
        {
            // "b" appears in both sets with good scores → should rank highest
            var vecResults = CreateVectorResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.2f, 0.3f });

            var ftsResults = CreateFtsResults(
                new[] { "b", "c" },
                new ulong[] { 2, 3 },
                new float[] { 0.1f, 0.9f });

            // weight=0.5: relevance = 1 - (0.5 * vecScore + 0.5 * ftsScore)
            // vecScore = 1 - distance
            // "a": 1-(0.5*0.8 + 0.5*1.0)=0.1, "b": 1-(0.5*0.7+0.5*0.1)=0.6, "c": 1-(0.5*0+0.5*0.9)=0.55
            var reranker = new LinearCombinationReranker(weight: 0.5f, fill: 1.0f);
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(3, result.Length);

            var names = (StringArray)result.Column(0);
            Assert.Equal("b", names.GetString(0));
        }

        [Fact]
        public async Task LinearCombination_EmptyVectorResults_ReturnsFtsOnly()
        {
            var vecResults = CreateEmptyVectorResults();
            var ftsResults = CreateFtsResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.8f, 0.5f });

            var reranker = new LinearCombinationReranker();
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(2, result.Length);
        }

        [Fact]
        public async Task LinearCombination_EmptyFtsResults_ReturnsVectorOnly()
        {
            var vecResults = CreateVectorResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.2f, 0.8f });
            var ftsResults = CreateEmptyFtsResults();

            var reranker = new LinearCombinationReranker();
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(2, result.Length);
        }

        [Fact]
        public async Task LinearCombination_BothEmpty_ReturnsEmpty()
        {
            var vecResults = CreateEmptyVectorResults();
            var ftsResults = CreateEmptyFtsResults();

            var reranker = new LinearCombinationReranker();
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            Assert.Equal(0, result.Length);
        }

        [Fact]
        public void LinearCombination_InvalidWeight_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new LinearCombinationReranker(weight: -0.1f));
            Assert.Throws<ArgumentOutOfRangeException>(() => new LinearCombinationReranker(weight: 1.1f));
        }

        [Fact]
        public async Task LinearCombination_Weight0_OnlyFtsMatters()
        {
            var vecResults = CreateVectorResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.1f, 0.9f });

            var ftsResults = CreateFtsResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.9f, 0.1f });

            // weight=0 means 0% vector, 100% FTS
            var reranker = new LinearCombinationReranker(weight: 0f);
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            // relevance = 1 - (0 * vecScore + 1 * ftsScore)
            // "a": 1 - (0 + 0.9) = 0.1, "b": 1 - (0 + 0.1) = 0.9
            // "b" should rank first
            var names = (StringArray)result.Column(0);
            Assert.Equal("b", names.GetString(0));
            Assert.Equal("a", names.GetString(1));
        }

        [Fact]
        public async Task LinearCombination_Weight1_OnlyVectorMatters()
        {
            var vecResults = CreateVectorResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.1f, 0.9f });

            var ftsResults = CreateFtsResults(
                new[] { "a", "b" },
                new ulong[] { 1, 2 },
                new float[] { 0.9f, 0.1f });

            // weight=1 means 100% vector, 0% FTS
            var reranker = new LinearCombinationReranker(weight: 1f);
            var result = await reranker.RerankHybrid("test", vecResults, ftsResults);

            // relevance = 1 - (1 * (1-dist) + 0)
            // "a": 1 - (1-0.1) = 0.1, "b": 1 - (1-0.9) = 0.9
            // "b" should rank first (higher distance = lower vector score = higher relevance? No...)
            // Actually: vectorScore = 1-dist. "a" vecScore=0.9, "b" vecScore=0.1
            // relevance = 1 - vecScore. "a": 1-0.9=0.1, "b": 1-0.1=0.9
            // So "b" ranks first
            var names = (StringArray)result.Column(0);
            Assert.Equal("b", names.GetString(0));
            Assert.Equal("a", names.GetString(1));
        }

        // ===== MergeResults Tests =====

        [Fact]
        public void MergeResults_DeduplicatesByRowId()
        {
            var schema = CreateSchema("name", "_rowid");

            var batch1 = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(new[] { "a", "b", "c" }).Build(),
                new UInt64Array.Builder().AppendRange(new ulong[] { 1, 2, 3 }).Build(),
            }, 3);

            var batch2 = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(new[] { "b", "d" }).Build(),
                new UInt64Array.Builder().AppendRange(new ulong[] { 2, 4 }).Build(),
            }, 2);

            var merged = RerankerHelpers.MergeResults(batch1, batch2);

            Assert.Equal(4, merged.Length); // b is deduplicated
            var rowIds = (UInt64Array)merged.Column(1);
            var ids = new System.Collections.Generic.HashSet<ulong>();
            for (int i = 0; i < rowIds.Length; i++)
            {
                Assert.True(ids.Add(rowIds.GetValue(i)!.Value), "Duplicate row ID found");
            }
        }

        [Fact]
        public void MergeResults_FirstEmptyReturnsSecond()
        {
            var schema = CreateSchema("name", "_rowid");

            var empty = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new UInt64Array.Builder().Build(),
            }, 0);

            var batch = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(new[] { "a" }).Build(),
                new UInt64Array.Builder().AppendRange(new ulong[] { 1 }).Build(),
            }, 1);

            var merged = RerankerHelpers.MergeResults(empty, batch);
            Assert.Equal(1, merged.Length);
        }
    }
}
