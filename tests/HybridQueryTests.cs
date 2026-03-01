namespace lancedb.tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using Xunit;

    public class HybridQueryTests
    {
        // ----- FTSQuery Tests -----

        [Fact]
        public async Task NearestToText_ReturnsFTSQuery()
        {
            using var fixture = await CreateHybridFixture("fts_type");
            var ftsQuery = fixture.Table.Query().NearestToText("apple");
            Assert.IsType<FTSQuery>(ftsQuery);
        }

        [Fact]
        public async Task FTSQuery_ToArrow_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("fts_exec");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task FTSQuery_WithLimit_RespectsLimit()
        {
            using var fixture = await CreateHybridFixture("fts_limit");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .Limit(1)
                .ToArrow();
            Assert.Equal(1, results.Length);
        }

        [Fact]
        public async Task FTSQuery_NearestTo_ReturnsHybridQuery()
        {
            using var fixture = await CreateHybridFixture("fts_to_hybrid");
            var hybridQuery = fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            Assert.IsType<HybridQuery>(hybridQuery);
        }

        // ----- HybridQuery Tests -----

        [Fact]
        public async Task HybridQuery_FromFTSQuery_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_from_fts");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_FromVectorQuery_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_from_vec");
            var results = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .NearestToText("apple")
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_FromTableEntry_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_table");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_DefaultReranker_IsRRF()
        {
            using var fixture = await CreateHybridFixture("hybrid_default_rrf");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();

            // Results should have _relevance_score from RRF
            var schema = results.Schema;
            Assert.Contains(schema.FieldsList, f => f.Name == "_relevance_score");
        }

        [Fact]
        public async Task HybridQuery_WithRRFReranker_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_rrf");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new RRFReranker(k: 30))
                .ToArrow();
            Assert.True(results.Length > 0);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
        }

        [Fact]
        public async Task HybridQuery_WithLinearCombination_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_linear");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new LinearCombinationReranker(weight: 0.5f))
                .ToArrow();
            Assert.True(results.Length > 0);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
        }

        [Fact]
        public async Task HybridQuery_WithLimit_RespectsLimit()
        {
            using var fixture = await CreateHybridFixture("hybrid_limit");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Limit(1)
                .ToArrow();
            Assert.True(results.Length <= 1);
        }

        [Fact]
        public async Task HybridQuery_WithWhere_FiltersResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_where");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Where("id = 0")
                .ToArrow();

            // Should only return the row with id=0
            var idCol = (Int32Array)results.Column("id");
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(0, idCol.GetValue(i));
            }
        }

        [Fact]
        public async Task HybridQuery_WithSelect_ProjectsColumns()
        {
            using var fixture = await CreateHybridFixture("hybrid_select");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Select(new[] { "id", "content" })
                .ToArrow();

            Assert.True(results.Length > 0);
            // The final result should have id and content, plus _relevance_score and _rowid
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "id");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "content");
        }

        [Fact]
        public async Task HybridQuery_ToList_ReturnsResults()
        {
            using var fixture = await CreateHybridFixture("hybrid_tolist");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .ToList();
            Assert.True(results.Count > 0);
            Assert.Contains("_relevance_score", results[0].Keys);
        }

        // ----- Fixture -----

        private static async Task<TestFixture> CreateHybridFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();

            string[] texts = new[] { "apple banana fruit", "cherry date sweet", "elderberry fig tart" };
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

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);

            // Create FTS index on content column
            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }
    }
}
