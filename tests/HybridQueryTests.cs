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
            // _rowid should NOT be in results unless user explicitly requests it
            Assert.DoesNotContain(schema.FieldsList, f => f.Name == "_rowid");
        }

        [Fact]
        public async Task HybridQuery_WithRowId_IncludesRowId()
        {
            using var fixture = await CreateHybridFixture("hybrid_with_rowid");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .WithRowId()
                .ToArrow();

            Assert.True(results.Length > 0);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_rowid");
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

        /// <summary>
        /// Verifies that rerankers with returnScore="relevance" (default) strip
        /// _distance and _score columns, and returnScore="all" keeps them.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// from lancedb.rerankers.rrf import RRFReranker
        /// from lancedb.rerankers.linear_combination import LinearCombinationReranker
        /// from lancedb.rerankers.mrr import MRRReranker
        ///
        /// # Default (return_score="relevance") — NO _distance or _score
        /// r = table.search(query_type='hybrid').vector([1,0,0]).text('apple') \
        ///     .rerank(RRFReranker()).to_arrow()
        /// assert r.column_names == ['id', 'text', 'vector', '_relevance_score']
        ///
        /// # return_score="all" — has _distance AND _score
        /// r = table.search(query_type='hybrid').vector([1,0,0]).text('apple') \
        ///     .rerank(RRFReranker(return_score="all")).to_arrow()
        /// assert '_distance' in r.column_names
        /// assert '_score' in r.column_names
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_ReturnScoreRelevance_StripsDistanceAndScore()
        {
            using var fixture = await CreateHybridFixture("hybrid_retscore_rel");

            // RRF default (returnScore="relevance") — should NOT have _distance or _score
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new RRFReranker())
                .ToArrow();
            Assert.True(results.Length > 0);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_distance");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_score");
        }

        [Fact]
        public async Task HybridQuery_ReturnScoreAll_KeepsDistanceAndScore()
        {
            using var fixture = await CreateHybridFixture("hybrid_retscore_all");

            // RRF with returnScore="all" — should have _distance AND _score
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new RRFReranker(returnScore: "all"))
                .ToArrow();
            Assert.True(results.Length > 0);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_distance");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_score");
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

        /// <summary>
        /// With 3 results, offset=1 and limit=2 should return 2 rows
        /// (skip 1, then take up to 2). Verifies offset is applied before limit.
        /// </summary>
        [Fact]
        public async Task HybridQuery_OffsetThenLimit_ReturnsCorrectCount()
        {
            using var fixture = await CreateHybridFixture("hybrid_off_lim");
            var allResults = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();

            // Sanity: we have 3 rows total
            Assert.Equal(3, allResults.Length);

            // offset=1, limit=2: skip 1 row, take up to 2 → should return 2
            var paged = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Offset(1)
                .Limit(2)
                .ToArrow();

            Assert.Equal(2, paged.Length);
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
            // Should contain selected columns plus _relevance_score
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "id");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "content");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
            // Should NOT contain unselected columns
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "vector");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_rowid");
        }

        /// <summary>
        /// Verifies that Select(Dictionary) passes SQL expressions to sub-queries so
        /// computed columns (like "price * 2") appear in the final result.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// r = await (table.query()
        ///     .nearest_to([1.0, 0.0, 0.0])
        ///     .nearest_to_text("apple")
        ///     .rerank(RRFReranker())
        ///     .select({"id": "id", "double_price": "price * 2"})
        ///     .to_arrow())
        /// # Columns: ['id', 'double_price', '_relevance_score']
        /// # double_price values are 2x the original price
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_SelectDict_PassesExpressionsToSubQueries()
        {
            using var fixture = await CreateHybridFixtureWithPrice("hybrid_sel_expr");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Select(new System.Collections.Generic.Dictionary<string, string>
                {
                    { "id", "id" },
                    { "double_price", "price * 2" },
                })
                .ToArrow();

            Assert.True(results.Length > 0);
            // Should contain computed column
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "double_price");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "id");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
            // Should NOT contain original price (not selected)
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "price");

            // Verify computed values: double_price should be 2x price
            var idCol = (Int32Array)results.Column("id");
            var dpCol = (Int32Array)results.Column("double_price");
            // Original prices: id=0 -> 10, id=1 -> 20, id=2 -> 30
            for (int i = 0; i < results.Length; i++)
            {
                int id = idCol.GetValue(i)!.Value;
                int doublePrice = dpCol.GetValue(i)!.Value;
                int expectedPrice = (id + 1) * 10 * 2; // price = (id+1)*10, doubled
                Assert.Equal(expectedPrice, doublePrice);
            }
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

        /// <summary>
        /// Verifies that when FTS sub-query returns no results, LinearCombinationReranker
        /// short-circuits to use inverted distance as relevance score (matching Python).
        /// Python skips the weighted formula and returns _relevance_score = 1 - distance.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// db = await lancedb.connect_async("/tmp/test")
        /// data = [
        ///     {"id": 1, "text": "apple banana fruit", "vector": [1.0, 0.0]},
        ///     {"id": 2, "text": "cherry date sweet",  "vector": [0.0, 1.0]},
        ///     {"id": 3, "text": "apple cherry tart",  "vector": [0.5, 0.5]},
        ///     {"id": 4, "text": "banana fig jam",      "vector": [0.9, 0.1]},
        ///     {"id": 5, "text": "apple pie dessert",  "vector": [0.0, 0.9]},
        /// ]
        /// table = await db.create_table("test", data, mode="overwrite")
        /// await table.create_index("text", config=FTS(with_position=False))
        ///
        /// # FTS empty + LC(weight=0.7, return_score="relevance")
        /// results = await (table.query()
        ///     .nearest_to([1.0, 0.0]).nearest_to_text("nonexistent_xyz")
        ///     .rerank(LinearCombinationReranker(weight=0.7)).to_arrow())
        /// # Columns: ['id', 'text', 'vector', '_relevance_score']
        /// # id=1 _relevance_score=1.0
        /// # id=4 _relevance_score=0.99
        /// # id=3 _relevance_score=0.75
        /// # id=5 _relevance_score=0.095
        /// # id=2 _relevance_score=0.0
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_EmptyFts_LinearCombination_MatchesPython()
        {
            using var fixture = await CreateNormalizationFixture("hybrid_empty_fts_lc");
            var reranker = new LinearCombinationReranker(weight: 0.7f, fill: 1.0f);
            var results = await fixture.Table
                .HybridSearch("nonexistent_xyz", new double[] { 1.0, 0.0 })
                .Rerank(reranker)
                .ToArrow();

            // Should still return vector results even though FTS is empty
            Assert.Equal(5, results.Length);

            var idIdx = results.Schema.GetFieldIndex("id");
            var ids = (Int32Array)results.Column(idIdx);
            var relIdx = results.Schema.GetFieldIndex("_relevance_score");
            var scores = (FloatArray)results.Column(relIdx);

            // Python short-circuits: relevance = 1 - normalized_distance
            var expectedIds = new int[] { 1, 4, 3, 5, 2 };
            var expectedScores = new float[] { 1.0f, 0.99f, 0.75f, 0.095f, 0.0f };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
                Assert.Equal(expectedScores[i], scores.GetValue(i)!.Value, precision: 1);
            }

            // Default returnScore="relevance": no _distance or _score columns
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_distance");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_score");
        }

        /// <summary>
        /// Verifies that when FTS is empty and returnScore="all", LC adds a NaN-filled
        /// _score column (matching Python).
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// results = await (table.query()
        ///     .nearest_to([1.0, 0.0]).nearest_to_text("nonexistent_xyz")
        ///     .rerank(LinearCombinationReranker(weight=0.7, return_score="all")).to_arrow())
        /// # Columns: ['id', 'text', 'vector', '_distance', '_relevance_score', '_score']
        /// # _score column is NaN for all rows
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_EmptyFts_LinearCombination_ReturnScoreAll_HasNanScore()
        {
            using var fixture = await CreateNormalizationFixture("hybrid_empty_fts_lc_all");
            var reranker = new LinearCombinationReranker(weight: 0.7f, fill: 1.0f,
                returnScore: "all");
            var results = await fixture.Table
                .HybridSearch("nonexistent_xyz", new double[] { 1.0, 0.0 })
                .Rerank(reranker)
                .ToArrow();

            Assert.Equal(5, results.Length);
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_distance");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_score");
            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");

            // _score should be NaN for all rows (no FTS results)
            var scoreIdx = results.Schema.GetFieldIndex("_score");
            var scores = (FloatArray)results.Column(scoreIdx);
            for (int i = 0; i < results.Length; i++)
            {
                Assert.True(float.IsNaN(scores.GetValue(i)!.Value),
                    $"Row {i} _score should be NaN");
            }
        }

        /// <summary>
        /// Verifies that RRF reranker handles empty FTS results correctly.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// results = await (table.query()
        ///     .nearest_to([1.0, 0.0]).nearest_to_text("nonexistent_xyz")
        ///     .rerank(RRFReranker()).to_arrow())
        /// # Columns: ['id', 'text', 'vector', '_relevance_score']
        /// # Rows: 5 (all from vector, ranked by position)
        /// # id=1 _relevance_score=0.0164
        /// # id=4 _relevance_score=0.0161
        /// # id=3 _relevance_score=0.0159
        /// # id=5 _relevance_score=0.0156
        /// # id=2 _relevance_score=0.0154
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_EmptyFts_RRF_StillWorks()
        {
            using var fixture = await CreateNormalizationFixture("hybrid_empty_fts_rrf");
            var reranker = new RRFReranker();
            var results = await fixture.Table
                .HybridSearch("nonexistent_xyz", new double[] { 1.0, 0.0 })
                .Rerank(reranker)
                .ToArrow();

            Assert.Equal(5, results.Length);

            var idIdx = results.Schema.GetFieldIndex("id");
            var ids = (Int32Array)results.Column(idIdx);

            // RRF only has vector-side rankings (1/(rank+60))
            var expectedIds = new int[] { 1, 4, 3, 5, 2 };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
            }

            Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_distance");
            Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_score");
        }

        /// <summary>
        /// Verifies that HybridQuery normalizes _distance and _score to [0,1]
        /// before reranking (matching Python behavior). Without normalization,
        /// LinearCombinationReranker produces wrong rankings because raw distances
        /// and FTS scores are on incomparable scales.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb v0.29.2):
        /// <code>
        /// db = lancedb.connect("/tmp/test")
        /// data = [
        ///     {"id": 1, "text": "apple banana fruit", "vector": [1.0, 0.0]},
        ///     {"id": 2, "text": "cherry date sweet",  "vector": [0.0, 1.0]},
        ///     {"id": 3, "text": "apple cherry tart",  "vector": [0.5, 0.5]},
        ///     {"id": 4, "text": "banana fig jam",     "vector": [0.9, 0.1]},
        ///     {"id": 5, "text": "apple pie dessert",  "vector": [0.0, 0.9]},
        /// ]
        /// table = db.create_table("test", data)
        /// table.create_fts_index("text")
        /// reranker = LinearCombinationReranker(weight=0.5, fill=1.0)
        /// results = (table.search(query_type="hybrid")
        ///     .vector([1.0, 0.0]).text("apple")
        ///     .rerank(reranker).limit(5).to_arrow())
        ///
        /// # Output:
        /// #   id=5, _relevance_score=0.9525
        /// #   id=3, _relevance_score=0.6250
        /// #   id=1, _relevance_score=0.5000
        /// #   id=2, _relevance_score=0.5000
        /// #   id=4, _relevance_score=0.0050
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_LinearCombination_NormalizesScoresBeforeReranking()
        {
            using var fixture = await CreateNormalizationFixture("hybrid_norm");
            var reranker = new LinearCombinationReranker(weight: 0.5f, fill: 1.0f);
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0 })
                .Rerank(reranker)
                .Limit(5)
                .ToArrow();

            // Expected ranking and scores verified against Python (see remarks above)
            var idIdx = results.Schema.GetFieldIndex("id");
            var ids = (Int32Array)results.Column(idIdx);
            var relIdx = results.Schema.GetFieldIndex("_relevance_score");
            var scores = (FloatArray)results.Column(relIdx);

            Assert.Equal(5, results.Length);

            var expectedIds = new int[] { 5, 3, 1, 2, 4 };
            var expectedScores = new float[] { 0.9525f, 0.625f, 0.5f, 0.5f, 0.005f };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
                Assert.Equal(expectedScores[i], scores.GetValue(i)!.Value, precision: 2);
            }
        }

        /// <summary>
        /// Verifies that normalize="rank" converts scores to ordinal ranks before
        /// min-max normalization, producing different rankings than normalize="score".
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb v0.29.2):
        /// <code>
        /// # Same data as NormalizesScoresBeforeReranking test
        /// reranker = LinearCombinationReranker(weight=0.5, fill=1.0)
        /// results = (table.search(query_type="hybrid")
        ///     .vector([1.0, 0.0]).text("apple")
        ///     .rerank(reranker, normalize="rank").limit(5).to_arrow())
        ///
        /// # Output:
        /// #   id=3, _relevance_score=0.7500
        /// #   id=5, _relevance_score=0.6250
        /// #   id=2, _relevance_score=0.5000
        /// #   id=4, _relevance_score=0.1250
        /// #   id=1, _relevance_score=0.0000
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_LinearCombination_NormalizeRank_MatchesPython()
        {
            using var fixture = await CreateNormalizationFixture("hybrid_rank_norm");
            var reranker = new LinearCombinationReranker(weight: 0.5f, fill: 1.0f);
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0 })
                .Rerank(reranker, normalize: "rank")
                .Limit(5)
                .ToArrow();

            var idIdx = results.Schema.GetFieldIndex("id");
            var ids = (Int32Array)results.Column(idIdx);
            var relIdx = results.Schema.GetFieldIndex("_relevance_score");
            var scores = (FloatArray)results.Column(relIdx);

            Assert.Equal(5, results.Length);

            var expectedIds = new int[] { 3, 5, 2, 4, 1 };
            var expectedScores = new float[] { 0.75f, 0.625f, 0.5f, 0.125f, 0.0f };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
                Assert.Equal(expectedScores[i], scores.GetValue(i)!.Value, precision: 2);
            }
        }

        // ----- Fixture -----

        /// <summary>
        /// Creates a fixture with 5 items designed to test score normalization.
        /// Vectors produce L2 distances in range [0, 2], FTS scores are all identical.
        /// </summary>
        private static async Task<TestFixture> CreateNormalizationFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();

            // id=1: matches FTS + closest vector
            // id=2: no FTS match + farthest vector
            // id=3: matches FTS + mid vector
            // id=4: no FTS match + close vector
            // id=5: matches FTS + far vector
            string[] texts = new[]
            {
                "apple banana fruit",
                "cherry date sweet",
                "apple cherry tart",
                "banana fig jam",
                "apple pie dessert",
            };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f },   // dist=0.0 from [1,0]
                new float[] { 0.0f, 1.0f },   // dist=2.0
                new float[] { 0.5f, 0.5f },   // dist=0.5
                new float[] { 0.9f, 0.1f },   // dist=0.02
                new float[] { 0.0f, 0.9f },   // dist=1.81
            };

            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, 2);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i + 1);
                contentBuilder.Append(texts[i]);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, 2);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);
            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }

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

        private static async Task<TestFixture> CreateHybridFixtureWithPrice(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();
            var priceBuilder = new Int32Array.Builder();

            string[] texts = new[] { "apple banana fruit", "cherry date sweet", "elderberry fig tart" };
            int[] prices = new[] { 10, 20, 30 };
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
                priceBuilder.Append(prices[i]);
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
                .Field(new Field("price", Int32Type.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), priceBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);

            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }
    }
}
