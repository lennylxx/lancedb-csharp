namespace lancedb.tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using Xunit;
    using static TestHelpers;

    public class HybridQueryTests
    {
        // ----- FTSQuery Tests -----

        [Fact]
        public async Task NearestToText_ReturnsFTSQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_type");
            var ftsQuery = fixture.Table.Query().NearestToText("apple");
            Assert.IsType<FTSQuery>(ftsQuery);
        }

        [Fact]
        public async Task FTSQuery_ToArrow_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_exec");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task FTSQuery_WithLimit_RespectsLimit()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_limit");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .Limit(1)
                .ToArrow();
            Assert.Equal(1, results.Length);
        }

        [Fact]
        public async Task FTSQuery_NearestTo_ReturnsHybridQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_to_hybrid");
            var hybridQuery = fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            Assert.IsType<HybridQuery>(hybridQuery);
        }

        // ----- FTSQuery Rerank Tests -----

        [Fact]
        public async Task FTSQuery_Rerank_AppliesReranker()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_rerank");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .Rerank(new FtsTestReranker())
                .ToArrow();
            Assert.True(results.Length > 0);
            var scoreIdx = results.Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx >= 0, "Expected _relevance_score column from reranker");
        }

        [Fact]
        public async Task FTSQuery_Rerank_ToBatches_AppliesReranker()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_rerank_batches");
            using var reader = await fixture.Table.Query()
                .NearestToText("apple")
                .Rerank(new FtsTestReranker())
                .ToBatches();
            var batches = new List<RecordBatch>();
            await foreach (var batch in reader)
            {
                batches.Add(batch);
            }
            Assert.True(batches.Count > 0);
            var scoreIdx = batches[0].Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx >= 0, "Expected _relevance_score column from reranker");
        }

        [Fact]
        public async Task FTSQuery_WithoutRerank_NoRelevanceScore()
        {
            using var fixture = await TestFixture.CreateHybridFixture("fts_no_rerank");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .ToArrow();
            Assert.True(results.Length > 0);
            var scoreIdx = results.Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx < 0, "Should not have _relevance_score without reranker");
        }

        // ----- VectorQuery Rerank Tests -----

        [Fact]
        public async Task VectorQuery_Rerank_AppliesReranker()
        {
            using var fixture = await TestFixture.CreateHybridFixture("vec_rerank");
            var results = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new VectorTestReranker(), "apple")
                .ToArrow();
            Assert.True(results.Length > 0);
            var scoreIdx = results.Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx >= 0, "Expected _relevance_score column from reranker");
        }

        [Fact]
        public async Task VectorQuery_Rerank_ToBatches_AppliesReranker()
        {
            using var fixture = await TestFixture.CreateHybridFixture("vec_rerank_batches");
            using var reader = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new VectorTestReranker(), "apple")
                .ToBatches();
            var batches = new List<RecordBatch>();
            await foreach (var batch in reader)
            {
                batches.Add(batch);
            }
            Assert.True(batches.Count > 0);
            var scoreIdx = batches[0].Schema.GetFieldIndex("_relevance_score");
            Assert.True(scoreIdx >= 0, "Expected _relevance_score column from reranker");
        }

        [Fact]
        public async Task VectorQuery_Rerank_RequiresQueryString()
        {
            using var fixture = await TestFixture.CreateHybridFixture("vec_rerank_no_qs");
            Assert.Throws<ArgumentException>(() =>
                fixture.Table.Query()
                    .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                    .Rerank(new VectorTestReranker()));
        }

        [Fact]
        public async Task IReranker_DefaultRerankFts_ThrowsNotSupported()
        {
            var reranker = new RRFReranker();
            var batch = new RecordBatch(
                new Schema(new[] { new Field("x", Int32Type.Default, false) }, null),
                new IArrowArray[] { new Int32Array.Builder().Append(1).Build() }, 1);
            await Assert.ThrowsAsync<NotSupportedException>(
                () => reranker.RerankFts("test", batch));
        }

        [Fact]
        public async Task IReranker_DefaultRerankVector_ThrowsNotSupported()
        {
            var reranker = new RRFReranker();
            var batch = new RecordBatch(
                new Schema(new[] { new Field("x", Int32Type.Default, false) }, null),
                new IArrowArray[] { new Int32Array.Builder().Append(1).Build() }, 1);
            await Assert.ThrowsAsync<NotSupportedException>(
                () => reranker.RerankVector("test", batch));
        }

        // ----- HybridQuery Tests -----

        [Fact]
        public async Task HybridQuery_FromFTSQuery_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_from_fts");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_FromVectorQuery_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_from_vec");
            var results = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .NearestToText("apple")
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_FromTableEntry_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_table");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.True(results.Length > 0);
        }

        [Fact]
        public async Task HybridQuery_DefaultReranker_IsRRF()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_default_rrf");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_with_rowid");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_rrf");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_linear");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_retscore_rel");

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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_retscore_all");

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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_limit");
            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Limit(1)
                .ToArrow();
            Assert.True(results.Length <= 1);
        }

        /// <summary>
        /// Verifies that limit is propagated to sub-queries, matching Python behavior.
        /// With limit=2 on a 5-item table, each sub-query returns at most 2 rows,
        /// resulting in different RRF scores than when all 5 candidates are ranked.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified by running lancedb python 0.33.0, exact fixture data):
        /// <code>
        /// data = [
        ///     {"id": 1, "content": "apple apple apple banana fruit", "vector": [1.0, 0.0]},
        ///     {"id": 2, "content": "cherry date sweet",              "vector": [0.0, 1.0]},
        ///     {"id": 3, "content": "apple apple cherry tart",        "vector": [0.5, 0.5]},
        ///     {"id": 4, "content": "banana fig jam",                 "vector": [0.9, 0.1]},
        ///     {"id": 5, "content": "apple pie dessert",              "vector": [0.0, 0.9]},
        /// ]
        /// # limit=2: vector top-2 = {id1, id4}, fts top-2 = {id1, id3}.
        /// # RRF (k=60): id1 = 1/61 + 1/61 = 0.032787 (in both sub-queries);
        /// #             id3 and id4 each appear once at rank 2 = 1/62 = 0.016129 (a tie).
        /// # So id1 ranks first unambiguously; the runner-up score is 1/62.
        /// # Without limit propagation each sub-query would rank all 5 rows and id1
        /// # would instead score 2/61 against a different candidate set.
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_LimitPropagatedToSubQueries_MatchesPython()
        {
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_lim_prop");

            var results = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0 })
                .Limit(2)
                .ToArrow();

            Assert.Equal(2, results.Length);

            // id1 wins unambiguously (present in both sub-queries' top-2).
            // The runner-up is one of id3/id4, which tie at RRF 1/62; the exact
            // winner depends on the FTS/vector engine's tie-ordering, so we assert
            // the tie-tolerant facts: id1 first at 1/61+1/61, runner-up at 1/62.
            var ids = (Int32Array)results.Column("id");
            var scores = (FloatArray)results.Column("_relevance_score");
            Assert.Equal(1, ids.GetValue(0)!.Value);
            Assert.Contains(ids.GetValue(1)!.Value, new[] { 3, 4 });
            Assert.Equal(0.032787f, scores.GetValue(0)!.Value, precision: 4);
            Assert.Equal(0.016129f, scores.GetValue(1)!.Value, precision: 4);
        }
        [Fact]
        public async Task HybridQuery_OffsetThenLimit_ReturnsCorrectCount()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_off_lim");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_where");
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

        /// <summary>
        /// Verifies that a hybrid query with a hard filter that matches no rows
        /// returns an empty result with a <c>_relevance_score</c> column, rather
        /// than crashing in the reranker pipeline.
        /// </summary>
        /// <remarks>
        /// When both vector and FTS sub-queries return zero rows, the hybrid
        /// reranker pipeline must produce an empty result table with a
        /// <c>_relevance_score</c> column attached.
        /// </remarks>
        [Fact]
        public async Task HybridQuery_WhereMatchesNothing_ReturnsEmptyWithRelevanceScore()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_where_empty");

            foreach (var reranker in new IReranker[]
            {
                new RRFReranker(),
                new LinearCombinationReranker(),
                new MRRReranker(),
            })
            {
                var results = await fixture.Table.Query()
                    .NearestToText("apple")
                    .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                    .Where("id = 999")
                    .Rerank(reranker)
                    .ToArrow();

                Assert.Equal(0, results.Length);
                Assert.Contains(results.Schema.FieldsList, f => f.Name == "_relevance_score");
                Assert.DoesNotContain(results.Schema.FieldsList, f => f.Name == "_rowid");
            }
        }

        /// <summary>
        /// Same as <see cref="HybridQuery_WhereMatchesNothing_ReturnsEmptyWithRelevanceScore"/>,
        /// but exercises <see cref="HybridQuery.ToList"/> and
        /// <see cref="HybridQuery.ToBatches"/> to ensure the empty-result path
        /// works across all output adapters without throwing.
        /// </summary>
        [Fact]
        public async Task HybridQuery_WhereMatchesNothing_ToListAndToBatches_ReturnEmpty()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_where_empty_outputs");

            var list = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Where("id = 999")
                .ToList();
            Assert.Empty(list);

            using var reader = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Where("id = 999")
                .ToBatches();
            int rowCount = 0;
            await foreach (var batch in reader)
            {
                rowCount += batch.Length;
                batch.Dispose();
            }
            Assert.Equal(0, rowCount);
        }

        [Fact]
        public async Task HybridQuery_WithSelect_ProjectsColumns()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_select");
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
            using var fixture = await TestFixture.CreateHybridFixtureWithPrice("hybrid_sel_expr");
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
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_tolist");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .ToList();
            Assert.True(results.Count > 0);
            Assert.Contains("_relevance_score", results[0].Keys);
        }

        /// <summary>
        /// Verifies DistanceRange filters vector results by distance bounds.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// hq = (table.query()
        ///     .nearest_to([1.0, 0.0, 0.0])
        ///     .nearest_to_text("apple")
        ///     .rerank(RRFReranker())
        ///     .distance_range(upper_bound=1.5))
        /// r = await hq.to_arrow()
        /// # Returns 2 rows (only those within distance &lt; 1.5)
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_DistanceRange_FiltersResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_distrange");
            // Without distance range: all 3 rows
            var allResults = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.Equal(3, allResults.Length);

            // With upper bound: fewer rows (only close vectors)
            var filtered = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .DistanceRange(upperBound: 1.5f)
                .ToArrow();
            Assert.True(filtered.Length < allResults.Length);
            Assert.True(filtered.Length > 0);
        }

        /// <summary>
        /// Verifies that <see cref="VectorQuery.DistanceRange"/> set BEFORE the
        /// vector query is promoted to a hybrid query (via
        /// <see cref="VectorQuery.NearestToText(string, string[]?)"/>) is carried
        /// over to the final hybrid execution.
        /// </summary>
        /// <remarks>
        /// Regression: previously, a vector-first chain such as
        /// <c>NearestTo(vec).DistanceRange(...).NearestToText("apple")</c>
        /// silently dropped the distance range because
        /// <see cref="HybridQuery"/>'s internal copy of <see cref="VectorQuery"/>
        /// state did not include the distance-range bounds. The FTS-first chain
        /// covered by <see cref="HybridQuery_DistanceRange_FiltersResults"/> was
        /// unaffected because it sets the range directly on
        /// <see cref="HybridQuery"/>.
        /// </remarks>
        [Fact]
        public async Task HybridQuery_DistanceRangeBeforePromotion_IsApplied()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_distrange_vec_first");

            // Vector-first chain: DistanceRange set on VectorQuery, then promoted via NearestToText.
            var filtered = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .DistanceRange(upperBound: 1.5f)
                .NearestToText("apple")
                .ToArrow();

            // FTS-first chain produces the same filter — used as the expected result.
            var expected = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .DistanceRange(upperBound: 1.5f)
                .ToArrow();

            Assert.Equal(expected.Length, filtered.Length);
            Assert.True(filtered.Length > 0);
            Assert.True(filtered.Length < 3, "DistanceRange should drop at least one of the 3 fixture rows.");
        }

        /// <summary>
        /// Verifies ExplainPlan returns combined vector and FTS plans.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// plan = await hq.explain_plan(verbose=True)
        /// # Returns string with "Vector Search Plan:" and "FTS Search Plan:" sections
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_ExplainPlan_ReturnsBothPlans()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_explain");
            var plan = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ExplainPlan(verbose: true);

            Assert.Contains("Vector Search Plan:", plan);
            Assert.Contains("FTS Search Plan:", plan);
        }

        /// <summary>
        /// Verifies AnalyzePlan returns combined vector and FTS execution metrics.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// plan = await hq.analyze_plan()
        /// # Returns string with "Vector Search Query:" and "FTS Search Query:" sections
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_AnalyzePlan_ReturnsBothPlans()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_analyze");
            var plan = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .AnalyzePlan();

            Assert.Contains("Vector Search Query:", plan);
            Assert.Contains("FTS Search Query:", plan);
        }

        /// <summary>
        /// Verifies ToBatches returns a streaming reader over hybrid results.
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified with lancedb pip v0.29.2):
        /// <code>
        /// reader = await hq.to_batches()
        /// batches = [b async for b in reader]
        /// # Returns batches with _relevance_score column
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_ToBatches_ReturnsStreamingResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_batches");
            var batches = new System.Collections.Generic.List<RecordBatch>();
            using var reader = await fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToBatches();
            await foreach (var batch in reader)
            {
                batches.Add(batch);
            }
            Assert.True(batches.Count > 0);
            int totalRows = 0;
            foreach (var b in batches)
            {
                totalRows += b.Length;
                Assert.Contains(b.Schema.FieldsList, f => f.Name == "_relevance_score");
            }
            Assert.True(totalRows > 0);
        }

        /// <summary>
        /// ToBatches with maxBatchLength on a hybrid query should split the reranked
        /// result into batches no larger than the limit. Hybrid results are
        /// materialized before chunking, so this verifies the client-side splitting
        /// honors max_batch_length.
        /// </summary>
        [Fact]
        public async Task HybridQuery_ToBatches_RespectsMaxBatchLength()
        {
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_batches_maxlen");

            int unboundedTotal = 0;
            using (var fullReader = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0 })
                .ToBatches())
            {
                await foreach (var batch in fullReader)
                {
                    unboundedTotal += batch.Length;
                }
            }
            Assert.True(unboundedTotal > 2, "fixture must return enough rows to require splitting");

            using var reader = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0 })
                .ToBatches(maxBatchLength: 2);

            int totalRows = 0;
            int batchCount = 0;
            await foreach (var batch in reader)
            {
                Assert.NotNull(batch);
                Assert.True(batch.Length <= 2, $"batch of {batch.Length} exceeds maxBatchLength 2");
                totalRows += batch.Length;
                batchCount++;
            }

            Assert.Equal(unboundedTotal, totalRows);
            Assert.True(batchCount > 1, "expected the result to be split into multiple batches");
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
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_empty_fts_lc");
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
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_empty_fts_lc_all");
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
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_empty_fts_rrf");
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
        /// Python equivalent (verified by running lancedb python 0.33.0, which
        /// contains the LinearCombinationReranker score-inversion fix):
        /// <code>
        /// db = lancedb.connect("/tmp/test")
        /// data = [
        ///     {"id": 1, "content": "apple apple apple banana fruit", "vector": [1.0, 0.0]},
        ///     {"id": 2, "content": "cherry date sweet",              "vector": [0.0, 1.0]},
        ///     {"id": 3, "content": "apple apple cherry tart",        "vector": [0.5, 0.5]},
        ///     {"id": 4, "content": "banana fig jam",                 "vector": [0.9, 0.1]},
        ///     {"id": 5, "content": "apple pie dessert",              "vector": [0.0, 0.9]},
        /// ]
        /// table = db.create_table("test", data)
        /// table.create_fts_index("content", use_tantivy=False)
        /// reranker = LinearCombinationReranker(weight=0.5, fill=1.0)
        /// results = (table.search(query_type="hybrid")
        ///     .vector([1.0, 0.0]).text("apple")
        ///     .rerank(reranker).limit(5).to_arrow())
        ///
        /// # Vector distances: id1=0.0, id4=0.02, id3=0.5, id5=1.81, id2=2.0.
        /// # FTS BM25 scores:  id1=0.7818, id3=0.7187, id5=0.5784 (distinct).
        /// # After min-max normalization, relevance = 0.5*(1 - normDist) + 0.5*normScore:
        /// #   id=1, _relevance_score=1.0000
        /// #   id=3, _relevance_score=0.7197
        /// #   id=4, _relevance_score=0.4950
        /// #   id=5, _relevance_score=0.0475
        /// #   id=2, _relevance_score=0.0000
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_LinearCombination_NormalizesScoresBeforeReranking()
        {
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_norm");
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

            var expectedIds = new int[] { 1, 3, 4, 5, 2 };
            var expectedScores = new float[] { 1.0000f, 0.7197f, 0.4950f, 0.0475f, 0.0000f };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
                Assert.True(Math.Abs(expectedScores[i] - scores.GetValue(i)!.Value) < 1e-3f,
                    $"Score mismatch at {i}: expected {expectedScores[i]}, got {scores.GetValue(i)}");
            }
        }

        /// <summary>
        /// Verifies that normalize="rank" converts scores to ordinal ranks before
        /// min-max normalization, producing different rankings than normalize="score".
        /// </summary>
        /// <remarks>
        /// Python equivalent (verified by running lancedb python 0.33.0, which
        /// contains the LinearCombinationReranker score-inversion fix):
        /// <code>
        /// # Same data as NormalizesScoresBeforeReranking test
        /// reranker = LinearCombinationReranker(weight=0.5, fill=1.0)
        /// results = (table.search(query_type="hybrid")
        ///     .vector([1.0, 0.0]).text("apple")
        ///     .rerank(reranker, normalize="rank").limit(5).to_arrow())
        ///
        /// # Vector ranks (ascending distance) normalize to: id1=0, id4=0.25,
        /// # id3=0.5, id5=0.75, id2=1.0. FTS ranks (ascending BM25 id5&lt;id3&lt;id1)
        /// # normalize to: id5=0, id3=0.5, id1=1.0. The distinct BM25 scores make
        /// # this deterministic (independent of FTS tie-ordering). Output:
        /// #   id=1, _relevance_score=1.0000
        /// #   id=3, _relevance_score=0.5000
        /// #   id=4, _relevance_score=0.3750
        /// #   id=5, _relevance_score=0.1250
        /// #   id=2, _relevance_score=0.0000
        /// </code>
        /// </remarks>
        [Fact]
        public async Task HybridQuery_LinearCombination_NormalizeRank_MatchesPython()
        {
            using var fixture = await TestFixture.CreateNormalizationFixture("hybrid_rank_norm");
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

            var expectedIds = new int[] { 1, 3, 4, 5, 2 };
            var expectedScores = new float[] { 1.0000f, 0.5000f, 0.3750f, 0.1250f, 0.0000f };
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expectedIds[i], ids.GetValue(i));
                Assert.True(Math.Abs(expectedScores[i] - scores.GetValue(i)!.Value) < 1e-3f,
                    $"Score mismatch at {i}: expected {expectedScores[i]}, got {scores.GetValue(i)}");
            }
        }

        /// <summary>
        /// Validates that HybridQuery throws if a custom reranker omits _relevance_score.
        /// Python equivalent: check_reranker_result() raises ValueError.
        /// </summary>
        [Fact]
        public async Task HybridQuery_BadReranker_ThrowsInvalidOperation()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_bad_reranker");
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await fixture.Table
                    .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                    .Rerank(new BadReranker())
                    .ToArrow();
            });
            Assert.Contains("_relevance_score", ex.Message);
        }

        /// <summary>
        /// Validates that RRFReranker passes the _relevance_score validation.
        /// </summary>
        [Fact]
        public async Task HybridQuery_RRFReranker_PassesValidation()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_rrf_validation");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new RRFReranker())
                .ToArrow();

            Assert.True(results.Length > 0);
            Assert.True(results.Schema.GetFieldIndex("_relevance_score") >= 0);
        }

        /// <summary>
        /// Validates that LinearCombinationReranker passes the _relevance_score validation.
        /// </summary>
        [Fact]
        public async Task HybridQuery_LinearCombinationReranker_PassesValidation()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_lc_validation");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new LinearCombinationReranker())
                .ToArrow();

            Assert.True(results.Length > 0);
            Assert.True(results.Schema.GetFieldIndex("_relevance_score") >= 0);
        }

        /// <summary>
        /// Validates that MRRReranker passes the _relevance_score validation.
        /// </summary>
        [Fact]
        public async Task HybridQuery_MRRReranker_PassesValidation()
        {
            using var fixture = await TestFixture.CreateHybridFixture("hybrid_mrr_validation");
            var results = await fixture.Table
                .HybridSearch("apple", new double[] { 1.0, 0.0, 0.0 })
                .Rerank(new MRRReranker())
                .ToArrow();

            Assert.True(results.Length > 0);
            Assert.True(results.Schema.GetFieldIndex("_relevance_score") >= 0);
        }

    }

    /// <summary>
    /// A reranker that deliberately omits the _relevance_score column,
    /// used to test validation of reranker output.
    /// </summary>
    internal class BadReranker : IReranker
    {
        public Task<RecordBatch> RerankHybrid(
            string query, RecordBatch vectorResults, RecordBatch ftsResults)
        {
            // Return a batch with no _relevance_score column
            var idArray = new Int64Array.Builder().Append(1).Build();
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, false)
            }, null);
            return Task.FromResult(new RecordBatch(schema, new IArrowArray[] { idArray }, 1));
        }

        public Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults)
        {
            throw new NotSupportedException();
        }

        public Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults)
        {
            throw new NotSupportedException();
        }
    }

    /// <summary>
    /// A test reranker that implements RerankFts — adds a _relevance_score column
    /// with descending scores based on row position.
    /// </summary>
    internal class FtsTestReranker : IReranker
    {
        public Task<RecordBatch> RerankHybrid(
            string query, RecordBatch vectorResults, RecordBatch ftsResults)
        {
            throw new NotSupportedException();
        }

        public Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults)
        {
            return Task.FromResult(AddRelevanceScore(ftsResults));
        }

        public Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults)
        {
            throw new NotSupportedException();
        }

    }

    /// <summary>
    /// A test reranker that implements RerankVector — adds a _relevance_score column
    /// with descending scores based on row position.
    /// </summary>
    internal class VectorTestReranker : IReranker
    {
        public Task<RecordBatch> RerankHybrid(
            string query, RecordBatch vectorResults, RecordBatch ftsResults)
        {
            throw new NotSupportedException();
        }

        public Task<RecordBatch> RerankFts(string query, RecordBatch ftsResults)
        {
            throw new NotSupportedException();
        }

        public Task<RecordBatch> RerankVector(string query, RecordBatch vectorResults)
        {
            return Task.FromResult(AddRelevanceScore(vectorResults));
        }

    }
}
