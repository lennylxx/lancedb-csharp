namespace lancedb.tests
{
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// End-to-end execution tests for structured <see cref="FullTextQuery"/> search
    /// via <see cref="Query.NearestToText(FullTextQuery)"/> and the hybrid overload.
    /// </summary>
    public class FullTextQueryExecutionTests
    {
        [Fact]
        public async Task NearestToText_MatchQuery_ReturnsMatchingRows()
        {
            using var fixture = await TestFixture.CreateTextFixture("ftq_match");
            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query().NearestToText(new MatchQuery("apple", "content"));
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        [Fact]
        public async Task NearestToText_PhraseQuery_ReturnsMatchingRows()
        {
            using var fixture = await TestFixture.CreateTextFixture("ftq_phrase");
            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex { WithPosition = true });

            using var query = fixture.Table.Query()
                .NearestToText(new PhraseQuery("apple banana", "content"));
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        [Fact]
        public async Task NearestToText_BooleanQuery_RespectsMustNot()
        {
            using var fixture = await TestFixture.CreateTextFixture("ftq_bool");
            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = new BooleanQuery(new[]
            {
                (Occur.Should, (FullTextQuery)new MatchQuery("apple", "content")),
                (Occur.Should, new MatchQuery("cherry", "content")),
                (Occur.MustNot, new MatchQuery("banana", "content")),
            });

            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            Assert.Single(rows);
            Assert.Equal("cherry date", rows[0]["content"]);
        }

        [Fact]
        public async Task NearestToText_AndOperator_RequiresBothTerms()
        {
            using var fixture = await TestFixture.CreateTextFixture("ftq_and");
            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = new MatchQuery("apple", "content") & new MatchQuery("banana", "content");
            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        [Fact]
        public async Task NearestToText_MultiMatchQuery_SearchesMultipleColumns()
        {
            using var fixture = await TestFixture.CreateMultiTextFixture("ftq_multi");
            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            var query = new MultiMatchQuery("apple", new[] { "title", "body" });
            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            // "apple pie" (title row 0) and "apple sauce recipe" (body row 1) both match.
            Assert.Equal(2, rows.Count);
        }

        [Fact]
        public async Task NearestToText_MultiMatchOrOperator_MatchesAnyTerm()
        {
            using var fixture = await TestFixture.CreateMultiTextFixture("ftq_multi_or");
            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            // OR (default): every body contains "recipe", so all three rows match.
            var query = new MultiMatchQuery("apple recipe", new[] { "title", "body" });
            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            Assert.Equal(3, rows.Count);
        }

        [Fact]
        public async Task NearestToText_MultiMatchAndOperator_RequiresAllTerms()
        {
            using var fixture = await TestFixture.CreateMultiTextFixture("ftq_multi_and");
            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            // AND requires both "apple" and "recipe" to be present. Row 2
            // ("cherry cake" / "fig jam recipe") has no "apple", so it is excluded;
            // rows 0 and 1 remain. This differs from the OR result (all three rows).
            var query = new MultiMatchQuery(
                "apple recipe", new[] { "title", "body" }, @operator: FullTextOperator.And);
            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            Assert.Equal(2, rows.Count);
            var ids = rows.Select(r => (int)r["id"]!).OrderBy(i => i).ToArray();
            Assert.Equal(new[] { 0, 1 }, ids);
        }

        [Fact]
        public async Task NearestToText_BoostQuery_Executes()
        {
            using var fixture = await TestFixture.CreateTextFixture("ftq_boost");
            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = new BoostQuery(
                new MatchQuery("apple", "content"),
                new MatchQuery("banana", "content"),
                negativeBoost: 0.1f);
            using var q = fixture.Table.Query().NearestToText(query);
            var rows = await q.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        [Fact]
        public async Task NearestToText_StructuredHybrid_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateHybridFixture("ftq_hybrid");

            var hybrid = fixture.Table.Query()
                .NearestTo(new float[] { 1.0f, 0.0f, 0.0f })
                .NearestToText(new MatchQuery("apple", "content"));
            var rows = await hybrid.ToList();

            Assert.NotEmpty(rows);
            Assert.Contains(rows, r => (string?)r["content"] == "apple banana fruit");
        }
    }
}
