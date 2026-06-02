namespace lancedb.tests
{
    using System.Threading.Tasks;

    /// <summary>
    /// Tests for <see cref="FTSQuery.QueryString"/>, mirroring the Python SDK's
    /// <c>AsyncFTSQuery.get_query()</c> accessor.
    /// </summary>
    public class FtsQueryStringTests
    {
        [Fact]
        public async Task QueryString_PlainString_ReturnsString()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_plain");
            var query = fixture.Table.Query().NearestToText("apple banana");
            Assert.Equal("apple banana", query.QueryString);
        }

        [Fact]
        public async Task QueryString_MatchQuery_ReturnsTerms()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_match");
            var query = fixture.Table.Query().NearestToText(new MatchQuery("apple", "content"));
            Assert.Equal("apple", query.QueryString);
        }

        [Fact]
        public async Task QueryString_PhraseQuery_ReturnsQuotedTerms()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_phrase");
            var query = fixture.Table.Query().NearestToText(new PhraseQuery("apple banana", "content"));
            Assert.Equal("\"apple banana\"", query.QueryString);
        }

        [Fact]
        public async Task QueryString_BoostQuery_ReturnsPositiveQuery()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_boost");
            var boost = new BoostQuery(
                new MatchQuery("apple", "content"),
                new MatchQuery("banana", "content"));
            var query = fixture.Table.Query().NearestToText(boost);
            Assert.Equal("apple", query.QueryString);
        }

        [Fact]
        public async Task QueryString_MultiMatchQuery_ReturnsTerms()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_multi");
            var query = fixture.Table.Query()
                .NearestToText(new MultiMatchQuery("apple", new[] { "title", "body" }));
            Assert.Equal("apple", query.QueryString);
        }

        [Fact]
        public async Task QueryString_BooleanQuery_ReturnsEmpty()
        {
            using var fixture = await TestFixture.CreateWithTable("qs_bool");
            var boolean = new BooleanQuery(new[]
            {
                (Occur.Should, (FullTextQuery)new MatchQuery("apple", "content")),
                (Occur.Should, new MatchQuery("banana", "content")),
            });
            var query = fixture.Table.Query().NearestToText(boolean);
            Assert.Equal(string.Empty, query.QueryString);
        }
    }
}
