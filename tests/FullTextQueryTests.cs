namespace lancedb.tests
{
    using System;
    using System.Linq;
    using System.Text.Json.Nodes;

    /// <summary>
    /// Unit tests for the <see cref="FullTextQuery"/> hierarchy JSON serialization
    /// and argument validation.
    /// </summary>
    public class FullTextQueryTests
    {
        private static JsonObject Parse(FullTextQuery query)
        {
            return (JsonObject)JsonNode.Parse(query.ToJson())!;
        }

        [Fact]
        public void MatchQuery_ToJson_MatchesExpectedShape()
        {
            var json = Parse(new MatchQuery("puppy", "text", fuzziness: 2));
            var match = (JsonObject)json["match"]!;

            Assert.Equal("text", (string?)match["column"]);
            Assert.Equal("puppy", (string?)match["terms"]);
            Assert.Equal(1.0f, (float)match["boost"]!);
            Assert.Equal(2, (int)match["fuzziness"]!);
            Assert.Equal(50, (int)match["max_expansions"]!);
            Assert.Equal("Or", (string?)match["operator"]);
            Assert.Equal(0, (int)match["prefix_length"]!);
        }

        [Fact]
        public void MatchQuery_DefaultFuzziness_EmitsZeroExact()
        {
            var match = (JsonObject)Parse(new MatchQuery("puppy", "text"))["match"]!;
            Assert.NotNull(match["fuzziness"]);
            Assert.Equal(0, (int)match["fuzziness"]!);
        }

        [Fact]
        public void MatchQuery_NullFuzziness_EmitsJsonNull()
        {
            var match = (JsonObject)Parse(new MatchQuery("puppy", "text", fuzziness: null))["match"]!;
            Assert.True(match.ContainsKey("fuzziness"));
            Assert.Null(match["fuzziness"]);
        }

        [Fact]
        public void MatchQuery_AndOperator_SerializesCapitalized()
        {
            var match = (JsonObject)Parse(
                new MatchQuery("puppy", "text", @operator: FullTextOperator.And))["match"]!;
            Assert.Equal("And", (string?)match["operator"]);
        }

        [Fact]
        public void MatchQuery_NegativeFuzziness_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new MatchQuery("puppy", "text", fuzziness: -1));
        }

        [Fact]
        public void MatchQuery_NonFiniteBoost_Throws()
        {
            Assert.Throws<ArgumentException>(
                () => new MatchQuery("puppy", "text", boost: float.NaN));
        }

        [Fact]
        public void MatchQuery_NullColumn_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new MatchQuery("puppy", null!));
        }

        [Fact]
        public void PhraseQuery_ToJson_MatchesExpectedShape()
        {
            var phrase = (JsonObject)Parse(new PhraseQuery("quick brown", "text", slop: 2))["phrase"]!;
            Assert.Equal("text", (string?)phrase["column"]);
            Assert.Equal("quick brown", (string?)phrase["terms"]);
            Assert.Equal(2, (int)phrase["slop"]!);
        }

        [Fact]
        public void PhraseQuery_NegativeSlop_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new PhraseQuery("x", "text", slop: -1));
        }

        [Fact]
        public void BoostQuery_ToJson_NestsChildQueries()
        {
            var query = new BoostQuery(
                new MatchQuery("cat", "text"),
                new MatchQuery("dog", "text"),
                negativeBoost: 0.25f);
            var boost = (JsonObject)Parse(query)["boost"]!;

            Assert.Equal(0.25f, (float)boost["negative_boost"]!);
            Assert.Equal("cat", (string?)((JsonObject)((JsonObject)boost["positive"]!)["match"]!)["terms"]);
            Assert.Equal("dog", (string?)((JsonObject)((JsonObject)boost["negative"]!)["match"]!)["terms"]);
        }

        [Fact]
        public void MultiMatchQuery_ToJson_MatchesExpectedShape()
        {
            var query = new MultiMatchQuery("recipe", new[] { "title", "body" }, new[] { 1.0f, 2.0f });
            var mm = (JsonObject)Parse(query)["multi_match"]!;

            Assert.Equal("recipe", (string?)mm["query"]);
            Assert.Equal(new[] { "title", "body" }, ((JsonArray)mm["columns"]!).Select(n => (string?)n));
            Assert.Equal(new[] { 1.0f, 2.0f }, ((JsonArray)mm["boost"]!).Select(n => (float)n!));
            Assert.Equal("Or", (string?)mm["operator"]);
        }

        [Fact]
        public void MultiMatchQuery_AndOperator_EmitsAnd()
        {
            var query = new MultiMatchQuery(
                "recipe", new[] { "title", "body" }, @operator: FullTextOperator.And);
            var mm = (JsonObject)Parse(query)["multi_match"]!;

            Assert.Equal("And", (string?)mm["operator"]);
        }

        [Fact]
        public void MultiMatchQuery_NoBoosts_OmitsBoostKey()
        {
            var mm = (JsonObject)Parse(new MultiMatchQuery("recipe", new[] { "title", "body" }))["multi_match"]!;
            Assert.False(mm.ContainsKey("boost"));
        }

        [Fact]
        public void MultiMatchQuery_BoostsLengthMismatch_Throws()
        {
            Assert.Throws<ArgumentException>(
                () => new MultiMatchQuery("recipe", new[] { "title", "body" }, new[] { 1.0f }));
        }

        [Fact]
        public void MultiMatchQuery_EmptyColumns_Throws()
        {
            Assert.Throws<ArgumentException>(() => new MultiMatchQuery("recipe", Array.Empty<string>()));
        }

        [Fact]
        public void BooleanQuery_ToJson_GroupsByOccur()
        {
            var query = new BooleanQuery(new[]
            {
                (Occur.Must, (FullTextQuery)new MatchQuery("cat", "text")),
                (Occur.Should, new MatchQuery("dog", "text")),
                (Occur.MustNot, new MatchQuery("fish", "text")),
            });
            var boolean = (JsonObject)Parse(query)["boolean"]!;

            Assert.Single((JsonArray)boolean["must"]!);
            Assert.Single((JsonArray)boolean["should"]!);
            Assert.Single((JsonArray)boolean["must_not"]!);
        }

        [Fact]
        public void AndOperator_ProducesMustBooleanQuery()
        {
            var query = new MatchQuery("cat", "text") & new MatchQuery("dog", "text");
            var boolean = (JsonObject)Parse(query)["boolean"]!;
            Assert.Equal(2, ((JsonArray)boolean["must"]!).Count);
            Assert.Empty((JsonArray)boolean["should"]!);
        }

        [Fact]
        public void OrOperator_ProducesShouldBooleanQuery()
        {
            var query = new MatchQuery("cat", "text") | new MatchQuery("dog", "text");
            var boolean = (JsonObject)Parse(query)["boolean"]!;
            Assert.Equal(2, ((JsonArray)boolean["should"]!).Count);
            Assert.Empty((JsonArray)boolean["must"]!);
        }

        [Fact]
        public void And_NullOther_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new MatchQuery("cat", "text").And(null!));
        }
    }
}
