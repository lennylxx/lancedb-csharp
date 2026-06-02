namespace lancedb.tests
{
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Tests for the convenience search entry points <see cref="Table.Search(float[], string)"/>,
    /// <see cref="Table.Search(string, string[])"/>, <see cref="Table.Search(FullTextQuery)"/>, and
    /// <see cref="Table.VectorSearch(float[])"/>, which mirror the Python SDK's
    /// <c>AsyncTable.search()</c> / <c>AsyncTable.vector_search()</c> helpers.
    /// </summary>
    public class SearchEntryPointTests
    {
        [Fact]
        public async Task VectorSearch_FloatVector_ReturnsNearestRow()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_vec_float");

            using var query = fixture.Table.VectorSearch(new float[] { 1.0f, 0.0f, 0.0f }).Limit(1);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(0, rows[0]["id"]);
        }

        [Fact]
        public async Task VectorSearch_DoubleVector_ReturnsNearestRow()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_vec_double");

            using var query = fixture.Table.VectorSearch(new double[] { 0.0, 1.0, 0.0 }).Limit(1);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(1, rows[0]["id"]);
        }

        [Fact]
        public async Task VectorSearch_ReturnsVectorQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_vec_type");

            var query = fixture.Table.VectorSearch(new float[] { 1.0f, 0.0f, 0.0f });

            Assert.IsType<VectorQuery>(query);
        }

        [Fact]
        public async Task Search_FloatVector_ReturnsNearestRow()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_vec");

            using var query = fixture.Table.Search(new float[] { 0.0f, 0.0f, 1.0f }).Limit(1);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(2, rows[0]["id"]);
        }

        [Fact]
        public async Task Search_DoubleVector_ReturnsNearestRow()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_vec_d");

            using var query = fixture.Table.Search(new double[] { 1.0, 0.0, 0.0 }).Limit(1);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(0, rows[0]["id"]);
        }

        [Fact]
        public async Task Search_VectorWithColumnName_ReturnsNearestRow()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_veccol");

            using var query = fixture.Table
                .Search(new float[] { 0.0f, 1.0f, 0.0f }, vectorColumnName: "vector")
                .Limit(1);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(1, rows[0]["id"]);
        }

        [Fact]
        public async Task Search_Vector_ReturnsVectorQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_vectype");

            var query = fixture.Table.Search(new float[] { 1.0f, 0.0f, 0.0f });

            Assert.IsType<VectorQuery>(query);
        }

        [Fact]
        public async Task Search_String_PerformsFullTextSearch()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_str");

            using var query = fixture.Table.Search("apple").Limit(10);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana fruit", rows[0]["content"]);
        }

        [Fact]
        public async Task Search_StringWithColumns_PerformsFullTextSearch()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_str_col");

            using var query = fixture.Table.Search("cherry", ftsColumns: new[] { "content" }).Limit(10);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("cherry date sweet", rows[0]["content"]);
        }

        [Fact]
        public async Task Search_String_ReturnsFtsQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_strtype");

            var query = fixture.Table.Search("apple");

            Assert.IsType<FTSQuery>(query);
        }

        [Fact]
        public async Task Search_FullTextQuery_PerformsFullTextSearch()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_ftq");

            using var query = fixture.Table
                .Search(new MatchQuery("elderberry", "content"))
                .Limit(10);
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("elderberry fig tart", rows[0]["content"]);
        }

        [Fact]
        public async Task Search_FullTextQuery_ReturnsFtsQuery()
        {
            using var fixture = await TestFixture.CreateHybridFixture("se_search_ftqtype");

            var query = fixture.Table.Search(new MatchQuery("apple", "content"));

            Assert.IsType<FTSQuery>(query);
        }
    }
}
