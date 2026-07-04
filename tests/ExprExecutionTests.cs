namespace lancedb.tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using lancedb;
    using Xunit;
    using static TestHelpers;

    /// <summary>
    /// End-to-end tests that execute <see cref="Expr"/>-based filters and projections
    /// against a real table to verify the generated SQL round-trips through the engine.
    /// </summary>
    public class ExprExecutionTests
    {
        private static async Task<int> CountAsync(Query query)
        {
            var batch = await query.ToArrow();
            return batch.Length;
        }

        [Fact]
        public async Task Where_Expr_FiltersRows()
        {
            // CreateTwoColumnBatch: ids 0..9, names name_0..name_9
            using var fixture = await TestFixture.CreateWithTable("expr_where", CreateTwoColumnBatch(10));

            using var query = fixture.Table.Query().Where(Expr.Col("id") > 4);
            Assert.Equal(5, await CountAsync(query));
        }

        [Fact]
        public async Task Where_IsIn_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_isin", CreateTwoColumnBatch(10));

            using var query = fixture.Table.Query().Where(Expr.Col("id").IsIn(1, 3, 5, 7));
            Assert.Equal(4, await CountAsync(query));
        }

        [Fact]
        public async Task Where_AndOr_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_andor", CreateTwoColumnBatch(10));

            var predicate = (Expr.Col("id") >= 2).And(Expr.Col("id") < 5);
            using var query = fixture.Table.Query().Where(predicate);
            Assert.Equal(3, await CountAsync(query));
        }

        [Fact]
        public async Task Where_EqString_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_eqstr", CreateTwoColumnBatch(10));

            using var query = fixture.Table.Query().Where(Expr.Col("name").Eq("name_3"));
            Assert.Equal(1, await CountAsync(query));
        }

        [Fact]
        public async Task Where_StringFunction_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_strfn", CreateTwoColumnBatch(10));

            using var query = fixture.Table.Query().Where(Expr.Col("name").Contains("name_7"));
            Assert.Equal(1, await CountAsync(query));
        }

        [Fact]
        public async Task Where_Cast_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_cast", CreateTwoColumnBatch(10));

            using var query = fixture.Table.Query().Where(Expr.Col("id").Cast("int64") > 7);
            Assert.Equal(2, await CountAsync(query));
        }

        [Fact]
        public async Task Where_NestedStructField_UsesStringFilter()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_nested", CreateNestedVectorBatch(4));

            using var query = fixture.Table.Query().Where("meta.label = 'label1'");
            Assert.Equal(1, await CountAsync(query));
        }

        [Fact]
        public async Task Select_Expr_ComputesDerivedColumn()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_select", CreateTwoColumnBatch(3));

            using var query = fixture.Table.Query()
                .Select(new Dictionary<string, Expr>
                {
                    ["id"] = Expr.Col("id"),
                    ["doubled"] = Expr.Col("id") * 2,
                });
            var rows = await query.ToList();

            Assert.Equal(3, rows.Count);
            Assert.All(rows, r => Assert.True(r.ContainsKey("doubled")));
            var byId = rows.ToDictionary(r => System.Convert.ToInt32(r["id"]), r => System.Convert.ToInt32(r["doubled"]));
            Assert.Equal(0, byId[0]);
            Assert.Equal(2, byId[1]);
            Assert.Equal(4, byId[2]);
        }

        [Fact]
        public async Task VectorQuery_Where_Expr_FiltersRows()
        {
            using var fixture = await TestFixture.CreateWithTable("expr_vq", CreateVectorBatch(10, 4));

            using var query = fixture.Table
                .VectorSearch(new float[] { 0f, 0f, 0f, 0f })
                .Where(Expr.Col("id") < 3)
                .Limit(10);
            var batch = await query.ToArrow();
            Assert.Equal(3, batch.Length);
        }

        [Fact]
        public async Task HybridQuery_Where_Expr_FiltersRows()
        {
            using var fixture = await TestFixture.CreateHybridFixtureWithPrice("expr_hybrid");

            var query = fixture.Table
                .HybridSearch("apple", new float[] { 1f, 0f, 0f })
                .Where(Expr.Col("price") > 15)
                .Limit(10);
            var batch = await query.ToArrow();

            // Only rows with price > 15 (ids 1 & 2 have prices 20 & 30) survive the filter.
            Assert.True(batch.Length <= 2);
        }
    }
}
