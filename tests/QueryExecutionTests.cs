namespace lancedb.tests;

/// <summary>
/// Tests for Query and VectorQuery execution.
/// </summary>
public class QueryExecutionTests
{
    /// <summary>
    /// ToArrow on a table with data should return a RecordBatch with the correct row count.
    /// </summary>
    [Fact]
    public async Task ToArrow_WithData_ReturnsRecordBatch()
    {
        using var fixture = await TestFixture.CreateWithTable("toarrow_test");
        await fixture.Table.Add(CreateTestBatch(5));

        using var query = fixture.Table.Query();
        var batch = await query.ToArrow();

        Assert.NotNull(batch);
        Assert.Equal(5, batch.Length);
        Assert.Equal("id", batch.Schema.FieldsList[0].Name);
    }

    /// <summary>
    /// ToArrow on an empty table should return a RecordBatch with 0 rows.
    /// </summary>
    [Fact]
    public async Task ToArrow_EmptyTable_ReturnsEmptyBatch()
    {
        using var fixture = await TestFixture.CreateWithTable("toarrow_empty");

        using var query = fixture.Table.Query();
        var batch = await query.ToArrow();

        Assert.NotNull(batch);
        Assert.Equal(0, batch.Length);
    }

    /// <summary>
    /// ToList should return a list of dictionaries with column values.
    /// </summary>
    [Fact]
    public async Task ToList_WithData_ReturnsDictionaries()
    {
        using var fixture = await TestFixture.CreateWithTable("tolist_test");
        await fixture.Table.Add(CreateTestBatch(3));

        using var query = fixture.Table.Query();
        var rows = await query.ToList();

        Assert.Equal(3, rows.Count);
        Assert.True(rows[0].ContainsKey("id"));
        Assert.Equal(0, rows[0]["id"]);
        Assert.Equal(1, rows[1]["id"]);
        Assert.Equal(2, rows[2]["id"]);
    }

    /// <summary>
    /// Select with column names should only return the specified columns.
    /// </summary>
    [Fact]
    public async Task Select_Columns_ReturnsOnlySpecified()
    {
        using var fixture = await CreateTwoColumnFixture("select_cols");

        using var query = fixture.Table.Query().Select(new[] { "id" });
        var batch = await query.ToArrow();

        Assert.Equal(1, batch.Length);
        Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("id", batch.Schema.FieldsList[0].Name);
    }

    /// <summary>
    /// Select with SQL expressions should return computed columns.
    /// </summary>
    [Fact]
    public async Task Select_WithTransform_ReturnsComputedColumns()
    {
        using var fixture = await CreateTwoColumnFixture("select_transform");
        await fixture.Table.Add(CreateTwoColumnBatch(3));

        using var query = fixture.Table.Query()
            .Select(new Dictionary<string, string> { { "double_id", "id * 2" } });
        var rows = await query.ToList();

        Assert.Equal(4, rows.Count);
        Assert.True(rows[0].ContainsKey("double_id"));
    }

    /// <summary>
    /// Where should filter rows based on a SQL predicate.
    /// </summary>
    [Fact]
    public async Task Where_FiltersPredicate_ReturnsMatchingRows()
    {
        using var fixture = await TestFixture.CreateWithTable("where_test");
        await fixture.Table.Add(CreateTestBatch(10));

        using var query = fixture.Table.Query().Where("id >= 5");
        var batch = await query.ToArrow();

        Assert.Equal(5, batch.Length);
    }

    /// <summary>
    /// Limit should restrict the number of results.
    /// </summary>
    [Fact]
    public async Task Limit_RestrictsResults()
    {
        using var fixture = await TestFixture.CreateWithTable("limit_test");
        await fixture.Table.Add(CreateTestBatch(10));

        using var query = fixture.Table.Query().Limit(3);
        var batch = await query.ToArrow();

        Assert.Equal(3, batch.Length);
    }

    /// <summary>
    /// Offset should skip rows.
    /// </summary>
    [Fact]
    public async Task Offset_SkipsRows()
    {
        using var fixture = await TestFixture.CreateWithTable("offset_test");
        await fixture.Table.Add(CreateTestBatch(10));

        using var query = fixture.Table.Query().Limit(100).Offset(7);
        var batch = await query.ToArrow();

        Assert.Equal(3, batch.Length);
    }

    /// <summary>
    /// Builder methods should be chainable.
    /// </summary>
    [Fact]
    public async Task Chaining_BuilderMethods_Works()
    {
        using var fixture = await TestFixture.CreateWithTable("chain_test");
        await fixture.Table.Add(CreateTestBatch(20));

        using var query = fixture.Table.Query()
            .Where("id >= 5")
            .Limit(5)
            .Select(new[] { "id" });
        var batch = await query.ToArrow();

        Assert.Equal(5, batch.Length);
        Assert.Single(batch.Schema.FieldsList);
    }

    /// <summary>
    /// Dispose after ToArrow should be safe.
    /// </summary>
    [Fact]
    public async Task Dispose_AfterToArrow_IsSafe()
    {
        using var fixture = await TestFixture.CreateWithTable("dispose_after");
        await fixture.Table.Add(CreateTestBatch(5));

        var query = fixture.Table.Query();
        var batch = await query.ToArrow();
        query.Dispose();
        query.Dispose();

        Assert.Equal(5, batch.Length);
    }

    /// <summary>
    /// WithRowId should include a _rowid column in results.
    /// </summary>
    [Fact]
    public async Task WithRowId_IncludesRowIdColumn()
    {
        using var fixture = await TestFixture.CreateWithTable("rowid_test");
        await fixture.Table.Add(CreateTestBatch(3));

        using var query = fixture.Table.Query().WithRowId();
        var batch = await query.ToArrow();

        Assert.Equal(3, batch.Length);
        Assert.Contains(batch.Schema.FieldsList, f => f.Name == "_rowid");
    }

    /// <summary>
    /// ToList on table with multiple column types should convert values correctly.
    /// </summary>
    [Fact]
    public async Task ToList_MultipleTypes_ConvertsCorrectly()
    {
        using var fixture = await CreateTwoColumnFixture("types_test");

        using var query = fixture.Table.Query();
        var rows = await query.ToList();

        Assert.Single(rows);
        Assert.Equal(0, rows[0]["id"]);
        Assert.Equal("name_0", rows[0]["name"]);
    }

    // -----------------------------------------------------------------------
    // Full-Text Search Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// FullTextSearch should return matching rows from an FTS-indexed table.
    /// </summary>
    [Fact]
    public async Task FullTextSearch_WithIndex_ReturnsMatchingRows()
    {
        using var fixture = await CreateTextFixture("fts_basic");

        await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

        using var query = fixture.Table.Query().FullTextSearch("apple");
        var rows = await query.ToList();

        Assert.Single(rows);
        Assert.Equal("apple banana", rows[0]["content"]);
    }

    /// <summary>
    /// FullTextSearch should be chainable with other builder methods.
    /// </summary>
    [Fact]
    public async Task FullTextSearch_WithChaining_Works()
    {
        using var fixture = await CreateTextFixture("fts_chain");

        await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

        using var query = fixture.Table.Query()
            .FullTextSearch("cherry")
            .Select(new[] { "content" });
        var batch = await query.ToArrow();

        Assert.Equal(1, batch.Length);
        Assert.Contains(batch.Schema.FieldsList, f => f.Name == "content");
    }

    /// <summary>
    /// FullTextSearch with no matching term should return empty results.
    /// </summary>
    [Fact]
    public async Task FullTextSearch_NoMatch_ReturnsEmpty()
    {
        using var fixture = await CreateTextFixture("fts_empty");

        await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

        using var query = fixture.Table.Query().FullTextSearch("zzzznotfound");
        var batch = await query.ToArrow();

        Assert.Equal(0, batch.Length);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Apache.Arrow.RecordBatch CreateTextBatch(string[] texts)
    {
        var idBuilder = new Apache.Arrow.Int32Array.Builder();
        var contentBuilder = new Apache.Arrow.StringArray.Builder();
        for (int i = 0; i < texts.Length; i++)
        {
            idBuilder.Append(i);
            contentBuilder.Append(texts[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Field(new Apache.Arrow.Field("content", Apache.Arrow.Types.StringType.Default, nullable: false))
            .Build();

        return new Apache.Arrow.RecordBatch(schema,
            new Apache.Arrow.IArrowArray[] { idBuilder.Build(), contentBuilder.Build() }, texts.Length);
    }

    private static async Task<TestFixture> CreateTextFixture(string tableName)
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        var connection = new Connection();
        await connection.Connect(tmpDir);
        var batch = CreateTextBatch(new[]
        {
            "apple banana",
            "cherry date",
            "elderberry fig"
        });
        var table = await connection.CreateTable(tableName, batch);
        return new TestFixture(connection, table, tmpDir);
    }

    private static Apache.Arrow.RecordBatch CreateTestBatch(int numRows, int startId = 0)
    {
        var idArray = new Apache.Arrow.Int32Array.Builder();
        for (int i = startId; i < startId + numRows; i++)
        {
            idArray.Append(i);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Build();

        return new Apache.Arrow.RecordBatch(schema,
            new Apache.Arrow.IArrowArray[] { idArray.Build() }, numRows);
    }

    private static Apache.Arrow.RecordBatch CreateTwoColumnBatch(int numRows)
    {
        var idBuilder = new Apache.Arrow.Int32Array.Builder();
        var nameBuilder = new Apache.Arrow.StringArray.Builder();
        for (int i = 0; i < numRows; i++)
        {
            idBuilder.Append(i);
            nameBuilder.Append($"name_{i}");
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, nullable: false))
            .Build();

        return new Apache.Arrow.RecordBatch(schema,
            new Apache.Arrow.IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, numRows);
    }

    private static async Task<TestFixture> CreateTwoColumnFixture(string tableName)
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        var connection = new Connection();
        await connection.Connect(tmpDir);
        var batch = CreateTwoColumnBatch(1);
        var table = await connection.CreateTable(tableName, batch);
        return new TestFixture(connection, table, tmpDir);
    }

    /// <summary>
    /// A test fixture that creates a table with id + name columns.
    /// </summary>
    private class TwoColumnTestFixture : TestFixture
    {
        public TwoColumnTestFixture(Connection connection, Table table, string tmpDir)
            : base(connection, table, tmpDir)
        {
        }
    }
}
