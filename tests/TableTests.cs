namespace lancedb.tests;

/// <summary>
/// Tests for the Table class.
/// These tests verify that Rust Arc ownership is handled correctly —
/// calling methods multiple times on the same handle must not invalidate the pointer.
/// </summary>
public class TableTests
{
    /// <summary>
    /// Calling Name multiple times on the same Table should return consistent results
    /// without crashing. This exercises the Arc borrow-vs-consume fix in table_get_name.
    /// </summary>
    [Fact]
    public async Task Name_CalledMultipleTimes_ReturnsSameValue()
    {
        using var fixture = await TestFixture.CreateWithTable("test_table");

        var name1 = fixture.Table.Name;
        var name2 = fixture.Table.Name;
        var name3 = fixture.Table.Name;

        Assert.Equal("test_table", name1);
        Assert.Equal(name1, name2);
        Assert.Equal(name2, name3);
    }

    /// <summary>
    /// Creating a query from a table should not invalidate the table pointer.
    /// This exercises the Arc borrow-vs-consume fix in table_create_query.
    /// </summary>
    [Fact]
    public async Task Query_AfterName_DoesNotCrash()
    {
        using var fixture = await TestFixture.CreateWithTable("test_table");

        var name = fixture.Table.Name;
        var query = fixture.Table.Query();

        Assert.Equal("test_table", name);
        Assert.NotNull(query);
    }

    /// <summary>
    /// Creating multiple queries from the same table should not invalidate the pointer.
    /// </summary>
    [Fact]
    public async Task Query_CalledMultipleTimes_DoesNotCrash()
    {
        using var fixture = await TestFixture.CreateWithTable("test_table");

        var query1 = fixture.Table.Query();
        var query2 = fixture.Table.Query();
        var name = fixture.Table.Name;

        Assert.NotNull(query1);
        Assert.NotNull(query2);
        Assert.Equal("test_table", name);
    }

    /// <summary>
    /// Table.Dispose should release the Rust handle without crashing.
    /// </summary>
    [Fact]
    public async Task Dispose_ReleasesRustHandle()
    {
        using var fixture = await TestFixture.CreateWithTable("dispose_table");

        var name = fixture.Table.Name;
        Assert.Equal("dispose_table", name);

        fixture.Table.Dispose();
        // Calling Dispose again should be safe (idempotent)
        fixture.Table.Dispose();
    }

    /// <summary>
    /// Connection.Dispose should release the Rust handle without crashing.
    /// </summary>
    [Fact]
    public async Task Connection_Dispose_ReleasesRustHandle()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            connection.Dispose();
            // Calling Dispose again should be safe (idempotent)
            connection.Dispose();
        }
        finally
        {
            if (Directory.Exists(tmpDir))
            {
                Directory.Delete(tmpDir, true);
            }
        }
    }

    /// <summary>
    /// Query.Dispose should release the Rust handle without crashing.
    /// </summary>
    [Fact]
    public async Task Query_Dispose_ReleasesRustHandle()
    {
        using var fixture = await TestFixture.CreateWithTable("query_dispose_table");

        var query = fixture.Table.Query();
        query.Dispose();
        // Calling Dispose again should be safe (idempotent)
        query.Dispose();
    }

    /// <summary>
    /// CountRows on an empty table should return 0.
    /// </summary>
    [Fact]
    public async Task CountRows_EmptyTable_ReturnsZero()
    {
        using var fixture = await TestFixture.CreateWithTable("count_empty");

        long count = await fixture.Table.CountRows();

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Schema should return valid Apache.Arrow.Schema for the table.
    /// </summary>
    [Fact]
    public async Task Schema_ReturnsArrowSchema()
    {
        using var fixture = await TestFixture.CreateWithTable("schema_test");

        var schema = await fixture.Table.Schema();

        Assert.NotNull(schema);
        Assert.Single(schema.FieldsList);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.Equal(Apache.Arrow.Types.ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);
        Assert.False(schema.FieldsList[0].IsNullable);
    }

    /// <summary>
    /// Delete with an impossible predicate should not throw.
    /// </summary>
    [Fact]
    public async Task Delete_EmptyResult_DoesNotThrow()
    {
        using var fixture = await TestFixture.CreateWithTable("delete_empty");

        await fixture.Table.Delete("id < 0");

        long count = await fixture.Table.CountRows();
        Assert.Equal(0, count);
    }

    /// <summary>
    /// Update with no matching rows should not throw.
    /// </summary>
    [Fact]
    public async Task Update_NoMatchingRows_DoesNotThrow()
    {
        using var fixture = await TestFixture.CreateWithTable("update_empty");

        await fixture.Table.Update(
            new Dictionary<string, string> { { "id", "0" } },
            @where: "id < 0");

        long count = await fixture.Table.CountRows();
        Assert.Equal(0, count);
    }

    /// <summary>
    /// Add a single RecordBatch and verify row count increases.
    /// </summary>
    [Fact]
    public async Task Add_SingleBatch_IncreasesRowCount()
    {
        using var fixture = await TestFixture.CreateWithTable("add_single");

        var batch = CreateTestBatch(5);
        await fixture.Table.Add(batch);

        long count = await fixture.Table.CountRows();
        Assert.Equal(5, count);
    }

    /// <summary>
    /// Add data twice in append mode and verify rows accumulate.
    /// </summary>
    [Fact]
    public async Task Add_AppendMode_AccumulatesRows()
    {
        using var fixture = await TestFixture.CreateWithTable("add_append");

        await fixture.Table.Add(CreateTestBatch(3));
        await fixture.Table.Add(CreateTestBatch(4));

        long count = await fixture.Table.CountRows();
        Assert.Equal(7, count);
    }

    /// <summary>
    /// Add data in overwrite mode replaces existing data.
    /// </summary>
    [Fact]
    public async Task Add_OverwriteMode_ReplacesData()
    {
        using var fixture = await TestFixture.CreateWithTable("add_overwrite");

        await fixture.Table.Add(CreateTestBatch(10));
        await fixture.Table.Add(CreateTestBatch(3), mode: "overwrite");

        long count = await fixture.Table.CountRows();
        Assert.Equal(3, count);
    }

    private static Apache.Arrow.RecordBatch CreateTestBatch(int numRows)
    {
        var idArray = new Apache.Arrow.Int32Array.Builder();
        for (int i = 0; i < numRows; i++)
        {
            idArray.Append(i);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Build();

        return new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[] { idArray.Build() }, numRows);
    }
}
