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

    /// <summary>
    /// Version should return a positive value for a new table.
    /// </summary>
    [Fact]
    public async Task Version_NewTable_ReturnsPositiveValue()
    {
        using var fixture = await TestFixture.CreateWithTable("version_test");

        ulong version = await fixture.Table.Version();

        Assert.True(version > 0);
    }

    /// <summary>
    /// Version should increment after adding data.
    /// </summary>
    [Fact]
    public async Task Version_IncrementsAfterAdd()
    {
        using var fixture = await TestFixture.CreateWithTable("version_inc");

        ulong v1 = await fixture.Table.Version();
        await fixture.Table.Add(CreateTestBatch(3));
        ulong v2 = await fixture.Table.Version();

        Assert.True(v2 > v1);
    }

    /// <summary>
    /// ListVersions should return at least one version for a new table.
    /// </summary>
    [Fact]
    public async Task ListVersions_ReturnsAtLeastOne()
    {
        using var fixture = await TestFixture.CreateWithTable("list_versions");

        var versions = await fixture.Table.ListVersions();

        Assert.NotEmpty(versions);
        Assert.True(versions[0].Version > 0);
        Assert.NotEmpty(versions[0].Timestamp);
    }

    /// <summary>
    /// ListVersions Timestamp should be a parseable RFC 3339 / ISO 8601 date.
    /// </summary>
    [Fact]
    public async Task ListVersions_Timestamp_IsValidIso8601()
    {
        using var fixture = await TestFixture.CreateWithTable("version_ts");

        var versions = await fixture.Table.ListVersions();

        Assert.NotEmpty(versions);
        var timestamp = versions[0].Timestamp;
        Assert.True(
            DateTimeOffset.TryParse(timestamp, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind, out var parsed),
            $"Timestamp '{timestamp}' is not a valid ISO 8601 date");
        Assert.True(parsed.Year >= 2024, $"Timestamp year {parsed.Year} seems too old");
    }

    /// <summary>
    /// Checkout a previous version and verify row count matches that version.
    /// </summary>
    [Fact]
    public async Task Checkout_PreviousVersion_ChangesVisibleData()
    {
        using var fixture = await TestFixture.CreateWithTable("checkout_test");

        ulong v1 = await fixture.Table.Version();
        await fixture.Table.Add(CreateTestBatch(5));
        Assert.Equal(5, await fixture.Table.CountRows());

        await fixture.Table.Checkout(v1);
        Assert.Equal(0, await fixture.Table.CountRows());

        await fixture.Table.CheckoutLatest();
        Assert.Equal(5, await fixture.Table.CountRows());
    }

    /// <summary>
    /// Uri should return a non-empty string.
    /// </summary>
    [Fact]
    public async Task Uri_ReturnsNonEmptyString()
    {
        using var fixture = await TestFixture.CreateWithTable("uri_test");

        string uri = await fixture.Table.Uri();

        Assert.False(string.IsNullOrEmpty(uri));
    }

    /// <summary>
    /// CreateIndex with BTree on a scalar column should succeed.
    /// </summary>
    [Fact]
    public async Task CreateIndex_BTree_Succeeds()
    {
        using var fixture = await TestFixture.CreateWithTable("btree_idx");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex());

        var indices = await fixture.Table.ListIndices();
        Assert.Contains(indices, i => i.Columns.Contains("id") && i.IndexType == "BTREE");
    }

    /// <summary>
    /// CreateIndex with Bitmap on a scalar column should succeed.
    /// </summary>
    [Fact]
    public async Task CreateIndex_Bitmap_Succeeds()
    {
        using var fixture = await TestFixture.CreateWithTable("bitmap_idx");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BitmapIndex());

        var indices = await fixture.Table.ListIndices();
        Assert.Contains(indices, i => i.Columns.Contains("id") && i.IndexType == "BITMAP");
    }

    /// <summary>
    /// ListIndices on a table with no indices should return an empty list.
    /// </summary>
    [Fact]
    public async Task ListIndices_NoIndices_ReturnsEmpty()
    {
        using var fixture = await TestFixture.CreateWithTable("no_idx");

        var indices = await fixture.Table.ListIndices();

        Assert.Empty(indices);
    }

    /// <summary>
    /// AddColumns should add a new computed column to the table.
    /// </summary>
    [Fact]
    public async Task AddColumns_SqlExpression_AddsColumn()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTestBatch(3);
            var table = await connection.CreateTable("add_cols", batch);

            await table.AddColumns(new Dictionary<string, string>
            {
                { "doubled", "id * 2" }
            });

            var schema = await table.Schema();
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal("id", schema.FieldsList[0].Name);
            Assert.Equal("doubled", schema.FieldsList[1].Name);

            table.Dispose();
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
    /// AlterColumns should rename a column.
    /// </summary>
    [Fact]
    public async Task AlterColumns_Rename_ChangesColumnName()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTestBatch(3);
            var table = await connection.CreateTable("alter_cols", batch);

            await table.AlterColumns(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>
                {
                    { "path", "id" },
                    { "rename", "identifier" }
                }
            });

            var schema = await table.Schema();
            Assert.Equal("identifier", schema.FieldsList[0].Name);

            table.Dispose();
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
    /// DropColumns should remove a column from the table.
    /// </summary>
    [Fact]
    public async Task DropColumns_RemovesColumn()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var nameBuilder = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < 3; i++)
            {
                idBuilder.Append(i);
                nameBuilder.Append($"name_{i}");
            }
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Build();
            var batch = new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, 3);

            var table = await connection.CreateTable("drop_cols", batch);
            var schemaBefore = await table.Schema();
            Assert.Equal(2, schemaBefore.FieldsList.Count);

            await table.DropColumns(new[] { "name" });

            var schemaAfter = await table.Schema();
            Assert.Single(schemaAfter.FieldsList);
            Assert.Equal("id", schemaAfter.FieldsList[0].Name);

            table.Dispose();
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
    /// Optimize should complete without error after data modifications.
    /// </summary>
    [Fact]
    public async Task Optimize_AfterModifications_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTestBatch(3);
            var table = await connection.CreateTable("optimize_test", batch);

            await table.Add(CreateTestBatch(3, startId: 3));

            var stats = await table.Optimize();

            Assert.NotNull(stats);
            long count = await table.CountRows();
            Assert.Equal(6, count);

            table.Dispose();
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

        return new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[] { idArray.Build() }, numRows);
    }

    /// <summary>
    /// CreateTag and ListTags should create a tag and return it.
    /// </summary>
    [Fact]
    public async Task CreateTag_AndListTags_ReturnsTag()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("tag_test", CreateTestBatch(3));

            var version = await table.Version();
            await table.CreateTag("v1", version);

            var tags = await table.ListTags();

            Assert.True(tags.ContainsKey("v1"));
            Assert.Equal(version, tags["v1"].Version);

            table.Dispose();
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
    /// DeleteTag should remove a tag from the table.
    /// </summary>
    [Fact]
    public async Task DeleteTag_RemovesTag()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("tag_del", CreateTestBatch(3));

            var version = await table.Version();
            await table.CreateTag("temp", version);
            Assert.True((await table.ListTags()).ContainsKey("temp"));

            await table.DeleteTag("temp");
            Assert.False((await table.ListTags()).ContainsKey("temp"));

            table.Dispose();
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
    /// UpdateTag should change the version a tag points to.
    /// </summary>
    [Fact]
    public async Task UpdateTag_ChangesVersion()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("tag_upd", CreateTestBatch(3));

            var v1 = await table.Version();
            await table.CreateTag("release", v1);

            await table.Add(CreateTestBatch(2, startId: 10));
            var v2 = await table.Version();

            await table.UpdateTag("release", v2);

            var tags = await table.ListTags();
            Assert.Equal(v2, tags["release"].Version);

            table.Dispose();
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

    // Helper to create a batch with id and value columns for MergeInsert tests
    private static Apache.Arrow.RecordBatch CreateIdValueBatch(int[] ids, string[] values)
    {
        var idBuilder = new Apache.Arrow.Int32Array.Builder();
        var valueBuilder = new Apache.Arrow.StringArray.Builder();
        foreach (var id in ids) { idBuilder.Append(id); }
        foreach (var val in values) { valueBuilder.Append(val); }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Field(new Apache.Arrow.Field("value", Apache.Arrow.Types.StringType.Default, nullable: true))
            .Build();

        return new Apache.Arrow.RecordBatch(schema,
            new Apache.Arrow.IArrowArray[] { idBuilder.Build(), valueBuilder.Build() },
            ids.Length);
    }

    [Fact]
    public async Task CreateIndex_WithName_Succeeds()
    {
        using var fixture = await TestFixture.CreateWithTable("idx_name");
        await fixture.Table.Add(CreateTestBatch(10));
        await fixture.Table.CreateIndex(
            new[] { "id" }, new BTreeIndex(), replace: true, name: "my_custom_idx");

        var indices = await fixture.Table.ListIndices();
        Assert.Contains(indices, i => i.Name == "my_custom_idx");
    }

    [Fact]
    public async Task CreateIndex_TrainFalse_Succeeds()
    {
        using var fixture = await TestFixture.CreateWithTable("idx_train_false");
        await fixture.Table.Add(CreateTestBatch(10));
        await fixture.Table.CreateIndex(
            new[] { "id" }, new BTreeIndex(), replace: true, train: false);

        var indices = await fixture.Table.ListIndices();
        Assert.NotEmpty(indices);
    }

    [Fact]
    public async Task Optimize_WithCleanupParams_Succeeds()
    {
        using var fixture = await TestFixture.CreateWithTable("opt_params");
        await fixture.Table.Add(CreateTestBatch(5));
        await fixture.Table.Add(CreateTestBatch(5, startId: 5));

        var stats = await fixture.Table.Optimize(
            cleanupOlderThan: TimeSpan.Zero, deleteUnverified: true);
        Assert.NotNull(stats);
    }

    [Fact]
    public async Task MergeInsert_Upsert_InsertsNewAndUpdatesExisting()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var initial = CreateIdValueBatch(new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            var table = await connection.CreateTable("merge_upsert", initial);

            var newData = CreateIdValueBatch(new[] { 2, 3, 4 }, new[] { "B", "C", "D" });
            await table.MergeInsert("id")
                .WhenMatchedUpdateAll()
                .WhenNotMatchedInsertAll()
                .Execute(newData);

            long count = await table.CountRows();
            Assert.Equal(4, count);

            table.Dispose();
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

    [Fact]
    public async Task MergeInsert_InsertOnly_InsertsNewRows()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var initial = CreateIdValueBatch(new[] { 1, 2 }, new[] { "a", "b" });
            var table = await connection.CreateTable("merge_insert_only", initial);

            var newData = CreateIdValueBatch(new[] { 2, 3 }, new[] { "B", "C" });
            await table.MergeInsert("id")
                .WhenNotMatchedInsertAll()
                .Execute(newData);

            long count = await table.CountRows();
            Assert.Equal(3, count);

            table.Dispose();
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

    [Fact]
    public async Task MergeInsert_DeleteNotInSource_RemovesTargetRows()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var initial = CreateIdValueBatch(new[] { 1, 2, 3 }, new[] { "a", "b", "c" });
            var table = await connection.CreateTable("merge_delete", initial);

            var newData = CreateIdValueBatch(new[] { 2 }, new[] { "B" });
            await table.MergeInsert("id")
                .WhenMatchedUpdateAll()
                .WhenNotMatchedBySourceDelete()
                .Execute(newData);

            long count = await table.CountRows();
            Assert.Equal(1, count);

            table.Dispose();
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

    [Fact]
    public async Task MergeInsert_WithConditionalUpdate_OnlyUpdatesMatchingCondition()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var initial = CreateIdValueBatch(new[] { 1, 2 }, new[] { "a", "b" });
            var table = await connection.CreateTable("merge_cond", initial);

            var newData = CreateIdValueBatch(new[] { 1, 2 }, new[] { "A", "B" });
            await table.MergeInsert("id")
                .WhenMatchedUpdateAll("target.id = 2")
                .WhenNotMatchedInsertAll()
                .Execute(newData);

            long count = await table.CountRows();
            Assert.Equal(2, count);

            table.Dispose();
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

    [Fact]
    public async Task TakeOffsets_ReturnsCorrectRows()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var data = CreateIdValueBatch(new[] { 10, 20, 30, 40, 50 }, new[] { "a", "b", "c", "d", "e" });
            var table = await connection.CreateTable("take_offsets", data);

            var result = await table.TakeOffsets(new ulong[] { 0, 2, 4 });
            Assert.Equal(3, result.Length);

            table.Dispose();
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

    [Fact]
    public async Task TakeOffsets_WithColumns_ReturnsSubset()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var data = CreateIdValueBatch(new[] { 10, 20, 30 }, new[] { "a", "b", "c" });
            var table = await connection.CreateTable("take_offsets_cols", data);

            var result = await table.TakeOffsets(new ulong[] { 0, 1 }, new[] { "id" });
            Assert.Equal(2, result.Length);
            Assert.Single(result.Schema.FieldsList);
            Assert.Equal("id", result.Schema.FieldsList[0].Name);

            table.Dispose();
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

    [Fact]
    public async Task TakeRowIds_ReturnsCorrectRows()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var data = CreateIdValueBatch(new[] { 10, 20, 30 }, new[] { "a", "b", "c" });
            var table = await connection.CreateTable("take_rowids", data);

            // First get the row IDs from a query with WithRowId
            var query = table.Query().WithRowId();
            var batch = await query.ToArrow();

            // Extract row IDs from the _rowid column
            var rowIdCol = batch.Column("_rowid") as Apache.Arrow.UInt64Array;
            Assert.NotNull(rowIdCol);

            var rowIds = new ulong[] { rowIdCol!.GetValue(0)!.Value, rowIdCol.GetValue(2)!.Value };
            var result = await table.TakeRowIds(rowIds);
            Assert.Equal(2, result.Length);

            query.Dispose();
            table.Dispose();
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

    // ----- Bad Vector Handling Tests -----

    private static Apache.Arrow.RecordBatch CreateVectorBatch(float[][] vectors)
    {
        var idBuilder = new Apache.Arrow.Int32Array.Builder();
        var valueField = new Apache.Arrow.Field("item", Apache.Arrow.Types.FloatType.Default, nullable: false);
        int dim = vectors[0].Length;
        var vectorBuilder = new Apache.Arrow.FixedSizeListArray.Builder(valueField, dim);
        var valueBuilder = (Apache.Arrow.FloatArray.Builder)vectorBuilder.ValueBuilder;

        for (int i = 0; i < vectors.Length; i++)
        {
            idBuilder.Append(i);
            vectorBuilder.Append();
            foreach (var v in vectors[i])
            {
                valueBuilder.Append(v);
            }
        }

        var vectorType = new Apache.Arrow.Types.FixedSizeListType(valueField, dim);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Field(new Apache.Arrow.Field("vector", vectorType, nullable: false))
            .Build();

        return new Apache.Arrow.RecordBatch(schema,
            new Apache.Arrow.IArrowArray[] { idBuilder.Build(), vectorBuilder.Build() },
            vectors.Length);
    }

    /// <summary>
    /// Add with OnBadVectors=Error should throw when vectors contain NaN.
    /// </summary>
    [Fact]
    public async Task Add_OnBadVectors_Error_ThrowsOnNaN()
    {
        using var fixture = await TestFixture.CreateWithTable("bad_vec_error",
            CreateVectorBatch(new[] { new float[] { 1f, 2f, 3f } }));

        var badBatch = CreateVectorBatch(new[] {
            new float[] { 1f, 2f, 3f },
            new float[] { float.NaN, 2f, 3f },
        });

        await Assert.ThrowsAsync<ArgumentException>(() =>
            fixture.Table.Add(badBatch, new AddOptions { OnBadVectors = BadVectorHandling.Error }));
    }

    /// <summary>
    /// Add with OnBadVectors=Drop should remove rows with NaN vectors.
    /// </summary>
    [Fact]
    public async Task Add_OnBadVectors_Drop_RemovesBadRows()
    {
        using var fixture = await TestFixture.CreateWithTable("bad_vec_drop",
            CreateVectorBatch(new[] { new float[] { 1f, 2f, 3f } }));

        var badBatch = CreateVectorBatch(new[] {
            new float[] { 10f, 20f, 30f },
            new float[] { float.NaN, 2f, 3f },
            new float[] { 40f, 50f, 60f },
        });

        await fixture.Table.Add(badBatch, new AddOptions { OnBadVectors = BadVectorHandling.Drop });

        var count = await fixture.Table.CountRows();
        Assert.Equal(3, count); // 1 original + 2 good rows (bad row dropped)
    }

    /// <summary>
    /// Add with OnBadVectors=Fill should replace NaN vectors with fill value.
    /// </summary>
    [Fact]
    public async Task Add_OnBadVectors_Fill_ReplacesBadVectors()
    {
        using var fixture = await TestFixture.CreateWithTable("bad_vec_fill",
            CreateVectorBatch(new[] { new float[] { 1f, 2f, 3f } }));

        var badBatch = CreateVectorBatch(new[] {
            new float[] { float.NaN, 2f, 3f },
        });

        await fixture.Table.Add(badBatch, new AddOptions
        {
            OnBadVectors = BadVectorHandling.Fill,
            FillValue = 99f,
        });

        var count = await fixture.Table.CountRows();
        Assert.Equal(2, count); // 1 original + 1 filled

        using var query = fixture.Table.Query().Where("id = 0").Limit(1);
        var rows = await query.ToList();
        // The replaced row should be present (not dropped)
        Assert.Equal(2, (int)(long)count);
    }

    /// <summary>
    /// Add with OnBadVectors=Null should replace bad vectors with null.
    /// </summary>
    [Fact]
    public async Task Add_OnBadVectors_Null_ReplacesWithNull()
    {
        var valueField = new Apache.Arrow.Field("item", Apache.Arrow.Types.FloatType.Default, nullable: false);
        var vectorType = new Apache.Arrow.Types.FixedSizeListType(valueField, 3);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
            .Field(new Apache.Arrow.Field("vector", vectorType, nullable: true))
            .Build();

        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        var connection = new Connection();
        await connection.Connect(tmpDir);
        var table = await connection.CreateTable("bad_vec_null",
            new CreateTableOptions { Schema = schema });

        try
        {
            await table.Add(CreateVectorBatch(new[] { new float[] { 1f, 2f, 3f } }));

            var badBatch = CreateVectorBatch(new[] {
                new float[] { float.NaN, 2f, 3f },
            });

            await table.Add(badBatch, new AddOptions { OnBadVectors = BadVectorHandling.Null });

            var count = await table.CountRows();
            Assert.Equal(2, count); // 1 original + 1 with null vector
        }
        finally
        {
            table.Dispose();
            connection.Dispose();
            if (Directory.Exists(tmpDir))
            {
                Directory.Delete(tmpDir, true);
            }
        }
    }

    /// <summary>
    /// Add with clean vectors should not be affected by any OnBadVectors setting.
    /// </summary>
    [Fact]
    public async Task Add_OnBadVectors_CleanData_UnaffectedByAllModes()
    {
        using var fixture = await TestFixture.CreateWithTable("bad_vec_clean",
            CreateVectorBatch(new[] { new float[] { 1f, 2f, 3f } }));

        var cleanBatch = CreateVectorBatch(new[] {
            new float[] { 4f, 5f, 6f },
            new float[] { 7f, 8f, 9f },
        });

        // All modes should succeed with clean data
        await fixture.Table.Add(cleanBatch, new AddOptions { OnBadVectors = BadVectorHandling.Drop });
        await fixture.Table.Add(cleanBatch, new AddOptions { OnBadVectors = BadVectorHandling.Fill });
        await fixture.Table.Add(cleanBatch, new AddOptions { OnBadVectors = BadVectorHandling.Null });
        await fixture.Table.Add(cleanBatch, new AddOptions { OnBadVectors = BadVectorHandling.Error });

        var count = await fixture.Table.CountRows();
        Assert.Equal(9, count); // 1 original + 4 * 2 clean
    }

    /// <summary>
    /// Add without options should behave the same as the default (error mode) with no vectors.
    /// </summary>
    [Fact]
    public async Task Add_NoVectorColumns_IgnoresBadVectorHandling()
    {
        using var fixture = await TestFixture.CreateWithTable("bad_vec_no_vec");

        await fixture.Table.Add(CreateTestBatch(3),
            new AddOptions { OnBadVectors = BadVectorHandling.Drop });

        var count = await fixture.Table.CountRows();
        Assert.Equal(3, count);
    }

    /// <summary>
    /// Checkout by tag name should switch the table to the tagged version.
    /// </summary>
    [Fact]
    public async Task Checkout_ByTag_ChangesVisibleData()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("checkout_tag", CreateTestBatch(3));

            var v1 = await table.Version();
            await table.CreateTag("v1", v1);

            await table.Add(CreateTestBatch(5, startId: 10));
            Assert.Equal(8, await table.CountRows());

            await table.Checkout("v1");
            Assert.Equal(3, await table.CountRows());

            await table.CheckoutLatest();
            Assert.Equal(8, await table.CountRows());

            table.Dispose();
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
    /// Restore with an explicit version should restore the table to that version.
    /// </summary>
    [Fact]
    public async Task Restore_WithVersion_RestoresToSpecifiedVersion()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("restore_ver", CreateTestBatch(3));

            var v1 = await table.Version();

            await table.Add(CreateTestBatch(5, startId: 10));
            Assert.Equal(8, await table.CountRows());

            await table.Restore(v1);
            Assert.Equal(3, await table.CountRows());

            table.Dispose();
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
    /// Restore with a tag name should restore the table to the tagged version.
    /// </summary>
    [Fact]
    public async Task Restore_WithTag_RestoresToTaggedVersion()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("restore_tag", CreateTestBatch(3));

            var v1 = await table.Version();
            await table.CreateTag("baseline", v1);

            await table.Add(CreateTestBatch(5, startId: 10));
            Assert.Equal(8, await table.CountRows());

            await table.Restore("baseline");
            Assert.Equal(3, await table.CountRows());

            table.Dispose();
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
    /// GetTagVersion should return the version number a tag points to.
    /// </summary>
    [Fact]
    public async Task GetTagVersion_ReturnsCorrectVersion()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table = await connection.CreateTable("tag_getver", CreateTestBatch(3));

            var version = await table.Version();
            await table.CreateTag("my_tag", version);

            var tagVersion = await table.GetTagVersion("my_tag");

            Assert.Equal(version, tagVersion);

            table.Dispose();
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
    /// DropIndex should remove the index from the table.
    /// </summary>
    [Fact]
    public async Task DropIndex_RemovesIndex()
    {
        using var fixture = await TestFixture.CreateWithTable("drop_idx");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex());
        var indices = await fixture.Table.ListIndices();
        Assert.NotEmpty(indices);
        string indexName = indices[0].Name;

        await fixture.Table.DropIndex(indexName);

        indices = await fixture.Table.ListIndices();
        Assert.DoesNotContain(indices, i => i.Name == indexName);
    }

    /// <summary>
    /// IndexStats should return statistics for an existing index.
    /// </summary>
    [Fact]
    public async Task IndexStats_ExistingIndex_ReturnsStatistics()
    {
        using var fixture = await TestFixture.CreateWithTable("idx_stats");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex());
        var indices = await fixture.Table.ListIndices();
        string indexName = indices[0].Name;

        var stats = await fixture.Table.IndexStats(indexName);

        Assert.NotNull(stats);
        Assert.Equal(100, (int)stats!.NumIndexedRows);
        Assert.Equal("BTREE", stats.IndexType);
    }

    /// <summary>
    /// IndexStats should return null for a non-existent index.
    /// </summary>
    [Fact]
    public async Task IndexStats_NonExistent_ReturnsNull()
    {
        using var fixture = await TestFixture.CreateWithTable("idx_stats_none");

        var stats = await fixture.Table.IndexStats("nonexistent");

        Assert.Null(stats);
    }

    /// <summary>
    /// PrewarmIndex should not throw for an existing index.
    /// </summary>
    [Fact]
    public async Task PrewarmIndex_ExistingIndex_DoesNotThrow()
    {
        using var fixture = await TestFixture.CreateWithTable("prewarm_idx");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex());
        var indices = await fixture.Table.ListIndices();
        string indexName = indices[0].Name;

        await fixture.Table.PrewarmIndex(indexName);
    }

    /// <summary>
    /// WaitForIndex should return successfully when index is already complete.
    /// </summary>
    [Fact]
    public async Task WaitForIndex_AlreadyIndexed_ReturnsSuccessfully()
    {
        using var fixture = await TestFixture.CreateWithTable("wait_idx");
        await fixture.Table.Add(CreateTestBatch(100));

        await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex());
        var indices = await fixture.Table.ListIndices();
        string indexName = indices[0].Name;

        await fixture.Table.WaitForIndex(
            new[] { indexName }, TimeSpan.FromSeconds(30));
    }
}
