namespace lancedb.tests;

/// <summary>
/// Tests for the Connection class.
/// Requires the Rust native library to be built first.
/// </summary>
public class ConnectionTests
{
    [Fact]
    public async Task Connect_ValidLocalPath_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            connection.Close();
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
    /// Verifies that creating multiple tables does not invalidate the connection pointer.
    /// Tests that database_open_table/database_create_empty_table borrows (not consumes) the Arc.
    /// </summary>
    [Fact]
    public async Task CreateEmptyTable_CalledMultipleTimes_DoesNotInvalidateConnection()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var table1 = await connection.CreateEmptyTable("table_one");
            var table2 = await connection.CreateEmptyTable("table_two");

            Assert.Equal("table_one", table1.Name);
            Assert.Equal("table_two", table2.Name);

            connection.Close();
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
    /// Opening a non-existent table should throw LanceDbException, not crash the process.
    /// </summary>
    [Fact]
    public async Task OpenTable_NonExistentTable_ThrowsLanceDbException()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            await Assert.ThrowsAsync<LanceDbException>(
                () => connection.OpenTable("does_not_exist"));

            connection.Close();
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
    /// Creating a table with a duplicate name should throw LanceDbException.
    /// </summary>
    [Fact]
    public async Task CreateEmptyTable_DuplicateName_ThrowsLanceDbException()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            await connection.CreateEmptyTable("my_table");

            await Assert.ThrowsAsync<LanceDbException>(
                () => connection.CreateEmptyTable("my_table"));

            connection.Close();
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
    public async Task TableNames_EmptyDatabase_ReturnsEmptyList()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var names = await connection.TableNames();
            Assert.Empty(names);

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
    public async Task TableNames_WithTables_ReturnsNamesInOrder()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            await connection.CreateEmptyTable("zebra");
            await connection.CreateEmptyTable("alpha");

            var names = await connection.TableNames();
            Assert.Equal(2, names.Count);
            Assert.Equal("alpha", names[0]);
            Assert.Equal("zebra", names[1]);

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
    public async Task DropTable_ExistingTable_RemovesIt()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            await connection.CreateEmptyTable("to_drop");

            await connection.DropTable("to_drop");

            var names = await connection.TableNames();
            Assert.Empty(names);

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
    public async Task DropTable_NonExistentTable_ThrowsLanceDbException()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            await Assert.ThrowsAsync<LanceDbException>(
                () => connection.DropTable("nonexistent"));

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
    public async Task DropAllTables_RemovesAllTables()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            await connection.CreateEmptyTable("table_a");
            await connection.CreateEmptyTable("table_b");

            await connection.DropAllTables();

            var names = await connection.TableNames();
            Assert.Empty(names);

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
    /// CreateTable with initial data should create the table and populate it.
    /// </summary>
    [Fact]
    public async Task CreateTable_WithData_CreatesAndPopulates()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var batch = CreateTestBatch(5);
            var table = await connection.CreateTable("my_table", batch);

            Assert.Equal("my_table", table.Name);
            long count = await table.CountRows();
            Assert.Equal(5, count);

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
    /// CreateTable with duplicate name should throw LanceDbException.
    /// </summary>
    [Fact]
    public async Task CreateTable_DuplicateName_ThrowsLanceDbException()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var batch = CreateTestBatch(3);
            await connection.CreateTable("dup_table", batch);

            await Assert.ThrowsAsync<LanceDbException>(
                () => connection.CreateTable("dup_table", CreateTestBatch(2)));

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
    /// CreateTable with overwrite mode should replace existing table data.
    /// </summary>
    [Fact]
    public async Task CreateTable_OverwriteMode_ReplacesExisting()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            await connection.CreateTable("overwrite_test", CreateTestBatch(10));
            var table = await connection.CreateTable("overwrite_test", CreateTestBatch(3), mode: "overwrite");

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

    /// <summary>
    /// RenameTable should throw NotImplementedException since it's Cloud-only.
    /// </summary>
    [Fact]
    public async Task RenameTable_ThrowsNotImplementedException()
    {
        var connection = new Connection();
        await Assert.ThrowsAsync<NotImplementedException>(
            () => connection.RenameTable("old", "new"));
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
    /// ExistOk should silently return the existing table instead of throwing.
    /// </summary>
    [Fact]
    public async Task CreateTable_ExistOk_ReturnsExistingTable()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTestBatch(5);
            var table1 = await connection.CreateTable("my_table", batch);

            var table2 = await connection.CreateTable("my_table", new CreateTableOptions
            {
                Data = new[] { batch },
                ExistOk = true
            });

            Assert.Equal("my_table", table2.Name);
            table1.Dispose();
            table2.Dispose();
            connection.Close();
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
    /// ExistOk on empty table creation should not throw for duplicate names.
    /// </summary>
    [Fact]
    public async Task CreateEmptyTable_ExistOk_ReturnsExistingTable()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table1 = await connection.CreateEmptyTable("my_table");

            var table2 = await connection.CreateEmptyTable("my_table", new CreateTableOptions
            {
                ExistOk = true
            });

            Assert.Equal("my_table", table2.Name);
            table1.Dispose();
            table2.Dispose();
            connection.Close();
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
    /// Creating a table with Schema (no data) should create an empty table
    /// with the specified schema.
    /// </summary>
    [Fact]
    public async Task CreateTable_WithSchema_CreatesEmptyTable()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, nullable: true))
                .Field(new Apache.Arrow.Field("age", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Build();

            var table = await connection.CreateTable("schema_table", new CreateTableOptions
            {
                Schema = schema
            });

            Assert.Equal("schema_table", table.Name);
            var count = await table.CountRows();
            Assert.Equal(0, count);

            var retrievedSchema = await table.Schema();
            Assert.Equal(2, retrievedSchema.FieldsList.Count);
            Assert.Equal("name", retrievedSchema.FieldsList[0].Name);
            Assert.Equal("age", retrievedSchema.FieldsList[1].Name);

            table.Dispose();
            connection.Close();
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
    /// CreateTable with neither Data nor Schema should throw ArgumentException.
    /// </summary>
    [Fact]
    public async Task CreateTable_NoDataNoSchema_ThrowsArgumentException()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            await Assert.ThrowsAsync<ArgumentException>(
                () => connection.CreateTable("bad_table", new CreateTableOptions()));

            connection.Close();
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
    /// CreateEmptyTable with a custom schema should use the provided schema.
    /// </summary>
    [Fact]
    public async Task CreateEmptyTable_WithCustomSchema_UsesProvidedSchema()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.FloatType.Default, nullable: false))
                .Field(new Apache.Arrow.Field("y", Apache.Arrow.Types.FloatType.Default, nullable: false))
                .Build();

            var table = await connection.CreateEmptyTable("custom_schema", new CreateTableOptions
            {
                Schema = schema
            });

            var retrievedSchema = await table.Schema();
            Assert.Equal(2, retrievedSchema.FieldsList.Count);
            Assert.Equal("x", retrievedSchema.FieldsList[0].Name);
            Assert.Equal("y", retrievedSchema.FieldsList[1].Name);

            table.Dispose();
            connection.Close();
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
    /// ConnectOptions with StorageOptions should be serialized and passed through FFI.
    /// </summary>
    [Fact]
    public async Task Connect_WithStorageOptions_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir, new ConnectionOptions
            {
                StorageOptions = new System.Collections.Generic.Dictionary<string, string>
                {
                    { "allow_http", "true" }
                }
            });
            var names = await connection.TableNames();
            Assert.Empty(names);
            connection.Close();
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
    /// ConnectOptions with ReadConsistencyInterval should be passed through FFI.
    /// </summary>
    [Fact]
    public async Task Connect_WithReadConsistencyInterval_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir, new ConnectionOptions
            {
                ReadConsistencyInterval = TimeSpan.FromSeconds(5)
            });
            var names = await connection.TableNames();
            Assert.Empty(names);
            connection.Close();
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
    /// OpenTable with StorageOptions should wire through FFI without error.
    /// </summary>
    [Fact]
    public async Task OpenTable_WithStorageOptions_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var table1 = await connection.CreateEmptyTable("opts_table");
            table1.Dispose();

            var table2 = await connection.OpenTable("opts_table", new OpenTableOptions
            {
                StorageOptions = new System.Collections.Generic.Dictionary<string, string>
                {
                    { "allow_http", "true" }
                },
                IndexCacheSize = 512
            });
            Assert.Equal("opts_table", table2.Name);
            table2.Dispose();
            connection.Close();
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
    /// CreateTable with StorageOptions should wire through FFI without error.
    /// </summary>
    [Fact]
    public async Task CreateTable_WithStorageOptions_Succeeds()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTestBatch(3);
            var table = await connection.CreateTable("storage_table", new CreateTableOptions
            {
                Data = new[] { batch },
                StorageOptions = new System.Collections.Generic.Dictionary<string, string>
                {
                    { "allow_http", "true" }
                }
            });
            Assert.Equal("storage_table", table.Name);
            Assert.Equal(3, await table.CountRows());
            table.Dispose();
            connection.Close();
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
    /// CreateTable with overwrite mode via options should replace an existing table.
    /// </summary>
    [Fact]
    public async Task CreateTable_OverwriteModeViaOptions_ReplacesExisting()
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        try
        {
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch1 = CreateTestBatch(5);
            var t1 = await connection.CreateTable("ow_table", batch1);
            Assert.Equal(5, await t1.CountRows());
            t1.Dispose();

            var batch2 = CreateTestBatch(2);
            var t2 = await connection.CreateTable("ow_table", new CreateTableOptions
            {
                Data = new[] { batch2 },
                Mode = "overwrite"
            });
            Assert.Equal(2, await t2.CountRows());
            t2.Dispose();
            connection.Close();
        }
        finally
        {
            if (Directory.Exists(tmpDir))
            {
                Directory.Delete(tmpDir, true);
            }
        }
    }
}
