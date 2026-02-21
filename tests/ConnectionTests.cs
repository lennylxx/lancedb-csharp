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
}
