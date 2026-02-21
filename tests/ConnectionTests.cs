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
}
