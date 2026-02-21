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
                Directory.Delete(tmpDir, true);
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
                Directory.Delete(tmpDir, true);
        }
    }
}
