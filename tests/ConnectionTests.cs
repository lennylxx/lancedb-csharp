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
}
