namespace lancedb.tests;

/// <summary>
/// Test fixture that creates a temporary LanceDB database with an empty table.
/// Cleans up the temp directory on dispose.
/// </summary>
public class TestFixture : IDisposable
{
    public Connection Connection { get; }
    public Table Table { get; }
    private readonly string _tmpDir;

    internal TestFixture(Connection connection, Table table, string tmpDir)
    {
        Connection = connection;
        Table = table;
        _tmpDir = tmpDir;
    }

    /// <summary>
    /// Creates a fixture with a connected database and an empty table.
    /// </summary>
    public static async Task<TestFixture> CreateWithTable(string tableName)
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        var connection = new Connection();
        await connection.Connect(tmpDir);
        var table = await connection.CreateEmptyTable(tableName);
        return new TestFixture(connection, table, tmpDir);
    }

    /// <summary>
    /// Creates a fixture with a connected database and a table pre-populated with data.
    /// </summary>
    public static async Task<TestFixture> CreateWithTable(
        string tableName, Apache.Arrow.RecordBatch initialData)
    {
        var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
        var connection = new Connection();
        await connection.Connect(tmpDir);
        var table = await connection.CreateTable(tableName, initialData);
        return new TestFixture(connection, table, tmpDir);
    }

    public void Dispose()
    {
        Table.Dispose();
        Connection.Dispose();
        if (Directory.Exists(_tmpDir))
        {
            Directory.Delete(_tmpDir, true);
        }
    }
}
