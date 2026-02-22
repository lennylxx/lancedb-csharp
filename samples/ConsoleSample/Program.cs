namespace ConsoleSample
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Arrow;

    using lancedb;

    public static class Program
    {
        public static async Task<int> Main()
        {
            string tempDir = Path.Combine(Path.GetTempPath(), "lancedb_sample_" + Guid.NewGuid().ToString("N")[..8]);
            try
            {
                Console.WriteLine($"Using temp directory: {tempDir}");

                // 1. Connect
                Console.Write("Connecting... ");
                var db = new Connection();
                await db.Connect(tempDir);
                Console.WriteLine("OK");

                // 2. Create a table
                Console.Write("Creating table... ");
                var schema = new Schema.Builder()
                    .Field(new Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                    .Field(new Field("name", Apache.Arrow.Types.StringType.Default, false))
                    .Build();

                var idArray = new Int32Array.Builder().Append(1).Append(2).Build();
                var nameArray = new StringArray.Builder().Append("alice").Append("bob").Build();
                var batch = new RecordBatch(schema, new IArrowArray[] { idArray, nameArray }, 2);

                using var table = await db.CreateTable("test_table", batch);
                Console.WriteLine("OK");

                // 3. Verify table name
                Console.Write("Checking table name... ");
                Assert(table.Name == "test_table", $"Expected 'test_table', got '{table.Name}'");
                Console.WriteLine($"OK ({table.Name})");

                // 4. Count rows
                Console.Write("Counting rows... ");
                long count = await table.CountRows();
                Assert(count == 2, $"Expected 2 rows, got {count}");
                Console.WriteLine($"OK ({count})");

                Console.WriteLine("\nAll checks passed!");
                db.Close();
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nFAILED: {ex}");
                return 1;
            }
            finally
            {
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, true);
                }
            }
        }

        private static void Assert(bool condition, string message)
        {
            if (!condition)
            {
                throw new Exception($"Assertion failed: {message}");
            }
        }
    }
}