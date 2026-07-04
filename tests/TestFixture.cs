namespace lancedb.tests
{
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using static TestHelpers;

    /// <summary>
    /// Test fixture that creates a temporary LanceDB database with an empty table.
    /// Cleans up the temp directory on dispose.
    /// </summary>
    public class TestFixture : IDisposable
    {
        public Connection Connection { get; }
        public lancedb.Table Table { get; }
        private readonly string _tmpDir;

        internal TestFixture(Connection connection, lancedb.Table table, string tmpDir)
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
            string tableName, RecordBatch initialData)
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
            DeleteDirectoryWithRetry(_tmpDir);
        }

        // The native runtime may still be flushing background filesystem work
        // when the managed handles are freed, so a recursive delete can
        // transiently race with those writes and fail with "directory not
        // empty" or an access error. Retry a few times before giving up.
        private static void DeleteDirectoryWithRetry(string dir)
        {
            if (!Directory.Exists(dir))
            {
                return;
            }

            const int maxAttempts = 5;
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    Directory.Delete(dir, true);
                    return;
                }
                catch (Exception ex) when (
                    (ex is IOException || ex is UnauthorizedAccessException) && attempt < maxAttempts)
                {
                    Thread.Sleep(100);
                }
            }
        }

        // ---------------------------------------------------------------
        // Scenario-specific fixture builders
        // ---------------------------------------------------------------

        /// <summary>
        /// 3-row fixture with id + content columns ("apple banana", "cherry date", "elderberry fig").
        /// </summary>
        public static async Task<TestFixture> CreateTextFixture(string tableName)
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

        /// <summary>
        /// 3-row fixture with id + title + body columns, used for multi-column FTS tests.
        /// </summary>
        public static async Task<TestFixture> CreateMultiTextFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var titleBuilder = new StringArray.Builder();
            var bodyBuilder = new StringArray.Builder();

            idBuilder.Append(0); titleBuilder.Append("apple pie"); bodyBuilder.Append("cherry tart recipe");
            idBuilder.Append(1); titleBuilder.Append("banana bread"); bodyBuilder.Append("apple sauce recipe");
            idBuilder.Append(2); titleBuilder.Append("cherry cake"); bodyBuilder.Append("fig jam recipe");

            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("title", StringType.Default, nullable: false))
                .Field(new Field("body", StringType.Default, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), titleBuilder.Build(), bodyBuilder.Build() }, 3);
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// Single-row fixture with id + name columns.
        /// </summary>
        public static async Task<TestFixture> CreateTwoColumnFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTwoColumnBatch(1);
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// 3-row fixture with id + content + 3-dim vector columns.
        /// </summary>
        public static async Task<TestFixture> CreateVectorTextFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateVectorTextBatch();
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// Creates a fixture with 5 items designed to test score normalization.
        /// Vectors produce L2 distances in range [0, 2], FTS scores are all identical.
        /// </summary>
        public static async Task<TestFixture> CreateNormalizationFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();

            // id=1: matches FTS (apple x3) + closest vector
            // id=2: no FTS match + farthest vector
            // id=3: matches FTS (apple x2) + mid vector
            // id=4: no FTS match + close vector
            // id=5: matches FTS (apple x1) + far vector
            // Distinct apple term-frequencies give ids 1,3,5 distinct BM25 scores
            // (no ties), so rank-based normalization is deterministic and does not
            // depend on the FTS engine's tie-ordering of equal scores.
            string[] texts = new[]
            {
                "apple apple apple banana fruit",
                "cherry date sweet",
                "apple apple cherry tart",
                "banana fig jam",
                "apple pie dessert",
            };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f },   // dist=0.0 from [1,0]
                new float[] { 0.0f, 1.0f },   // dist=2.0
                new float[] { 0.5f, 0.5f },   // dist=0.5
                new float[] { 0.9f, 0.1f },   // dist=0.02
                new float[] { 0.0f, 0.9f },   // dist=1.81
            };

            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, 2);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i + 1);
                contentBuilder.Append(texts[i]);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, 2);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);
            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// 3-row hybrid fixture with id + content + 3-dim vector and a pre-built FTS index on content.
        /// </summary>
        public static async Task<TestFixture> CreateHybridFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();

            string[] texts = new[] { "apple banana fruit", "cherry date sweet", "elderberry fig tart" };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f, 0.0f },
                new float[] { 0.0f, 1.0f, 0.0f },
                new float[] { 0.0f, 0.0f, 1.0f },
            };

            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, 3);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i);
                contentBuilder.Append(texts[i]);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, 3);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);

            // Create FTS index on content column
            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// Same shape as <see cref="CreateHybridFixture"/> plus a "price" int32 column (values 10/20/30).
        /// </summary>
        public static async Task<TestFixture> CreateHybridFixtureWithPrice(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Int32Array.Builder();
            var contentBuilder = new StringArray.Builder();
            var priceBuilder = new Int32Array.Builder();

            string[] texts = new[] { "apple banana fruit", "cherry date sweet", "elderberry fig tart" };
            int[] prices = new[] { 10, 20, 30 };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f, 0.0f },
                new float[] { 0.0f, 1.0f, 0.0f },
                new float[] { 0.0f, 0.0f, 1.0f },
            };

            var valueField = new Field("item", FloatType.Default, nullable: false);
            var vectorBuilder = new FixedSizeListArray.Builder(valueField, 3);
            var valueBuilder = (FloatArray.Builder)vectorBuilder.ValueBuilder;

            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i);
                contentBuilder.Append(texts[i]);
                priceBuilder.Append(prices[i]);
                vectorBuilder.Append();
                foreach (var v in vectors[i])
                {
                    valueBuilder.Append(v);
                }
            }

            var vectorType = new FixedSizeListType(valueField, 3);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: false))
                .Field(new Field("content", StringType.Default, nullable: false))
                .Field(new Field("price", Int32Type.Default, nullable: false))
                .Field(new Field("vector", vectorType, nullable: false))
                .Build();

            var batch = new RecordBatch(schema,
                new IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), priceBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
            var table = await connection.CreateTable(tableName, batch);

            await table.CreateIndex(new[] { "content" }, new FtsIndex());

            return new TestFixture(connection, table, tmpDir);
        }
    }
}
