namespace lancedb.tests
{
    /// <summary>
    /// Tests for Query and VectorQuery execution.
    /// </summary>
    public class QueryExecutionTests
    {
        /// <summary>
        /// ToArrow on a table with data should return a RecordBatch with the correct row count.
        /// </summary>
        [Fact]
        public async Task ToArrow_WithData_ReturnsRecordBatch()
        {
            using var fixture = await TestFixture.CreateWithTable("toarrow_test");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.Equal(5, batch.Length);
            Assert.Equal("id", batch.Schema.FieldsList[0].Name);
        }

        /// <summary>
        /// ToArrow on an empty table should return a RecordBatch with 0 rows.
        /// </summary>
        [Fact]
        public async Task ToArrow_EmptyTable_ReturnsEmptyBatch()
        {
            using var fixture = await TestFixture.CreateWithTable("toarrow_empty");

            using var query = fixture.Table.Query();
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.Equal(0, batch.Length);
        }

        /// <summary>
        /// ToList should return a list of dictionaries with column values.
        /// </summary>
        [Fact]
        public async Task ToList_WithData_ReturnsDictionaries()
        {
            using var fixture = await TestFixture.CreateWithTable("tolist_test");
            await fixture.Table.Add(CreateTestBatch(3));

            using var query = fixture.Table.Query();
            var rows = await query.ToList();

            Assert.Equal(3, rows.Count);
            Assert.True(rows[0].ContainsKey("id"));
            Assert.Equal(0, rows[0]["id"]);
            Assert.Equal(1, rows[1]["id"]);
            Assert.Equal(2, rows[2]["id"]);
        }

        /// <summary>
        /// Select with column names should only return the specified columns.
        /// </summary>
        [Fact]
        public async Task Select_Columns_ReturnsOnlySpecified()
        {
            using var fixture = await CreateTwoColumnFixture("select_cols");

            using var query = fixture.Table.Query().Select(new[] { "id" });
            var batch = await query.ToArrow();

            Assert.Equal(1, batch.Length);
            Assert.Single(batch.Schema.FieldsList);
            Assert.Equal("id", batch.Schema.FieldsList[0].Name);
        }

        /// <summary>
        /// Select with SQL expressions should return computed columns.
        /// </summary>
        [Fact]
        public async Task Select_WithTransform_ReturnsComputedColumns()
        {
            using var fixture = await CreateTwoColumnFixture("select_transform");
            await fixture.Table.Add(CreateTwoColumnBatch(3));

            using var query = fixture.Table.Query()
                .Select(new Dictionary<string, string> { { "double_id", "id * 2" } });
            var rows = await query.ToList();

            Assert.Equal(4, rows.Count);
            Assert.True(rows[0].ContainsKey("double_id"));
        }

        /// <summary>
        /// Where should filter rows based on a SQL predicate.
        /// </summary>
        [Fact]
        public async Task Where_FiltersPredicate_ReturnsMatchingRows()
        {
            using var fixture = await TestFixture.CreateWithTable("where_test");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query().Where("id >= 5");
            var batch = await query.ToArrow();

            Assert.Equal(5, batch.Length);
        }

        /// <summary>
        /// Limit should restrict the number of results.
        /// </summary>
        [Fact]
        public async Task Limit_RestrictsResults()
        {
            using var fixture = await TestFixture.CreateWithTable("limit_test");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query().Limit(3);
            var batch = await query.ToArrow();

            Assert.Equal(3, batch.Length);
        }

        /// <summary>
        /// Offset should skip rows.
        /// </summary>
        [Fact]
        public async Task Offset_SkipsRows()
        {
            using var fixture = await TestFixture.CreateWithTable("offset_test");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query().Limit(100).Offset(7);
            var batch = await query.ToArrow();

            Assert.Equal(3, batch.Length);
        }

        /// <summary>
        /// Builder methods should be chainable.
        /// </summary>
        [Fact]
        public async Task Chaining_BuilderMethods_Works()
        {
            using var fixture = await TestFixture.CreateWithTable("chain_test");
            await fixture.Table.Add(CreateTestBatch(20));

            using var query = fixture.Table.Query()
                .Where("id >= 5")
                .Limit(5)
                .Select(new[] { "id" });
            var batch = await query.ToArrow();

            Assert.Equal(5, batch.Length);
            Assert.Single(batch.Schema.FieldsList);
        }

        /// <summary>
        /// Dispose after ToArrow should be safe.
        /// </summary>
        [Fact]
        public async Task Dispose_AfterToArrow_IsSafe()
        {
            using var fixture = await TestFixture.CreateWithTable("dispose_after");
            await fixture.Table.Add(CreateTestBatch(5));

            var query = fixture.Table.Query();
            var batch = await query.ToArrow();
            query.Dispose();
            query.Dispose();

            Assert.Equal(5, batch.Length);
        }

        /// <summary>
        /// WithRowId should include a _rowid column in results.
        /// </summary>
        [Fact]
        public async Task WithRowId_IncludesRowIdColumn()
        {
            using var fixture = await TestFixture.CreateWithTable("rowid_test");
            await fixture.Table.Add(CreateTestBatch(3));

            using var query = fixture.Table.Query().WithRowId();
            var batch = await query.ToArrow();

            Assert.Equal(3, batch.Length);
            Assert.Contains(batch.Schema.FieldsList, f => f.Name == "_rowid");
        }

        /// <summary>
        /// ToList on table with multiple column types should convert values correctly.
        /// </summary>
        [Fact]
        public async Task ToList_MultipleTypes_ConvertsCorrectly()
        {
            using var fixture = await CreateTwoColumnFixture("types_test");

            using var query = fixture.Table.Query();
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(0, rows[0]["id"]);
            Assert.Equal("name_0", rows[0]["name"]);
        }

        // -----------------------------------------------------------------------
        // Full-Text Search Tests
        // -----------------------------------------------------------------------

        /// <summary>
        /// FullTextSearch should return matching rows from an FTS-indexed table.
        /// </summary>
        [Fact]
        public async Task FullTextSearch_WithIndex_ReturnsMatchingRows()
        {
            using var fixture = await CreateTextFixture("fts_basic");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query().FullTextSearch("apple");
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        /// <summary>
        /// FullTextSearch should be chainable with other builder methods.
        /// </summary>
        [Fact]
        public async Task FullTextSearch_WithChaining_Works()
        {
            using var fixture = await CreateTextFixture("fts_chain");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .FullTextSearch("cherry")
                .Select(new[] { "content" });
            var batch = await query.ToArrow();

            Assert.Equal(1, batch.Length);
            Assert.Contains(batch.Schema.FieldsList, f => f.Name == "content");
        }

        /// <summary>
        /// FullTextSearch with no matching term should return empty results.
        /// </summary>
        [Fact]
        public async Task FullTextSearch_NoMatch_ReturnsEmpty()
        {
            using var fixture = await CreateTextFixture("fts_empty");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query().FullTextSearch("zzzznotfound");
            var batch = await query.ToArrow();

            Assert.Equal(0, batch.Length);
        }

        /// <summary>
        /// FastSearch should be chainable with FullTextSearch.
        /// </summary>
        [Fact]
        public async Task FastSearch_WithFts_IsChainable()
        {
            using var fixture = await CreateTextFixture("fts_fast");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .FullTextSearch("apple")
                .FastSearch();
            var rows = await query.ToList();

            Assert.Single(rows);
        }

        /// <summary>
        /// Postfilter should be chainable on a regular Query with FTS.
        /// </summary>
        [Fact]
        public async Task Postfilter_OnQuery_IsChainable()
        {
            using var fixture = await CreateTextFixture("fts_postfilter");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .FullTextSearch("apple")
                .Postfilter()
                .Limit(10);
            var rows = await query.ToList();

            Assert.Single(rows);
        }

        // -----------------------------------------------------------------------
        // NearestToText Tests
        // -----------------------------------------------------------------------

        /// <summary>
        /// NearestToText should return matching rows from an FTS-indexed table.
        /// </summary>
        [Fact]
        public async Task NearestToText_WithIndex_ReturnsMatchingRows()
        {
            using var fixture = await CreateTextFixture("ntt_basic");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query().NearestToText("apple");
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple banana", rows[0]["content"]);
        }

        /// <summary>
        /// NearestToText should be chainable with other builder methods.
        /// </summary>
        [Fact]
        public async Task NearestToText_WithChaining_Works()
        {
            using var fixture = await CreateTextFixture("ntt_chain");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("cherry")
                .Select(new[] { "content" })
                .Limit(5);
            var batch = await query.ToArrow();

            Assert.Equal(1, batch.Length);
            Assert.Contains(batch.Schema.FieldsList, f => f.Name == "content");
        }

        /// <summary>
        /// NearestToText with no matching term should return empty results.
        /// </summary>
        [Fact]
        public async Task NearestToText_NoMatch_ReturnsEmpty()
        {
            using var fixture = await CreateTextFixture("ntt_empty");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query().NearestToText("zzzznotfound");
            var batch = await query.ToArrow();

            Assert.Equal(0, batch.Length);
        }

        /// <summary>
        /// NearestToText should support FastSearch chaining.
        /// </summary>
        [Fact]
        public async Task NearestToText_FastSearch_IsChainable()
        {
            using var fixture = await CreateTextFixture("ntt_fast");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("apple")
                .FastSearch();
            var rows = await query.ToList();

            Assert.Single(rows);
        }

        /// <summary>
        /// NearestToText should be safe to dispose after execution.
        /// </summary>
        [Fact]
        public async Task NearestToText_Dispose_IsSafe()
        {
            using var fixture = await CreateTextFixture("ntt_dispose");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = fixture.Table.Query().NearestToText("apple");
            var rows = await query.ToList();
            query.Dispose();
            query.Dispose();

            Assert.Single(rows);
        }

        /// <summary>
        /// The original Query should remain usable after calling NearestToText.
        /// </summary>
        [Fact]
        public async Task NearestToText_DoesNotConsumeOriginalQuery()
        {
            using var fixture = await CreateTextFixture("ntt_noconsume");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var baseQuery = fixture.Table.Query();
            using var ftsQuery = baseQuery.NearestToText("apple");
            var ftsRows = await ftsQuery.ToList();

            var allRows = await baseQuery.ToList();

            Assert.Single(ftsRows);
            Assert.Equal(3, allRows.Count);
        }

        /// <summary>
        /// NearestToText chained with NearestTo should produce hybrid search results.
        /// </summary>
        [Fact]
        public async Task NearestToText_NearestTo_ReturnsHybridResults()
        {
            using var fixture = await CreateVectorTextFixture("hybrid_exec");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            var rows = await query.ToList();

            Assert.NotEmpty(rows);
        }

        /// <summary>
        /// Hybrid search via vector-first path (NearestTo then FullTextSearch)
        /// should also work.
        /// </summary>
        [Fact]
        public async Task HybridSearch_VectorFirst_ReturnsResults()
        {
            using var fixture = await CreateVectorTextFixture("hybrid_vecfirst");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .FullTextSearch("apple");
            var rows = await query.ToList();

            Assert.NotEmpty(rows);
        }

        /// <summary>
        /// NearestToText with columns parameter should only search specified columns.
        /// </summary>
        [Fact]
        public async Task NearestToText_WithColumns_SearchesSpecifiedColumns()
        {
            using var fixture = await CreateMultiTextFixture("ntt_columns");

            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("apple", new[] { "title" });
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple pie", rows[0]["title"]);
        }

        /// <summary>
        /// FullTextSearch with columns parameter should only search specified columns.
        /// </summary>
        [Fact]
        public async Task FullTextSearch_WithColumns_SearchesSpecifiedColumns()
        {
            using var fixture = await CreateMultiTextFixture("fts_columns");

            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .FullTextSearch("apple", new[] { "body" });
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple sauce recipe", rows[0]["body"]);
        }

        // -----------------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------------

        private static Apache.Arrow.RecordBatch CreateTextBatch(string[] texts)
        {
            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var contentBuilder = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < texts.Length; i++)
            {
                idBuilder.Append(i);
                contentBuilder.Append(texts[i]);
            }

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Field(new Apache.Arrow.Field("content", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Build();

            return new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), contentBuilder.Build() }, texts.Length);
        }

        private static async Task<TestFixture> CreateTextFixture(string tableName)
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

        private static async Task<TestFixture> CreateMultiTextFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);

            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var titleBuilder = new Apache.Arrow.StringArray.Builder();
            var bodyBuilder = new Apache.Arrow.StringArray.Builder();

            idBuilder.Append(0); titleBuilder.Append("apple pie"); bodyBuilder.Append("cherry tart recipe");
            idBuilder.Append(1); titleBuilder.Append("banana bread"); bodyBuilder.Append("apple sauce recipe");
            idBuilder.Append(2); titleBuilder.Append("cherry cake"); bodyBuilder.Append("fig jam recipe");

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Field(new Apache.Arrow.Field("title", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Field(new Apache.Arrow.Field("body", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Build();

            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), titleBuilder.Build(), bodyBuilder.Build() }, 3);
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
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

            return new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idArray.Build() }, numRows);
        }

        private static Apache.Arrow.RecordBatch CreateTwoColumnBatch(int numRows)
        {
            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var nameBuilder = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < numRows; i++)
            {
                idBuilder.Append(i);
                nameBuilder.Append($"name_{i}");
            }

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Build();

            return new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), nameBuilder.Build() }, numRows);
        }

        private static async Task<TestFixture> CreateTwoColumnFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateTwoColumnBatch(1);
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
        }

        /// <summary>
        /// A test fixture that creates a table with id + name columns.
        /// </summary>
        private class TwoColumnTestFixture : TestFixture
        {
            public TwoColumnTestFixture(Connection connection, Table table, string tmpDir)
                : base(connection, table, tmpDir)
            {
            }
        }

        private static Apache.Arrow.RecordBatch CreateVectorTextBatch()
        {
            var idBuilder = new Apache.Arrow.Int32Array.Builder();
            var contentBuilder = new Apache.Arrow.StringArray.Builder();

            string[] texts = new[] { "apple banana", "cherry date", "elderberry fig" };
            float[][] vectors = new[]
            {
                new float[] { 1.0f, 0.0f, 0.0f },
                new float[] { 0.0f, 1.0f, 0.0f },
                new float[] { 0.0f, 0.0f, 1.0f },
            };

            var valueField = new Apache.Arrow.Field("item", Apache.Arrow.Types.FloatType.Default, nullable: false);
            var vectorBuilder = new Apache.Arrow.FixedSizeListArray.Builder(valueField, 3);
            var valueBuilder = (Apache.Arrow.FloatArray.Builder)vectorBuilder.ValueBuilder;

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

            var vectorType = new Apache.Arrow.Types.FixedSizeListType(valueField, 3);
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, nullable: false))
                .Field(new Apache.Arrow.Field("content", Apache.Arrow.Types.StringType.Default, nullable: false))
                .Field(new Apache.Arrow.Field("vector", vectorType, nullable: false))
                .Build();

            return new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { idBuilder.Build(), contentBuilder.Build(), vectorBuilder.Build() },
                texts.Length);
        }

        private static async Task<TestFixture> CreateVectorTextFixture(string tableName)
        {
            var tmpDir = Path.Combine(Path.GetTempPath(), "lancedb_test_" + Guid.NewGuid().ToString("N"));
            var connection = new Connection();
            await connection.Connect(tmpDir);
            var batch = CreateVectorTextBatch();
            var table = await connection.CreateTable(tableName, batch);
            return new TestFixture(connection, table, tmpDir);
        }

        // ----- Query Missing Features Tests -----

        /// <summary>
        /// ExplainPlan should return a non-empty plan string.
        /// </summary>
        [Fact]
        public async Task ExplainPlan_ReturnsNonEmptyString()
        {
            using var fixture = await TestFixture.CreateWithTable("explain_plan");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            string plan = await query.ExplainPlan();

            Assert.NotNull(plan);
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// ExplainPlan with verbose=true should return a different (usually longer) plan.
        /// </summary>
        [Fact]
        public async Task ExplainPlan_Verbose_ReturnsDetailedPlan()
        {
            using var fixture = await TestFixture.CreateWithTable("explain_verbose");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            string plan = await query.ExplainPlan(verbose: true);

            Assert.NotNull(plan);
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// AnalyzePlan should return a non-empty string with runtime metrics.
        /// </summary>
        [Fact]
        public async Task AnalyzePlan_ReturnsNonEmptyString()
        {
            using var fixture = await TestFixture.CreateWithTable("analyze_plan");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            string plan = await query.AnalyzePlan();

            Assert.NotNull(plan);
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// OutputSchema should return the schema matching the table columns.
        /// </summary>
        [Fact]
        public async Task OutputSchema_ReturnsTableSchema()
        {
            using var fixture = await TestFixture.CreateWithTable("output_schema");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            var schema = await query.OutputSchema();

            Assert.NotNull(schema);
            Assert.True(schema.FieldsList.Count > 0);
            Assert.Equal("id", schema.FieldsList[0].Name);
        }

        /// <summary>
        /// OutputSchema with Select should only include selected columns.
        /// </summary>
        [Fact]
        public async Task OutputSchema_WithSelect_ReturnsFilteredSchema()
        {
            using var fixture = await TestFixture.CreateWithTable("output_schema_sel");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query().Select(new[] { "id" });
            var schema = await query.OutputSchema();

            Assert.NotNull(schema);
            Assert.Single(schema.FieldsList);
            Assert.Equal("id", schema.FieldsList[0].Name);
        }

        /// <summary>
        /// ToArrow with maxBatchLength should still return correct data.
        /// </summary>
        [Fact]
        public async Task ToArrow_WithMaxBatchLength_ReturnsCorrectData()
        {
            using var fixture = await TestFixture.CreateWithTable("max_batch");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query();
            var batch = await query.ToArrow(maxBatchLength: 5);

            Assert.NotNull(batch);
            Assert.Equal(10, batch.Length);
        }

        /// <summary>
        /// ToArrow with a long timeout should succeed normally.
        /// </summary>
        [Fact]
        public async Task ToArrow_WithTimeout_Succeeds()
        {
            using var fixture = await TestFixture.CreateWithTable("timeout_test");
            await fixture.Table.Add(CreateTestBatch(5));

            using var query = fixture.Table.Query();
            var batch = await query.ToArrow(timeout: TimeSpan.FromSeconds(30));

            Assert.NotNull(batch);
            Assert.Equal(5, batch.Length);
        }

        /// <summary>
        /// VectorQuery ExplainPlan should return a plan string.
        /// </summary>
        [Fact]
        public async Task VectorQuery_ExplainPlan_ReturnsString()
        {
            using var fixture = await CreateVectorTextFixture("vq_explain");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            string plan = await query.ExplainPlan();

            Assert.NotNull(plan);
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// VectorQuery AnalyzePlan should return runtime metrics.
        /// </summary>
        [Fact]
        public async Task VectorQuery_AnalyzePlan_ReturnsString()
        {
            using var fixture = await CreateVectorTextFixture("vq_analyze");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            string plan = await query.AnalyzePlan();

            Assert.NotNull(plan);
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// VectorQuery OutputSchema should return schema with distance column.
        /// </summary>
        [Fact]
        public async Task VectorQuery_OutputSchema_IncludesDistanceColumn()
        {
            using var fixture = await CreateVectorTextFixture("vq_out_schema");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            var schema = await query.OutputSchema();

            Assert.NotNull(schema);
            var fieldNames = schema.FieldsList.Select(f => f.Name).ToList();
            Assert.Contains("_distance", fieldNames);
        }

        /// <summary>
        /// MinimumNprobes should chain successfully and execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_MinimumNprobes_ChainsAndExecutes()
        {
            using var fixture = await CreateVectorTextFixture("vq_min_nprobes");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .MinimumNprobes(1);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// MaximumNprobes should chain successfully and execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_MaximumNprobes_ChainsAndExecutes()
        {
            using var fixture = await CreateVectorTextFixture("vq_max_nprobes");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .MaximumNprobes(50);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// MaximumNprobes(0) means unlimited.
        /// </summary>
        [Fact]
        public async Task VectorQuery_MaximumNprobes_ZeroMeansUnlimited()
        {
            using var fixture = await CreateVectorTextFixture("vq_max_np_zero");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .MaximumNprobes(0);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// AddQueryVector should add an additional search vector and execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_AddQueryVector_ExecutesSuccessfully()
        {
            using var fixture = await CreateVectorTextFixture("vq_add_vec");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .AddQueryVector(new float[] { 0.0f, 1.0f, 0.0f });
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// VectorQuery with timeout and maxBatchLength should execute successfully.
        /// </summary>
        [Fact]
        public async Task VectorQuery_ToArrow_WithTimeoutAndBatchLength()
        {
            using var fixture = await CreateVectorTextFixture("vq_timeout_batch");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            var batch = await query.ToArrow(
                timeout: TimeSpan.FromSeconds(30),
                maxBatchLength: 2);

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        // ----- FFI Error Surfacing Tests -----

        /// <summary>
        /// Executing a query with an invalid WHERE filter should throw LanceDbException.
        /// The error is raised asynchronously during query execution on the Rust side.
        /// </summary>
        [Fact]
        public async Task Query_InvalidWhereFilter_ThrowsLanceDbExceptionOnExecute()
        {
            using var fixture = await TestFixture.CreateWithTable("q_bad_where");
            await fixture.Table.Add(CreateTestBatch(3));

            using var query = fixture.Table.Query()
                .Where("INVALID %%% SYNTAX");

            await Assert.ThrowsAsync<LanceDbException>(() => query.ToArrow());
        }

        /// <summary>
        /// CountRows with an invalid SQL filter should throw LanceDbException.
        /// </summary>
        [Fact]
        public async Task Table_CountRows_InvalidFilter_ThrowsLanceDbException()
        {
            using var fixture = await TestFixture.CreateWithTable("count_bad_filter");
            await fixture.Table.Add(CreateTestBatch(3));

            await Assert.ThrowsAsync<LanceDbException>(
                () => fixture.Table.CountRows("INVALID %%% SQL"));
        }

        /// <summary>
        /// Delete with an invalid SQL predicate should throw LanceDbException.
        /// </summary>
        [Fact]
        public async Task Table_Delete_InvalidPredicate_ThrowsLanceDbException()
        {
            using var fixture = await TestFixture.CreateWithTable("delete_bad_pred");
            await fixture.Table.Add(CreateTestBatch(3));

            await Assert.ThrowsAsync<LanceDbException>(
                () => fixture.Table.Delete("INVALID %%% SYNTAX"));
        }

        /// <summary>
        /// NearestTo on a table that has no vector column should throw LanceDbException
        /// when executed (at query execution time, not at build time).
        /// </summary>
        [Fact]
        public async Task NearestTo_NoVectorColumn_ThrowsLanceDbExceptionOnExecute()
        {
            using var fixture = await TestFixture.CreateWithTable("nearest_novector");
            await fixture.Table.Add(CreateTestBatch(3));

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 2.0, 3.0 });

            await Assert.ThrowsAsync<LanceDbException>(() => query.ToArrow());
        }

        /// <summary>
        /// VectorQuery.Select with valid columns but invalid expression should throw
        /// LanceDbException on execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_InvalidSelectExpression_ThrowsLanceDbExceptionOnExecute()
        {
            using var fixture = await CreateVectorTextFixture("vq_bad_select_exec");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Select(new Dictionary<string, string>
                {
                    { "bad_col", "INVALID_FUNCTION(nonexistent)" }
                });

            await Assert.ThrowsAsync<LanceDbException>(() => query.ToArrow());
        }

        /// <summary>
        /// Update with an invalid column expression should throw LanceDbException.
        /// </summary>
        [Fact]
        public async Task Table_Update_InvalidExpression_ThrowsLanceDbException()
        {
            using var fixture = await TestFixture.CreateWithTable("update_bad_expr");
            await fixture.Table.Add(CreateTestBatch(3));

            var ex = await Assert.ThrowsAsync<LanceDbException>(
                () => fixture.Table.Update(
                    new Dictionary<string, string> { { "id", "abc()" } }));
            Assert.Contains("lance error: Invalid user input", ex.Message);
            Assert.Contains("Error during planning: Invalid function 'abc'", ex.Message);
        }
    }
}
