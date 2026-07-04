namespace lancedb.tests
{
    using Apache.Arrow;
    using static TestHelpers;

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
            using var fixture = await TestFixture.CreateTwoColumnFixture("select_cols");

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
            using var fixture = await TestFixture.CreateTwoColumnFixture("select_transform");
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
            using var fixture = await TestFixture.CreateTwoColumnFixture("types_test");

            using var query = fixture.Table.Query();
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal(0, rows[0]["id"]);
            Assert.Equal("name_0", rows[0]["name"]);
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
            using var fixture = await TestFixture.CreateTextFixture("ntt_basic");

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
            using var fixture = await TestFixture.CreateTextFixture("ntt_chain");

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
            using var fixture = await TestFixture.CreateTextFixture("ntt_empty");

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
            using var fixture = await TestFixture.CreateTextFixture("ntt_fast");

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
            using var fixture = await TestFixture.CreateTextFixture("ntt_dispose");

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
            using var fixture = await TestFixture.CreateTextFixture("ntt_noconsume");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            using var baseQuery = fixture.Table.Query();
            using var ftsQuery = baseQuery.NearestToText("apple");
            var ftsRows = await ftsQuery.ToList();

            var allRows = await baseQuery.ToList();

            Assert.Single(ftsRows);
            Assert.Equal(3, allRows.Count);

            // The FTS hit must be one of the rows the base query also returns —
            // proves the two queries share the same underlying table data and
            // that NearestToText didn't consume the base query's state.
            int ftsId = (int)ftsRows[0]["id"]!;
            bool found = false;
            foreach (var row in allRows)
            {
                if ((int)row["id"]! == ftsId) { found = true; break; }
            }
            Assert.True(found, $"FTS hit id={ftsId} should also appear in base query results");
        }

        /// <summary>
        /// NearestToText chained with NearestTo should produce hybrid search results.
        /// </summary>
        [Fact]
        public async Task NearestToText_NearestTo_ReturnsHybridResults()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("hybrid_exec");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = fixture.Table.Query()
                .NearestToText("apple")
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            var rows = await query.ToList();

            Assert.NotEmpty(rows);
        }

        /// <summary>
        /// Hybrid search via vector-first path (NearestTo then NearestToText)
        /// should also work.
        /// </summary>
        [Fact]
        public async Task HybridSearch_VectorFirst_ReturnsResults()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("hybrid_vecfirst");

            await fixture.Table.CreateIndex(new[] { "content" }, new FtsIndex());

            var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .NearestToText("apple");
            var rows = await query.ToList();

            Assert.NotEmpty(rows);
        }

        /// <summary>
        /// NearestToText with columns parameter should only search specified columns.
        /// </summary>
        [Fact]
        public async Task NearestToText_WithColumns_SearchesSpecifiedColumns()
        {
            using var fixture = await TestFixture.CreateMultiTextFixture("ntt_columns");

            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("apple", new[] { "title" });
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple pie", rows[0]["title"]);
        }

        /// <summary>
        /// NearestToText with columns parameter should only search specified columns (body).
        /// </summary>
        [Fact]
        public async Task NearestToText_WithColumnsBody_SearchesSpecifiedColumns()
        {
            using var fixture = await TestFixture.CreateMultiTextFixture("fts_columns");

            await fixture.Table.CreateIndex(new[] { "title" }, new FtsIndex());
            await fixture.Table.CreateIndex(new[] { "body" }, new FtsIndex());

            using var query = fixture.Table.Query()
                .NearestToText("apple", new[] { "body" });
            var rows = await query.ToList();

            Assert.Single(rows);
            Assert.Equal("apple sauce recipe", rows[0]["body"]);
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
            // Guard against ExplainPlan returning a stub string. A real plan
            // is substantial (operator names + line breaks + indentation).
            Assert.True(plan.Length > 30, $"Plan unexpectedly short: '{plan}'");
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
        /// Round-trips every settable base-query parameter through SerializeParamsUtf8
        /// and the Rust parse_query_params deserializer. If any builder field stops
        /// being included in the serialized JSON, one of the asserted effects below
        /// will silently revert to the default and the test will fail.
        /// </summary>
        [Fact]
        public async Task FullyPopulatedQuery_AllParamsRoundTripThroughJson()
        {
            using var fixture = await TestFixture.CreateWithTable("params_roundtrip");
            await fixture.Table.Add(CreateTestBatch(20));

            using var query = fixture.Table.Query()
                .Select(new[] { "id" })
                .Where("id >= 10")
                .Limit(3)
                .Offset(2)
                .WithRowId()
                .FastSearch()
                .Postfilter();

            var batch = await query.ToArrow();

            // Limit + Offset: 20 rows, filter keeps ids [10..19] (10 rows), offset 2 + limit 3 → 3 rows.
            Assert.Equal(3, batch.Length);
            // Select: only "id" projected (plus _rowid from WithRowId).
            var fieldNames = batch.Schema.FieldsList.Select(f => f.Name).ToHashSet();
            Assert.Contains("id", fieldNames);
            Assert.Contains("_rowid", fieldNames);
            Assert.DoesNotContain("data", fieldNames);
            // Where + Offset: first returned id must be 10 + 2 = 12.
            var idArray = (Int32Array)batch.Column("id");
            Assert.Equal(12, idArray.GetValue(0));
            Assert.Equal(13, idArray.GetValue(1));
            Assert.Equal(14, idArray.GetValue(2));

            // FastSearch + Postfilter don't affect this result set but must still
            // deserialize without error — covered by ExplainPlan completing.
            string plan = await query.ExplainPlan();
            Assert.NotEmpty(plan);
        }

        /// <summary>
        /// Exercises the primary <c>NearestTo(float[])</c> path so the f32-native
        /// FFI route is tested directly (the existing tests use the <c>double[]</c>
        /// back-compat overload).
        /// </summary>
        [Fact]
        public async Task NearestTo_FloatVector_ExecutesOnPrimaryPath()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("nearest_to_float");

            var query = fixture.Table.Query()
                .NearestTo(new float[] { 1.0f, 0.0f, 0.0f })
                .Limit(3);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0 && batch.Length <= 3);
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
            Assert.Single(schema.FieldsList);
            Assert.Equal("id", schema.FieldsList[0].Name);
            Assert.IsType<Apache.Arrow.Types.Int32Type>(schema.FieldsList[0].DataType);
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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_explain");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_analyze");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_out_schema");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_min_nprobes");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_max_nprobes");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_max_np_zero");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .MaximumNprobes(0);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// ApproxMode should chain successfully and execute for each speed/accuracy
        /// tradeoff. It only affects RQ-quantized indexes but is a no-op otherwise.
        /// </summary>
        [Theory]
        [InlineData(ApproxMode.Fast)]
        [InlineData(ApproxMode.Normal)]
        [InlineData(ApproxMode.Accurate)]
        public async Task VectorQuery_ApproxMode_ChainsAndExecutes(ApproxMode mode)
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_approx_mode");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ApproxMode(mode);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// Plain Nprobes(int) should chain successfully and execute. Mirrors the
        /// MinimumNprobes/MaximumNprobes coverage for the single-value setter.
        /// </summary>
        [Fact]
        public async Task VectorQuery_Nprobes_ChainsAndExecutes()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_nprobes");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .Nprobes(20);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// RefineFactor should chain successfully and execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_RefineFactor_ChainsAndExecutes()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_refine");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .RefineFactor(10);
            var batch = await query.ToArrow();

            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        /// <summary>
        /// DistanceRange on a plain VectorQuery should filter results by L2 bounds.
        /// The CreateVectorTextFixture has 3 orthogonal vectors: query (1,0,0) is at
        /// distance 0 from row 0, and ~1.414 from the others. An upper bound of 1.0
        /// should isolate row 0.
        /// </summary>
        [Fact]
        public async Task VectorQuery_DistanceRange_FiltersResults()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_distrange");

            var allResults = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .ToArrow();
            Assert.Equal(3, allResults.Length);

            var filtered = await fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 })
                .DistanceRange(upperBound: 1.0f)
                .ToArrow();
            Assert.True(filtered.Length < allResults.Length,
                $"Expected fewer than {allResults.Length} rows under bound 1.0, got {filtered.Length}");
            Assert.True(filtered.Length > 0);
        }

        /// <summary>
        /// AddQueryVector should add an additional search vector and execute.
        /// </summary>
        [Fact]
        public async Task VectorQuery_AddQueryVector_ExecutesSuccessfully()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_add_vec");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_timeout_batch");

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
            using var fixture = await TestFixture.CreateVectorTextFixture("vq_bad_select_exec");

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

        [Fact]
        public async Task ToBatches_Query_StreamsAllRows()
        {
            using var fixture = await TestFixture.CreateWithTable("tobatches_query");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query();
            using var reader = await query.ToBatches();

            int totalRows = 0;
            await foreach (var batch in reader)
            {
                Assert.NotNull(batch);
                totalRows += batch.Length;
            }

            Assert.Equal(10, totalRows);
        }

        [Fact]
        public async Task ToBatches_EmptyTable_StreamsZeroBatches()
        {
            using var fixture = await TestFixture.CreateWithTable("tobatches_empty");

            using var query = fixture.Table.Query();
            using var reader = await query.ToBatches();

            int batchCount = 0;
            await foreach (var batch in reader)
            {
                batchCount++;
            }

            Assert.Equal(0, batchCount);
        }

        [Fact]
        public async Task ToBatches_VectorQuery_StreamsResults()
        {
            using var fixture = await TestFixture.CreateVectorTextFixture("tobatches_vq");

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 1.0, 0.0, 0.0 });
            using var reader = await query.ToBatches();

            int totalRows = 0;
            await foreach (var batch in reader)
            {
                Assert.NotNull(batch);
                totalRows += batch.Length;
            }

            Assert.True(totalRows > 0);
        }

        /// <summary>
        /// ToBatches with maxBatchLength on a full scan should split the result
        /// into multiple native batches, each no larger than the limit. Verifies
        /// the Rust crate honors max_batch_length (fixed in lancedb v0.27.2).
        /// </summary>
        [Fact]
        public async Task ToBatches_Query_RespectsMaxBatchLength()
        {
            using var fixture = await TestFixture.CreateWithTable("tobatches_maxlen");
            await fixture.Table.Add(CreateTestBatch(10));

            using var query = fixture.Table.Query();
            using var reader = await query.ToBatches(maxBatchLength: 4);

            int totalRows = 0;
            int batchCount = 0;
            await foreach (var batch in reader)
            {
                Assert.NotNull(batch);
                Assert.True(batch.Length <= 4, $"batch of {batch.Length} exceeds maxBatchLength 4");
                totalRows += batch.Length;
                batchCount++;
            }

            Assert.Equal(10, totalRows);
            Assert.True(batchCount > 1, "expected the result to be split into multiple batches");
        }

        /// <summary>
        /// ToBatches with maxBatchLength on a vector query (native streaming path,
        /// no reranker) should split results into batches no larger than the limit.
        /// Verifies the Rust crate honors max_batch_length for vector queries
        /// (fixed in lancedb v0.27.2).
        /// </summary>
        [Fact]
        public async Task ToBatches_VectorQuery_RespectsMaxBatchLength()
        {
            using var fixture = await TestFixture.CreateWithTable("tobatches_vq_maxlen",
                CreateVectorBatch(20, dimension: 8));

            var queryVector = new double[8];
            for (int i = 0; i < 8; i++)
            {
                queryVector[i] = 0.5;
            }

            using var query = fixture.Table.Query()
                .NearestTo(queryVector)
                .Limit(20);
            using var reader = await query.ToBatches(maxBatchLength: 5);

            int totalRows = 0;
            int batchCount = 0;
            await foreach (var batch in reader)
            {
                Assert.NotNull(batch);
                Assert.True(batch.Length <= 5, $"batch of {batch.Length} exceeds maxBatchLength 5");
                totalRows += batch.Length;
                batchCount++;
            }

            Assert.Equal(20, totalRows);
            Assert.True(batchCount > 1, "expected the result to be split into multiple batches");
        }

        /// <summary>
        /// A vector search without an explicit column should auto-discover a vector column
        /// nested inside a struct ("meta.vector"). Verifies nested vector-column discovery
        /// (fixed in the lancedb core bundled with the Rust upgrade).
        /// </summary>
        [Fact]
        public async Task NearestTo_NestedVectorColumn_AutoDiscovered()
        {
            using var fixture = await TestFixture.CreateWithTable("nested_vec_discovery",
                CreateNestedVectorBatch(300, dimension: 4));

            using var query = fixture.Table.Query()
                .NearestTo(new double[] { 0.5, 0.5, 0.5, 0.5 })
                .Limit(3);
            var result = await query.ToArrow();

            Assert.Equal(3, result.Length);
        }
    }
}
