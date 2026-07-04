namespace lancedb.tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using lancedb;
    using Xunit;
    using static TestHelpers;

    /// <summary>
    /// Tests for <see cref="LsmWriteSpec"/> construction and the
    /// <see cref="Table.SetUnenforcedPrimaryKey(string)"/>,
    /// <see cref="Table.SetLsmWriteSpec"/>, and <see cref="Table.UnsetLsmWriteSpec"/>
    /// table operations.
    /// </summary>
    public class LsmWriteSpecTests
    {
        [Fact]
        public void Bucket_NullColumn_ThrowsArgumentNull()
        {
            Assert.Throws<ArgumentNullException>(() => LsmWriteSpec.Bucket(null!, 8));
        }

        [Fact]
        public void Identity_NullColumn_ThrowsArgumentNull()
        {
            Assert.Throws<ArgumentNullException>(() => LsmWriteSpec.Identity(null!));
        }

        [Fact]
        public void WithMaintainedIndexes_Null_ThrowsArgumentNull()
        {
            Assert.Throws<ArgumentNullException>(() => LsmWriteSpec.Unsharded().WithMaintainedIndexes(null!));
        }

        [Fact]
        public void WithWriterConfigDefaults_Null_ThrowsArgumentNull()
        {
            Assert.Throws<ArgumentNullException>(() => LsmWriteSpec.Unsharded().WithWriterConfigDefaults(null!));
        }

        [Fact]
        public async Task SetLsmWriteSpec_NullSpec_ThrowsArgumentNull()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_nullspec", CreateTwoColumnBatch(5));
            await Assert.ThrowsAsync<ArgumentNullException>(() => fixture.Table.SetLsmWriteSpec(null!));
        }

        [Fact]
        public async Task SetUnenforcedPrimaryKey_NullColumn_ThrowsArgumentNull()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_nullpk", CreateTwoColumnBatch(5));
            await Assert.ThrowsAsync<ArgumentNullException>(() => fixture.Table.SetUnenforcedPrimaryKey((string)null!));
        }

        [Fact]
        public async Task SetUnenforcedPrimaryKey_SingleColumn_Succeeds()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_pk", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");
        }

        [Fact]
        public async Task SetLsmWriteSpec_Unsharded_RoundTrips()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_unsharded", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");

            await fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Unsharded());
            await fixture.Table.UnsetLsmWriteSpec();
        }

        [Fact]
        public async Task SetLsmWriteSpec_Bucket_RoundTrips()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_bucket", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");

            await fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Bucket("id", 16));
            await fixture.Table.UnsetLsmWriteSpec();
        }

        [Fact]
        public async Task SetLsmWriteSpec_Identity_RoundTrips()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_identity", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");

            await fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Identity("id"));
            await fixture.Table.UnsetLsmWriteSpec();
        }

        [Fact]
        public async Task SetLsmWriteSpec_WithMaintainedIndexes_Succeeds()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_maintained", CreateTwoColumnBatch(20));
            await fixture.Table.CreateIndex(new[] { "id" }, new BTreeIndex(), name: "id_idx");
            await fixture.Table.SetUnenforcedPrimaryKey("id");

            var spec = LsmWriteSpec.Bucket("id", 8).WithMaintainedIndexes(new[] { "id_idx" });
            await fixture.Table.SetLsmWriteSpec(spec);
            await fixture.Table.UnsetLsmWriteSpec();
        }

        [Fact]
        public async Task SetLsmWriteSpec_WithWriterConfigDefaults_Succeeds()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_defaults", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");

            var spec = LsmWriteSpec.Unsharded()
                .WithWriterConfigDefaults(new Dictionary<string, string> { ["max_rows_per_shard"] = "1000" });
            await fixture.Table.SetLsmWriteSpec(spec);
            await fixture.Table.UnsetLsmWriteSpec();
        }

        [Fact]
        public async Task SetLsmWriteSpec_AlreadySet_Throws()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_twice", CreateTwoColumnBatch(10));
            await fixture.Table.SetUnenforcedPrimaryKey("id");
            await fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Unsharded());

            await Assert.ThrowsAsync<LanceDbException>(
                () => fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Unsharded()));
        }

        [Fact]
        public async Task UnsetLsmWriteSpec_NotSet_Throws()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_unsetnone", CreateTwoColumnBatch(10));

            await Assert.ThrowsAsync<LanceDbException>(() => fixture.Table.UnsetLsmWriteSpec());
        }

        // ===== CloseLsmWriters =====

        /// <summary>
        /// <see cref="Table.CloseLsmWriters"/> must succeed as a no-op when no
        /// MemWAL shard writers are cached for the table (i.e., either no
        /// <see cref="LsmWriteSpec"/> is installed or no LSM-routed
        /// <c>merge_insert</c> has run yet).
        /// </summary>
        [Fact]
        public async Task CloseLsmWriters_NoCachedWriters_IsNoOp()
        {
            using var fixture = await TestFixture.CreateWithTable("lsm_close_noop", CreateTwoColumnBatch(5));

            // No spec, no merge_insert: nothing cached. Must not throw.
            await fixture.Table.CloseLsmWriters();
        }

        // ===== MergeInsertBuilder.UseLsmWrite =====

        /// <summary>
        /// <see cref="MergeInsertBuilder.UseLsmWrite(bool)"/> with <c>false</c>
        /// explicitly opts out of the MemWAL LSM write path. The standard
        /// <c>merge_insert</c> path runs even without an
        /// <see cref="LsmWriteSpec"/> installed, and the result reports the
        /// per-row insert/update breakdown.
        /// </summary>
        [Fact]
        public async Task MergeInsert_UseLsmWriteFalse_FallsBackToStandardPath()
        {
            using var fixture = await TestFixture.CreateWithTable(
                "lsm_optout", CreateTwoColumnBatch(3));

            // New data: row 0..2 match (will update), 3..4 are new (will insert).
            var newData = CreateTwoColumnBatch(5);
            var result = await fixture.Table
                .MergeInsert(new[] { "id" })
                .WhenMatchedUpdateAll()
                .WhenNotMatchedInsertAll()
                .UseLsmWrite(false)
                .Execute(newData);

            Assert.Equal((ulong)2, result.NumInsertedRows);
            Assert.Equal((ulong)3, result.NumUpdatedRows);
            Assert.Equal((ulong)0, result.NumDeletedRows);
            Assert.Equal(5L, await fixture.Table.CountRows());
        }

        /// <summary>
        /// <see cref="MergeInsertBuilder.UseLsmWrite(bool)"/> with <c>true</c>
        /// requires an <see cref="LsmWriteSpec"/> to be installed. Calling it
        /// on a table without a spec must fail.
        /// </summary>
        [Fact]
        public async Task MergeInsert_UseLsmWriteTrue_WithoutSpec_Throws()
        {
            using var fixture = await TestFixture.CreateWithTable(
                "lsm_no_spec", CreateTwoColumnBatch(3));

            var newData = CreateTwoColumnBatch(1);
            await Assert.ThrowsAsync<LanceDbException>(() => fixture.Table
                .MergeInsert(new[] { "id" })
                .WhenMatchedUpdateAll()
                .WhenNotMatchedInsertAll()
                .UseLsmWrite(true)
                .Execute(newData));
        }

        /// <summary>
        /// End-to-end LSM write loop: install an <see cref="LsmWriteSpec"/>,
        /// merge_insert through the MemWAL path with
        /// <see cref="MergeInsertBuilder.UseLsmWrite(bool)"/>(<c>true</c>),
        /// drain the cached writer with <see cref="Table.CloseLsmWriters"/>,
        /// then merge_insert again. The LSM path reports
        /// <see cref="MergeResult.NumRows"/> and leaves the per-row
        /// breakdown fields at zero.
        /// </summary>
        [Fact]
        public async Task MergeInsert_UseLsmWriteTrue_WithSpec_RoundTripsThroughCloseLsmWriters()
        {
            using var fixture = await TestFixture.CreateWithTable(
                "lsm_e2e", CreateTwoColumnBatch(3));

            await fixture.Table.SetUnenforcedPrimaryKey("id");
            // num_buckets=1: every row routes to the same shard.
            await fixture.Table.SetLsmWriteSpec(LsmWriteSpec.Bucket("id", 1));

            // First LSM merge — empty `on` defaults to the primary key.
            var firstBatch = CreateTwoColumnBatch(5);
            var firstResult = await fixture.Table
                .MergeInsert(System.Array.Empty<string>())
                .WhenMatchedUpdateAll()
                .WhenNotMatchedInsertAll()
                .UseLsmWrite(true)
                .Execute(firstBatch);

            // LSM path: num_rows reports total written; the insert/update
            // breakdown is unknown until compaction.
            Assert.Equal((ulong)5, firstResult.NumRows);
            Assert.Equal((ulong)0, firstResult.NumInsertedRows);
            Assert.Equal((ulong)0, firstResult.NumUpdatedRows);
            Assert.Equal((ulong)0, firstResult.NumDeletedRows);
            Assert.Equal((ulong)0, firstResult.Version);

            // Drain and close the cached shard writer.
            await fixture.Table.CloseLsmWriters();

            // A subsequent merge_insert reopens the writer lazily.
            var secondBatch = CreateTwoColumnBatch(1);
            var secondResult = await fixture.Table
                .MergeInsert(System.Array.Empty<string>())
                .WhenMatchedUpdateAll()
                .WhenNotMatchedInsertAll()
                .UseLsmWrite(true)
                .Execute(secondBatch);

            Assert.Equal((ulong)1, secondResult.NumRows);
        }
    }
}
