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
    }
}
