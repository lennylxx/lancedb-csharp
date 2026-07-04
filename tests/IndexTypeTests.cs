namespace lancedb.tests
{
    using System.Text.Json;
    using lancedb;
    using Xunit;

    /// <summary>
    /// Unit tests for <see cref="IndexType"/> JSON serialization, covering the
    /// string names exchanged with the native LanceDB layer.
    /// </summary>
    public class IndexTypeTests
    {
        /// <summary>
        /// The FM (substring search) scalar index type should serialize to its
        /// canonical "FM" string name.
        /// </summary>
        [Fact]
        public void Serialize_Fm_WritesFmString()
        {
            string json = JsonSerializer.Serialize(IndexType.Fm);
            Assert.Equal("\"FM\"", json);
        }

        /// <summary>
        /// The "FM" string name should deserialize back to <see cref="IndexType.Fm"/>.
        /// </summary>
        [Fact]
        public void Deserialize_FmString_ReturnsFm()
        {
            var value = JsonSerializer.Deserialize<IndexType>("\"FM\"");
            Assert.Equal(IndexType.Fm, value);
        }

        /// <summary>
        /// The FFI integer for FM should deserialize to <see cref="IndexType.Fm"/>
        /// without disturbing the existing FTS mapping.
        /// </summary>
        [Fact]
        public void Deserialize_FmAndFtsIntegers_MapToDistinctTypes()
        {
            Assert.Equal(IndexType.FTS, JsonSerializer.Deserialize<IndexType>("10"));
            Assert.Equal(IndexType.Fm, JsonSerializer.Deserialize<IndexType>("11"));
        }
    }
}
