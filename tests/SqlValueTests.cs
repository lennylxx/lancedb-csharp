namespace lancedb.tests
{
    using System;
    using System.Collections.Generic;
    using Xunit;

    public class SqlValueTests
    {
        [Fact]
        public void ValueToSql_String_WrapsInSingleQuotes()
        {
            Assert.Equal("'foo'", SqlValue.ValueToSql("foo"));
        }

        [Fact]
        public void ValueToSql_StringWithSingleQuote_DoublesQuote()
        {
            // O'Brien -> 'O''Brien'  (matches Python value_to_sql escaping)
            Assert.Equal("'O''Brien'", SqlValue.ValueToSql("O'Brien"));
        }

        [Fact]
        public void ValueToSql_StringWithInjection_IsEscapedAsLiteral()
        {
            Assert.Equal("'x''; DROP TABLE t; --'", SqlValue.ValueToSql("x'; DROP TABLE t; --"));
        }

        [Fact]
        public void ValueToSql_Null_ReturnsNull()
        {
            Assert.Equal("NULL", SqlValue.ValueToSql(null));
        }

        [Fact]
        public void ValueToSql_Bool_ReturnsUpperCase()
        {
            Assert.Equal("TRUE", SqlValue.ValueToSql(true));
            Assert.Equal("FALSE", SqlValue.ValueToSql(false));
        }

        [Fact]
        public void ValueToSql_Integers_ReturnPlainString()
        {
            Assert.Equal("7", SqlValue.ValueToSql(7));
            Assert.Equal("-42", SqlValue.ValueToSql(-42L));
            Assert.Equal("255", SqlValue.ValueToSql((byte)255));
        }

        [Fact]
        public void ValueToSql_Floats_UseInvariantCulture()
        {
            Assert.Equal("1.5", SqlValue.ValueToSql(1.5));
            Assert.Equal("0.25", SqlValue.ValueToSql(0.25f));
            Assert.Equal("3.14", SqlValue.ValueToSql(3.14m));
        }

        [Fact]
        public void ValueToSql_Bytes_ReturnsHexLiteral()
        {
            Assert.Equal("X'01ff'", SqlValue.ValueToSql(new byte[] { 0x01, 0xff }));
        }

        [Fact]
        public void ValueToSql_List_ReturnsArrayLiteral()
        {
            Assert.Equal("[1, 2, 3]", SqlValue.ValueToSql(new[] { 1, 2, 3 }));
            Assert.Equal("['a', 'b']", SqlValue.ValueToSql(new List<string> { "a", "b" }));
        }

        [Fact]
        public void ValueToSql_DateTime_ReturnsIsoLiteral()
        {
            var dt = new DateTime(2021, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            Assert.Equal("'2021-01-02T03:04:05'", SqlValue.ValueToSql(dt));
        }

        [Fact]
        public void ValueToSql_UnsupportedType_Throws()
        {
            Assert.Throws<NotSupportedException>(() => SqlValue.ValueToSql(new object()));
        }
    }
}
