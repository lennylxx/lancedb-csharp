namespace lancedb.tests
{
    using System;
    using System.Collections.Generic;
    using lancedb;
    using Xunit;

    /// <summary>
    /// Unit tests for the <see cref="Expr"/> type-safe expression builder, verifying
    /// the SQL fragment produced by each builder method.
    /// </summary>
    public class ExprTests
    {
        // ── col ──────────────────────────────────────────────────────────────

        [Fact]
        public void Col_SimpleName_BacktickQuoted()
        {
            Assert.Equal("`age`", Expr.Col("age").ToSql());
        }

        [Fact]
        public void Col_DottedName_SingleIdentifier()
        {
            Assert.Equal("`meta.label`", Expr.Col("meta.label").ToSql());
        }

        [Fact]
        public void Col_EmbeddedBacktick_Doubled()
        {
            Assert.Equal("`a``b`", Expr.Col("a`b").ToSql());
        }

        [Fact]
        public void Col_EmptyName_Throws()
        {
            Assert.Throws<ArgumentException>(() => Expr.Col(""));
        }

        // ── lit ──────────────────────────────────────────────────────────────

        [Fact]
        public void Lit_String_QuotedAndEscaped()
        {
            Assert.Equal("'alice'", Expr.Lit("alice").ToSql());
            Assert.Equal("'O''Brien'", Expr.Lit("O'Brien").ToSql());
        }

        [Fact]
        public void Lit_Int_PlainDecimal()
        {
            Assert.Equal("18", Expr.Lit(18).ToSql());
        }

        [Fact]
        public void Lit_Bool_UpperKeyword()
        {
            Assert.Equal("TRUE", Expr.Lit(true).ToSql());
            Assert.Equal("FALSE", Expr.Lit(false).ToSql());
        }

        [Fact]
        public void Lit_Double_InvariantCulture()
        {
            Assert.Equal("1.5", Expr.Lit(1.5).ToSql());
        }

        [Fact]
        public void Lit_Null_BecomesNull()
        {
            Assert.Equal("NULL", Expr.Lit(null).ToSql());
        }

        [Fact]
        public void Lit_NaN_Throws()
        {
            Assert.Throws<ArgumentException>(() => Expr.Lit(double.NaN));
            Assert.Throws<ArgumentException>(() => Expr.Lit(float.PositiveInfinity));
        }

        // ── comparisons ──────────────────────────────────────────────────────

        [Fact]
        public void Gt_Literal_ParenthesizedBinary()
        {
            Assert.Equal("(`age` > 18)", Expr.Col("age").Gt(18).ToSql());
        }

        [Fact]
        public void Eq_Literal_UsesEquals()
        {
            Assert.Equal("(`name` = 'alice')", Expr.Col("name").Eq("alice").ToSql());
        }

        [Fact]
        public void Ne_Literal_UsesNotEquals()
        {
            Assert.Equal("(`name` <> 'alice')", Expr.Col("name").Ne("alice").ToSql());
        }

        [Fact]
        public void Eq_Null_BecomesIsNull()
        {
            Assert.Equal("(`x` IS NULL)", Expr.Col("x").Eq(null).ToSql());
        }

        [Fact]
        public void Ne_Null_BecomesIsNotNull()
        {
            Assert.Equal("(`x` IS NOT NULL)", Expr.Col("x").Ne(null).ToSql());
        }

        [Fact]
        public void Comparisons_AllOperators()
        {
            Assert.Equal("(`a` < 1)", Expr.Col("a").Lt(1).ToSql());
            Assert.Equal("(`a` <= 1)", Expr.Col("a").Lte(1).ToSql());
            Assert.Equal("(`a` >= 1)", Expr.Col("a").Gte(1).ToSql());
        }

        [Fact]
        public void Compare_TwoColumns()
        {
            Assert.Equal("(`a` > `b`)", Expr.Col("a").Gt(Expr.Col("b")).ToSql());
        }

        // ── logical ──────────────────────────────────────────────────────────

        [Fact]
        public void And_Or_Not()
        {
            var a = Expr.Col("age").Gt(18);
            var b = Expr.Col("status").Eq("active");
            Assert.Equal("((`age` > 18) AND (`status` = 'active'))", a.And(b).ToSql());
            Assert.Equal("((`age` > 18) OR (`status` = 'active'))", a.Or(b).ToSql());
            Assert.Equal("(NOT (`age` > 18))", a.Not().ToSql());
        }

        // ── arithmetic ───────────────────────────────────────────────────────

        [Fact]
        public void Arithmetic_AllOperators()
        {
            Assert.Equal("(`price` * 1.1)", Expr.Col("price").Mul(1.1).ToSql());
            Assert.Equal("(`x` + 1)", Expr.Col("x").Add(1).ToSql());
            Assert.Equal("(`x` - 1)", Expr.Col("x").Sub(1).ToSql());
            Assert.Equal("(`x` / 2)", Expr.Col("x").Div(2).ToSql());
        }

        // ── string ───────────────────────────────────────────────────────────

        [Fact]
        public void StringMethods()
        {
            Assert.Equal("lower(`name`)", Expr.Col("name").Lower().ToSql());
            Assert.Equal("upper(`name`)", Expr.Col("name").Upper().ToSql());
            Assert.Equal("contains(`name`, 'ali')", Expr.Col("name").Contains("ali").ToSql());
        }

        // ── func ─────────────────────────────────────────────────────────────

        [Fact]
        public void Func_RendersCall()
        {
            Assert.Equal("lower(`name`)", Expr.Func("lower", Expr.Col("name")).ToSql());
            Assert.Equal("concat(`a`, 'x')", Expr.Func("concat", Expr.Col("a"), "x").ToSql());
        }

        [Fact]
        public void Func_NoArgs()
        {
            Assert.Equal("now()", Expr.Func("now").ToSql());
        }

        [Fact]
        public void Func_InvalidName_Throws()
        {
            Assert.Throws<ArgumentException>(() => Expr.Func("bad name", Expr.Col("a")));
            Assert.Throws<ArgumentException>(() => Expr.Func("drop;table", Expr.Col("a")));
        }

        // ── cast ─────────────────────────────────────────────────────────────

        [Theory]
        [InlineData("bool", "BOOLEAN")]
        [InlineData("int8", "TINYINT")]
        [InlineData("int16", "SMALLINT")]
        [InlineData("int32", "INT")]
        [InlineData("int64", "BIGINT")]
        [InlineData("uint8", "TINYINT UNSIGNED")]
        [InlineData("uint16", "SMALLINT UNSIGNED")]
        [InlineData("uint32", "INT UNSIGNED")]
        [InlineData("uint64", "BIGINT UNSIGNED")]
        [InlineData("float32", "FLOAT")]
        [InlineData("float", "FLOAT")]
        [InlineData("float64", "DOUBLE")]
        [InlineData("double", "DOUBLE")]
        [InlineData("string", "STRING")]
        [InlineData("str", "STRING")]
        [InlineData("utf8", "STRING")]
        [InlineData("date32", "DATE")]
        [InlineData("date", "DATE")]
        public void Cast_MapsTypeNames(string dataType, string sqlType)
        {
            Assert.Equal($"CAST(`id` AS {sqlType})", Expr.Col("id").Cast(dataType).ToSql());
        }

        [Fact]
        public void Cast_UnknownType_ThrowsArgument()
        {
            Assert.Throws<ArgumentException>(() => Expr.Col("id").Cast("frobnicate"));
        }

        [Theory]
        [InlineData("float16")]
        [InlineData("large_string")]
        [InlineData("date64")]
        public void Cast_UnsupportedByLance_ThrowsNotSupported(string dataType)
        {
            Assert.Throws<NotSupportedException>(() => Expr.Col("id").Cast(dataType));
        }

        // ── operators ────────────────────────────────────────────────────────

        [Fact]
        public void Operators_Relational()
        {
            Assert.Equal("(`age` > 18)", (Expr.Col("age") > 18).ToSql());
            Assert.Equal("(`age` < 18)", (Expr.Col("age") < 18).ToSql());
            Assert.Equal("(`age` >= 18)", (Expr.Col("age") >= 18).ToSql());
            Assert.Equal("(`age` <= 18)", (Expr.Col("age") <= 18).ToSql());
        }

        [Fact]
        public void Operators_Reflected()
        {
            Assert.Equal("(18 < `age`)", (18 < Expr.Col("age")).ToSql());
            Assert.Equal("(1 + `x`)", (1 + Expr.Col("x")).ToSql());
        }

        [Fact]
        public void Operators_Logical()
        {
            var a = Expr.Col("age") > 18;
            var b = Expr.Col("name").Eq("alice");
            Assert.Equal("((`age` > 18) AND (`name` = 'alice'))", (a & b).ToSql());
            Assert.Equal("((`age` > 18) OR (`name` = 'alice'))", (a | b).ToSql());
            Assert.Equal("(NOT (`age` > 18))", (!a).ToSql());
        }

        [Fact]
        public void Operators_Arithmetic()
        {
            Assert.Equal("(`price` * 1.1)", (Expr.Col("price") * 1.1).ToSql());
            Assert.Equal("(`x` + `y`)", (Expr.Col("x") + Expr.Col("y")).ToSql());
        }

        // ── misc ─────────────────────────────────────────────────────────────

        [Fact]
        public void ToString_WrapsSql()
        {
            Assert.Equal("Expr((`age` > 18))", (Expr.Col("age") > 18).ToString());
        }

        [Fact]
        public void Composite_DocExample()
        {
            // (col("age") > 18) AND (col("status") == "active")
            var filt = (Expr.Col("age") > 18).And(Expr.Col("status").Eq("active"));
            Assert.Equal("((`age` > 18) AND (`status` = 'active'))", filt.ToSql());
        }
    }
}
