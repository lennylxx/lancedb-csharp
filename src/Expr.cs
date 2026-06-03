namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// A type-safe expression node for building filters and projections without
    /// hand-writing SQL strings.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Construct instances with <see cref="Col(string)"/> and <see cref="Lit(object?)"/>,
    /// then combine them using the named methods (<c>Eq</c>, <c>Gt</c>, <c>And</c>, …) or
    /// the overloaded operators (<c>&gt;</c>, <c>&lt;</c>, <c>&amp;</c>, <c>|</c>, <c>!</c>,
    /// <c>+</c>, <c>-</c>, <c>*</c>, <c>/</c>). The resulting expression can be passed to
    /// <see cref="QueryBase{T}.Where(Expr)"/> or
    /// <see cref="QueryBase{T}.Select(Dictionary{string, Expr})"/>.
    /// </para>
    /// <para>
    /// Equality is expressed via
    /// the <see cref="Eq(object?)"/> / <see cref="Ne(object?)"/> methods rather than the
    /// <c>==</c> / <c>!=</c> operators (those retain their normal reference-equality meaning
    /// in C#).
    /// </para>
    /// <example>
    /// <code>
    /// // filter: age &gt; 18 AND status = 'active'
    /// var filt = (Expr.Col("age") &gt; 18).And(Expr.Col("status").Eq("active"));
    ///
    /// // projection: compute a derived column
    /// var proj = new Dictionary&lt;string, Expr&gt; { ["score"] = Expr.Col("raw_score") * 1.5 };
    ///
    /// var rows = await table.Query().Where(filt).Select(proj).ToList();
    /// </code>
    /// </example>
    /// </remarks>
    public sealed class Expr
    {
        private readonly string _sql;

        private Expr(string sql)
        {
            _sql = sql;
        }

        // ── factories ────────────────────────────────────────────────────────

        /// <summary>
        /// Reference a table column by name.
        /// </summary>
        /// <remarks>
        /// The name is preserved exactly as given (case-sensitive) and treated as a single,
        /// atomic column identifier. A dotted name such as <c>meta.label</c> therefore refers
        /// to a column literally named <c>meta.label</c>, not a nested struct field. To filter
        /// on a nested struct field path, use a raw SQL string with
        /// <see cref="QueryBase{T}.Where(string)"/> instead.
        /// </remarks>
        /// <param name="name">The column name.</param>
        /// <returns>A column-reference expression.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="name"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="name"/> is empty.</exception>
        public static Expr Col(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (name.Length == 0)
            {
                throw new ArgumentException("Column name must not be empty.", nameof(name));
            }

            return new Expr("`" + name.Replace("`", "``") + "`");
        }

        /// <summary>
        /// Create a literal (constant) value expression.
        /// </summary>
        /// <remarks>
        /// Supported values mirror <see cref="SqlValue.ValueToSql(object?)"/> (e.g.
        /// <see cref="bool"/>, integer types, <see cref="float"/>/<see cref="double"/>,
        /// <see cref="string"/>, <c>byte[]</c>, and <c>null</c>).
        /// </remarks>
        /// <param name="value">The literal value.</param>
        /// <returns>A literal expression.</returns>
        /// <exception cref="ArgumentException">Thrown if <paramref name="value"/> is a non-finite (NaN or infinite) float or double.</exception>
        public static Expr Lit(object? value)
        {
            switch (value)
            {
                case float f when float.IsNaN(f) || float.IsInfinity(f):
                    throw new ArgumentException("Cannot create a literal from a non-finite float.", nameof(value));
                case double d when double.IsNaN(d) || double.IsInfinity(d):
                    throw new ArgumentException("Cannot create a literal from a non-finite double.", nameof(value));
                default:
                    return new Expr(SqlValue.ValueToSql(value));
            }
        }

        /// <summary>
        /// Call a SQL scalar function by name.
        /// </summary>
        /// <param name="name">The SQL function name (e.g. <c>"lower"</c>, <c>"concat"</c>). Must be a valid identifier.</param>
        /// <param name="args">The function arguments, each an <see cref="Expr"/> or a plain literal value.</param>
        /// <returns>A function-call expression.</returns>
        /// <exception cref="ArgumentException">Thrown if <paramref name="name"/> is not a valid SQL identifier.</exception>
        public static Expr Func(string name, params object?[] args)
        {
            if (!IsValidIdentifier(name))
            {
                throw new ArgumentException(
                    $"'{name}' is not a valid SQL function name.", nameof(name));
            }

            var sb = new StringBuilder();
            sb.Append(name).Append('(');
            if (args != null)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    if (i > 0)
                    {
                        sb.Append(", ");
                    }

                    sb.Append(Coerce(args[i]));
                }
            }

            sb.Append(')');
            return new Expr(sb.ToString());
        }

        // ── comparisons ──────────────────────────────────────────────────────

        /// <summary>Equal to. A null operand produces an <c>IS NULL</c> check.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Eq(object? other)
        {
            if (other == null)
            {
                return new Expr($"({_sql} IS NULL)");
            }

            return Binary("=", other);
        }

        /// <summary>Not equal to. A null operand produces an <c>IS NOT NULL</c> check.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Ne(object? other)
        {
            if (other == null)
            {
                return new Expr($"({_sql} IS NOT NULL)");
            }

            return Binary("<>", other);
        }

        /// <summary>Less than.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Lt(object? other)
        {
            return Binary("<", other);
        }

        /// <summary>Less than or equal to.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Lte(object? other)
        {
            return Binary("<=", other);
        }

        /// <summary>Greater than.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Gt(object? other)
        {
            return Binary(">", other);
        }

        /// <summary>Greater than or equal to.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value to compare against.</param>
        /// <returns>The comparison expression.</returns>
        public Expr Gte(object? other)
        {
            return Binary(">=", other);
        }

        // ── logical ──────────────────────────────────────────────────────────

        /// <summary>Logical AND.</summary>
        /// <param name="other">The right-hand expression.</param>
        /// <returns>The combined expression.</returns>
        public Expr And(Expr other)
        {
            return Binary("AND", other);
        }

        /// <summary>Logical OR.</summary>
        /// <param name="other">The right-hand expression.</param>
        /// <returns>The combined expression.</returns>
        public Expr Or(Expr other)
        {
            return Binary("OR", other);
        }

        /// <summary>Logical NOT.</summary>
        /// <returns>The negated expression.</returns>
        public Expr Not()
        {
            return new Expr($"(NOT {_sql})");
        }

        // ── arithmetic ───────────────────────────────────────────────────────

        /// <summary>Add.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value.</param>
        /// <returns>The arithmetic expression.</returns>
        public Expr Add(object? other)
        {
            return Binary("+", other);
        }

        /// <summary>Subtract.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value.</param>
        /// <returns>The arithmetic expression.</returns>
        public Expr Sub(object? other)
        {
            return Binary("-", other);
        }

        /// <summary>Multiply.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value.</param>
        /// <returns>The arithmetic expression.</returns>
        public Expr Mul(object? other)
        {
            return Binary("*", other);
        }

        /// <summary>Divide.</summary>
        /// <param name="other">An <see cref="Expr"/> or literal value.</param>
        /// <returns>The arithmetic expression.</returns>
        public Expr Div(object? other)
        {
            return Binary("/", other);
        }

        // ── string ───────────────────────────────────────────────────────────

        /// <summary>Convert string values to lowercase.</summary>
        /// <returns>The transformed expression.</returns>
        public Expr Lower()
        {
            return new Expr($"lower({_sql})");
        }

        /// <summary>Convert string values to uppercase.</summary>
        /// <returns>The transformed expression.</returns>
        public Expr Upper()
        {
            return new Expr($"upper({_sql})");
        }

        /// <summary>Return true where the string contains <paramref name="substr"/>.</summary>
        /// <param name="substr">An <see cref="Expr"/> or literal substring.</param>
        /// <returns>The predicate expression.</returns>
        public Expr Contains(object? substr)
        {
            return new Expr($"contains({_sql}, {Coerce(substr)})");
        }

        // ── cast ─────────────────────────────────────────────────────────────

        /// <summary>
        /// Cast values to <paramref name="dataType"/>.
        /// </summary>
        /// <param name="dataType">
        /// One of the Arrow type-name strings: <c>"bool"</c>, <c>"int8"</c>, <c>"int16"</c>,
        /// <c>"int32"</c>, <c>"int64"</c>, <c>"uint8"</c>–<c>"uint64"</c>, <c>"float32"</c>,
        /// <c>"float"</c>, <c>"float64"</c>, <c>"double"</c>, <c>"string"</c>/<c>"str"</c>/<c>"utf8"</c>,
        /// <c>"date32"</c>/<c>"date"</c>.
        /// </param>
        /// <returns>The cast expression.</returns>
        /// <exception cref="ArgumentException">Thrown if <paramref name="dataType"/> is not a recognized type name.</exception>
        /// <exception cref="NotSupportedException">Thrown for recognized Arrow types that the LanceDB SQL engine cannot cast to (e.g. <c>"float16"</c>, <c>"large_string"</c>, <c>"date64"</c>).</exception>
        public Expr Cast(string dataType)
        {
            return new Expr($"CAST({_sql} AS {MapCastType(dataType)})");
        }

        // ── utilities ────────────────────────────────────────────────────────

        /// <summary>
        /// Render the expression as a SQL string.
        /// </summary>
        /// <returns>The SQL representation of the expression.</returns>
        public string ToSql()
        {
            return _sql;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"Expr({_sql})";
        }

        // ── operators ────────────────────────────────────────────────────────

        /// <summary>Logical AND operator. See <see cref="And(Expr)"/>.</summary>
        public static Expr operator &(Expr left, Expr right)
        {
            return left.And(right);
        }

        /// <summary>Logical OR operator. See <see cref="Or(Expr)"/>.</summary>
        public static Expr operator |(Expr left, Expr right)
        {
            return left.Or(right);
        }

        /// <summary>Logical NOT operator. See <see cref="Not()"/>.</summary>
        public static Expr operator !(Expr operand)
        {
            return operand.Not();
        }

        /// <summary>Less-than operator. See <see cref="Lt(object?)"/>.</summary>
        public static Expr operator <(Expr left, object? right)
        {
            return left.Lt(right);
        }

        /// <summary>Greater-than operator. See <see cref="Gt(object?)"/>.</summary>
        public static Expr operator >(Expr left, object? right)
        {
            return left.Gt(right);
        }

        /// <summary>Reflected less-than operator (<c>literal &lt; expr</c>).</summary>
        public static Expr operator <(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} < {right._sql})");
        }

        /// <summary>Reflected greater-than operator (<c>literal &gt; expr</c>).</summary>
        public static Expr operator >(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} > {right._sql})");
        }

        /// <summary>Less-than-or-equal operator. See <see cref="Lte(object?)"/>.</summary>
        public static Expr operator <=(Expr left, object? right)
        {
            return left.Lte(right);
        }

        /// <summary>Greater-than-or-equal operator. See <see cref="Gte(object?)"/>.</summary>
        public static Expr operator >=(Expr left, object? right)
        {
            return left.Gte(right);
        }

        /// <summary>Reflected less-than-or-equal operator (<c>literal &lt;= expr</c>).</summary>
        public static Expr operator <=(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} <= {right._sql})");
        }

        /// <summary>Reflected greater-than-or-equal operator (<c>literal &gt;= expr</c>).</summary>
        public static Expr operator >=(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} >= {right._sql})");
        }

        /// <summary>Addition operator. See <see cref="Add(object?)"/>.</summary>
        public static Expr operator +(Expr left, Expr right)
        {
            return left.Binary("+", right);
        }

        /// <summary>Addition operator with a literal right operand.</summary>
        public static Expr operator +(Expr left, object? right)
        {
            return left.Add(right);
        }

        /// <summary>Reflected addition operator (<c>literal + expr</c>).</summary>
        public static Expr operator +(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} + {right._sql})");
        }

        /// <summary>Subtraction operator. See <see cref="Sub(object?)"/>.</summary>
        public static Expr operator -(Expr left, Expr right)
        {
            return left.Binary("-", right);
        }

        /// <summary>Subtraction operator with a literal right operand.</summary>
        public static Expr operator -(Expr left, object? right)
        {
            return left.Sub(right);
        }

        /// <summary>Reflected subtraction operator (<c>literal - expr</c>).</summary>
        public static Expr operator -(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} - {right._sql})");
        }

        /// <summary>Multiplication operator. See <see cref="Mul(object?)"/>.</summary>
        public static Expr operator *(Expr left, Expr right)
        {
            return left.Binary("*", right);
        }

        /// <summary>Multiplication operator with a literal right operand.</summary>
        public static Expr operator *(Expr left, object? right)
        {
            return left.Mul(right);
        }

        /// <summary>Reflected multiplication operator (<c>literal * expr</c>).</summary>
        public static Expr operator *(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} * {right._sql})");
        }

        /// <summary>Division operator. See <see cref="Div(object?)"/>.</summary>
        public static Expr operator /(Expr left, Expr right)
        {
            return left.Binary("/", right);
        }

        /// <summary>Division operator with a literal right operand.</summary>
        public static Expr operator /(Expr left, object? right)
        {
            return left.Div(right);
        }

        /// <summary>Reflected division operator (<c>literal / expr</c>).</summary>
        public static Expr operator /(object? left, Expr right)
        {
            return new Expr($"({Coerce(left)} / {right._sql})");
        }

        // ── helpers ──────────────────────────────────────────────────────────

        private Expr Binary(string op, object? other)
        {
            return new Expr($"({_sql} {op} {Coerce(other)})");
        }

        private static string Coerce(object? value)
        {
            if (value is Expr expr)
            {
                return expr._sql;
            }

            return Lit(value)._sql;
        }

        private static bool IsValidIdentifier(string? name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return false;
            }

            char first = name![0];
            if (!(char.IsLetter(first) || first == '_'))
            {
                return false;
            }

            for (int i = 1; i < name.Length; i++)
            {
                char c = name[i];
                if (!(char.IsLetterOrDigit(c) || c == '_'))
                {
                    return false;
                }
            }

            return true;
        }

        private static string MapCastType(string dataType)
        {
            if (dataType == null)
            {
                throw new ArgumentNullException(nameof(dataType));
            }

            switch (dataType.Trim().ToLowerInvariant())
            {
                case "bool":
                case "boolean":
                    return "BOOLEAN";
                case "int8":
                    return "TINYINT";
                case "int16":
                    return "SMALLINT";
                case "int32":
                    return "INT";
                case "int64":
                    return "BIGINT";
                case "uint8":
                    return "TINYINT UNSIGNED";
                case "uint16":
                    return "SMALLINT UNSIGNED";
                case "uint32":
                    return "INT UNSIGNED";
                case "uint64":
                    return "BIGINT UNSIGNED";
                case "float32":
                case "float":
                    return "FLOAT";
                case "float64":
                case "double":
                    return "DOUBLE";
                case "string":
                case "str":
                case "utf8":
                    return "STRING";
                case "date32":
                case "date":
                    return "DATE";
                case "float16":
                case "large_string":
                case "large_utf8":
                case "date64":
                    throw new NotSupportedException(
                        $"The LanceDB SQL engine does not support casting to '{dataType}'.");
                default:
                    throw new ArgumentException(
                        $"Unsupported data type: '{dataType}'.", nameof(dataType));
            }
        }
    }
}
