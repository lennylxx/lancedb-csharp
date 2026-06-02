namespace lancedb
{
    using System;
    using System.Collections;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// Converts CLR values into SQL literal strings, mirroring the LanceDB
    /// Python <c>value_to_sql</c> utility. This is used to safely build the
    /// SQL expressions for literal-value updates so that callers do not have to
    /// hand-format (and correctly escape) values themselves.
    /// </summary>
    public static class SqlValue
    {
        /// <summary>
        /// Convert a value into its SQL literal representation.
        /// </summary>
        /// <param name="value">
        /// The value to convert. Supported types and their SQL forms:
        /// <list type="bullet">
        /// <item><description><c>null</c> becomes <c>NULL</c>.</description></item>
        /// <item><description><see cref="string"/> is wrapped in single quotes with embedded quotes doubled (e.g. <c>O'Brien</c> becomes <c>'O''Brien'</c>).</description></item>
        /// <item><description><see cref="bool"/> becomes <c>TRUE</c> or <c>FALSE</c>.</description></item>
        /// <item><description>Integer types are emitted as plain decimal text.</description></item>
        /// <item><description><see cref="float"/>, <see cref="double"/> and <see cref="decimal"/> are emitted using the invariant culture.</description></item>
        /// <item><description><c>byte[]</c> becomes a hex literal of the form <c>X'..'</c>.</description></item>
        /// <item><description><see cref="DateTime"/> and <see cref="DateTimeOffset"/> become quoted ISO-8601 strings; <c>DateOnly</c> becomes a quoted date.</description></item>
        /// <item><description><see cref="IEnumerable"/> (other than <see cref="string"/> and <c>byte[]</c>) becomes a bracketed list literal, e.g. <c>[1, 2, 3]</c>.</description></item>
        /// </list>
        /// </param>
        /// <returns>The SQL literal representation of <paramref name="value"/>.</returns>
        /// <exception cref="NotSupportedException">
        /// Thrown if the value's type has no SQL literal conversion.
        /// </exception>
        public static string ValueToSql(object? value)
        {
            switch (value)
            {
                case null:
                    return "NULL";
                case string s:
                    return "'" + s.Replace("'", "''") + "'";
                case bool b:
                    return b ? "TRUE" : "FALSE";
                case byte[] bytes:
                    return BytesToSql(bytes);
                case sbyte or byte or short or ushort or int or uint or long or ulong:
                    return Convert.ToString(value, CultureInfo.InvariantCulture)!;
                case float f:
                    return f.ToString(CultureInfo.InvariantCulture);
                case double d:
                    return d.ToString(CultureInfo.InvariantCulture);
                case decimal m:
                    return m.ToString(CultureInfo.InvariantCulture);
                case DateTime dt:
                    return "'" + dt.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture) + "'";
                case DateTimeOffset dto:
                    return "'" + dto.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFFzzz", CultureInfo.InvariantCulture) + "'";
#if NET6_0_OR_GREATER
                case DateOnly date:
                    return "'" + date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'";
#endif
                case IEnumerable enumerable:
                    return ListToSql(enumerable);
                default:
                    throw new NotSupportedException(
                        $"SQL conversion is not implemented for type {value.GetType()}.");
            }
        }

        private static string BytesToSql(byte[] bytes)
        {
            var sb = new StringBuilder(2 + (bytes.Length * 2));
            sb.Append("X'");
            foreach (var b in bytes)
            {
                sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
            }
            sb.Append('\'');
            return sb.ToString();
        }

        private static string ListToSql(IEnumerable enumerable)
        {
            var sb = new StringBuilder();
            sb.Append('[');
            bool first = true;
            foreach (var item in enumerable)
            {
                if (!first)
                {
                    sb.Append(", ");
                }
                sb.Append(ValueToSql(item));
                first = false;
            }
            sb.Append(']');
            return sb.ToString();
        }
    }
}
