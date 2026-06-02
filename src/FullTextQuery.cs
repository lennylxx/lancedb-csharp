namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text.Json.Nodes;

    /// <summary>
    /// The operator to use for combining terms in a full-text query.
    /// </summary>
    public enum FullTextOperator
    {
        /// <summary>All terms in the query must match.</summary>
        And,

        /// <summary>At least one term in the query must match.</summary>
        Or,
    }

    /// <summary>
    /// Specifies how a clause in a <see cref="BooleanQuery"/> affects matching.
    /// </summary>
    public enum Occur
    {
        /// <summary>The clause should match; matches increase relevance but are not required.</summary>
        Should,

        /// <summary>The clause must match for a document to be returned.</summary>
        Must,

        /// <summary>The clause must not match; matching documents are excluded.</summary>
        MustNot,
    }

    /// <summary>
    /// Base class for structured full-text search queries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="FullTextQuery"/> describes a full-text search beyond a simple
    /// string. Concrete query types include <see cref="MatchQuery"/>,
    /// <see cref="PhraseQuery"/>, <see cref="BoostQuery"/>,
    /// <see cref="MultiMatchQuery"/>, and <see cref="BooleanQuery"/>.
    /// </para>
    /// <para>
    /// Pass an instance to <see cref="Query.NearestToText(FullTextQuery)"/> (or
    /// <see cref="VectorQuery.NearestToText(FullTextQuery)"/> for hybrid search)
    /// to run the query.
    /// </para>
    /// </remarks>
    public abstract class FullTextQuery
    {
        /// <summary>
        /// Builds the JSON representation of this query node, matching the
        /// serialization format expected by the native LanceDB engine.
        /// </summary>
        internal abstract JsonNode ToJsonNode();

        /// <summary>
        /// The plain-text representation of this query, mirroring the Rust
        /// <c>FtsQuery::query()</c> accessor exposed by the Python SDK's
        /// <c>get_query()</c>. It is surfaced through <see cref="FTSQuery.QueryString"/>
        /// and used as the query string passed to rerankers. It does not affect
        /// query execution.
        /// </summary>
        internal abstract string QueryText { get; }

        /// <summary>
        /// Convert the query to a JSON string.
        /// </summary>
        /// <returns>A JSON string representation of the query.</returns>
        public string ToJson()
        {
            return ToJsonNode().ToJsonString();
        }

        /// <summary>
        /// Combine this query with another using a logical AND operation.
        /// </summary>
        /// <param name="other">The other query to combine with.</param>
        /// <returns>A new query that requires both queries to match.</returns>
        public BooleanQuery And(FullTextQuery other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }
            return new BooleanQuery(new[]
            {
                (Occur.Must, this),
                (Occur.Must, other),
            });
        }

        /// <summary>
        /// Combine this query with another using a logical OR operation.
        /// </summary>
        /// <param name="other">The other query to combine with.</param>
        /// <returns>A new query that matches if either query matches.</returns>
        public BooleanQuery Or(FullTextQuery other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }
            return new BooleanQuery(new[]
            {
                (Occur.Should, this),
                (Occur.Should, other),
            });
        }

        /// <summary>
        /// Combine two queries with a logical AND operation. Equivalent to
        /// <see cref="And"/>.
        /// </summary>
        /// <param name="left">The left query.</param>
        /// <param name="right">The right query.</param>
        /// <returns>A new query that requires both queries to match.</returns>
        public static BooleanQuery operator &(FullTextQuery left, FullTextQuery right)
        {
            if (left == null)
            {
                throw new ArgumentNullException(nameof(left));
            }
            return left.And(right);
        }

        /// <summary>
        /// Combine two queries with a logical OR operation. Equivalent to
        /// <see cref="Or"/>.
        /// </summary>
        /// <param name="left">The left query.</param>
        /// <param name="right">The right query.</param>
        /// <returns>A new query that matches if either query matches.</returns>
        public static BooleanQuery operator |(FullTextQuery left, FullTextQuery right)
        {
            if (left == null)
            {
                throw new ArgumentNullException(nameof(left));
            }
            return left.Or(right);
        }

        private protected static string OperatorToJson(FullTextOperator op)
        {
            return op == FullTextOperator.And ? "And" : "Or";
        }

        private protected static void EnsureFinite(double value, string paramName)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                throw new ArgumentException("Value must be a finite number.", paramName);
            }
        }
    }

    /// <summary>
    /// Match query for full-text search.
    /// </summary>
    public sealed class MatchQuery : FullTextQuery
    {
        /// <summary>
        /// Creates a match query.
        /// </summary>
        /// <param name="query">The query string to match against.</param>
        /// <param name="column">The name of the column to match against.</param>
        /// <param name="boost">
        /// The boost factor for the query. The score of each matching document is
        /// multiplied by this value. Defaults to <c>1.0</c>.
        /// </param>
        /// <param name="fuzziness">
        /// The maximum edit distance for each term in the match query. Defaults to
        /// <c>0</c> (exact match). If <c>null</c>, fuzziness is applied automatically
        /// by the rules:
        /// <list type="bullet">
        /// <item><description>0 for terms with length &lt;= 2</description></item>
        /// <item><description>1 for terms with length &lt;= 5</description></item>
        /// <item><description>2 for terms with length &gt; 5</description></item>
        /// </list>
        /// </param>
        /// <param name="maxExpansions">
        /// The maximum number of terms to consider for fuzzy matching. Defaults to <c>50</c>.
        /// </param>
        /// <param name="operator">
        /// The operator to use for combining the query results. Can be either
        /// <see cref="FullTextOperator.And"/> or <see cref="FullTextOperator.Or"/>.
        /// If <see cref="FullTextOperator.And"/>, all terms in the query must match.
        /// If <see cref="FullTextOperator.Or"/>, at least one term in the query must
        /// match. Defaults to <see cref="FullTextOperator.Or"/>.
        /// </param>
        /// <param name="prefixLength">
        /// The number of beginning characters being unchanged for fuzzy matching.
        /// This is useful to achieve prefix matching. Defaults to <c>0</c>.
        /// </param>
        public MatchQuery(
            string query,
            string column,
            float boost = 1.0f,
            int? fuzziness = 0,
            int maxExpansions = 50,
            FullTextOperator @operator = FullTextOperator.Or,
            int prefixLength = 0)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Column = column ?? throw new ArgumentNullException(nameof(column));
            EnsureFinite(boost, nameof(boost));
            if (fuzziness.HasValue && fuzziness.Value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fuzziness), "fuzziness must be non-negative.");
            }
            if (maxExpansions < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxExpansions), "maxExpansions must be non-negative.");
            }
            if (prefixLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(prefixLength), "prefixLength must be non-negative.");
            }
            Boost = boost;
            Fuzziness = fuzziness;
            MaxExpansions = maxExpansions;
            Operator = @operator;
            PrefixLength = prefixLength;
        }

        /// <summary>The query string to match against.</summary>
        public string Query { get; }

        /// <summary>The name of the column to match against.</summary>
        public string Column { get; }

        /// <summary>The boost factor applied to matching documents.</summary>
        public float Boost { get; }

        /// <summary>The maximum edit distance per term, or <c>null</c> for automatic fuzziness.</summary>
        public int? Fuzziness { get; }

        /// <summary>The maximum number of terms to consider for fuzzy matching.</summary>
        public int MaxExpansions { get; }

        /// <summary>The operator used for combining the query results.</summary>
        public FullTextOperator Operator { get; }

        /// <summary>The number of beginning characters kept unchanged for fuzzy matching.</summary>
        public int PrefixLength { get; }

        internal override string QueryText => Query;

        internal override JsonNode ToJsonNode()
        {
            var inner = new JsonObject
            {
                ["column"] = Column,
                ["terms"] = Query,
                ["boost"] = Boost,
                ["fuzziness"] = Fuzziness.HasValue ? JsonValue.Create(Fuzziness.Value) : null,
                ["max_expansions"] = MaxExpansions,
                ["operator"] = OperatorToJson(Operator),
                ["prefix_length"] = PrefixLength,
            };
            return new JsonObject { ["match"] = inner };
        }
    }

    /// <summary>
    /// Phrase query for full-text search.
    /// </summary>
    public sealed class PhraseQuery : FullTextQuery
    {
        /// <summary>
        /// Creates a phrase query.
        /// </summary>
        /// <param name="query">The query string to match against.</param>
        /// <param name="column">The name of the column to match against.</param>
        /// <param name="slop">
        /// The maximum number of intervening unmatched positions allowed between
        /// terms of the phrase. Defaults to <c>0</c> (exact phrase).
        /// </param>
        public PhraseQuery(string query, string column, int slop = 0)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            Column = column ?? throw new ArgumentNullException(nameof(column));
            if (slop < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(slop), "slop must be non-negative.");
            }
            Slop = slop;
        }

        /// <summary>The query string to match against.</summary>
        public string Query { get; }

        /// <summary>The name of the column to match against.</summary>
        public string Column { get; }

        /// <summary>The maximum number of intervening unmatched positions allowed between terms.</summary>
        public int Slop { get; }

        internal override string QueryText => "\"" + Query + "\"";

        internal override JsonNode ToJsonNode()
        {
            var inner = new JsonObject
            {
                ["column"] = Column,
                ["terms"] = Query,
                ["slop"] = Slop,
            };
            return new JsonObject { ["phrase"] = inner };
        }
    }

    /// <summary>
    /// Boost query for full-text search.
    /// </summary>
    /// <remarks>
    /// Documents matching the positive query are boosted, while documents matching
    /// the negative query have their score reduced by <c>negative_boost</c>.
    /// </remarks>
    public sealed class BoostQuery : FullTextQuery
    {
        /// <summary>
        /// Creates a boost query.
        /// </summary>
        /// <param name="positive">The positive query.</param>
        /// <param name="negative">The negative query.</param>
        /// <param name="negativeBoost">The boost factor for the negative query. Defaults to <c>0.5</c>.</param>
        public BoostQuery(FullTextQuery positive, FullTextQuery negative, float negativeBoost = 0.5f)
        {
            Positive = positive ?? throw new ArgumentNullException(nameof(positive));
            Negative = negative ?? throw new ArgumentNullException(nameof(negative));
            EnsureFinite(negativeBoost, nameof(negativeBoost));
            NegativeBoost = negativeBoost;
        }

        /// <summary>The positive query.</summary>
        public FullTextQuery Positive { get; }

        /// <summary>The negative query.</summary>
        public FullTextQuery Negative { get; }

        /// <summary>The boost factor for the negative query.</summary>
        public float NegativeBoost { get; }

        internal override string QueryText => Positive.QueryText;

        internal override JsonNode ToJsonNode()
        {
            var inner = new JsonObject
            {
                ["positive"] = Positive.ToJsonNode(),
                ["negative"] = Negative.ToJsonNode(),
                ["negative_boost"] = NegativeBoost,
            };
            return new JsonObject { ["boost"] = inner };
        }
    }

    /// <summary>
    /// Multi-match query for full-text search.
    /// </summary>
    /// <remarks>
    /// Runs the same query string against multiple columns, optionally with a
    /// per-column boost factor and a combining operator.
    /// </remarks>
    public sealed class MultiMatchQuery : FullTextQuery
    {
        /// <summary>
        /// Creates a multi-match query.
        /// </summary>
        /// <param name="query">The query string to match against.</param>
        /// <param name="columns">The list of columns to match against.</param>
        /// <param name="boosts">
        /// The list of boost factors for each column. If not provided, all columns
        /// will have the same boost factor. When provided, its length must equal the
        /// number of columns.
        /// </param>
        /// <param name="operator">
        /// The operator to use for combining the query results. Can be either
        /// <see cref="FullTextOperator.And"/> or <see cref="FullTextOperator.Or"/>.
        /// If <see cref="FullTextOperator.And"/>, all terms in the query must match.
        /// If <see cref="FullTextOperator.Or"/>, at least one term in the query must
        /// match. Defaults to <see cref="FullTextOperator.Or"/>.
        /// </param>
        public MultiMatchQuery(
            string query,
            IEnumerable<string> columns,
            IEnumerable<float>? boosts = null,
            FullTextOperator @operator = FullTextOperator.Or)
        {
            Query = query ?? throw new ArgumentNullException(nameof(query));
            if (columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }
            Columns = new List<string>(columns);
            if (Columns.Count == 0)
            {
                throw new ArgumentException("At least one column is required.", nameof(columns));
            }
            if (boosts != null)
            {
                Boosts = new List<float>(boosts);
                if (Boosts.Count != Columns.Count)
                {
                    throw new ArgumentException(
                        "The number of boosts must match the number of columns.", nameof(boosts));
                }
                foreach (var b in Boosts)
                {
                    EnsureFinite(b, nameof(boosts));
                }
            }
            Operator = @operator;
        }

        /// <summary>The query string to match against.</summary>
        public string Query { get; }

        /// <summary>The list of columns to match against.</summary>
        public IReadOnlyList<string> Columns { get; }

        /// <summary>The list of per-column boost factors, or <c>null</c> if not specified.</summary>
        public IReadOnlyList<float>? Boosts { get; }

        /// <summary>The operator used for combining the query results.</summary>
        public FullTextOperator Operator { get; }

        internal override string QueryText => Query;

        internal override JsonNode ToJsonNode()
        {
            var columns = new JsonArray();
            foreach (var c in Columns)
            {
                columns.Add(c);
            }
            var inner = new JsonObject
            {
                ["query"] = Query,
                ["columns"] = columns,
                ["operator"] = OperatorToJson(Operator),
            };
            if (Boosts != null)
            {
                var boosts = new JsonArray();
                foreach (var b in Boosts)
                {
                    boosts.Add(b);
                }
                inner["boost"] = boosts;
            }
            return new JsonObject { ["multi_match"] = inner };
        }
    }

    /// <summary>
    /// Boolean query for full-text search.
    /// </summary>
    /// <remarks>
    /// Combines multiple sub-queries, each with an <see cref="Occur"/> requirement
    /// (<see cref="Occur.Must"/>, <see cref="Occur.Should"/>, or
    /// <see cref="Occur.MustNot"/>).
    /// </remarks>
    public sealed class BooleanQuery : FullTextQuery
    {
        private readonly List<(Occur Occur, FullTextQuery Query)> _queries;

        /// <summary>
        /// Creates a boolean query.
        /// </summary>
        /// <param name="queries">
        /// The list of sub-queries with their occurrence requirements. Each entry
        /// pairs an <see cref="Occur"/> value (<see cref="Occur.Must"/>,
        /// <see cref="Occur.Should"/>, or <see cref="Occur.MustNot"/>) with a
        /// <see cref="FullTextQuery"/> to apply.
        /// </param>
        public BooleanQuery(IEnumerable<(Occur Occur, FullTextQuery Query)> queries)
        {
            if (queries == null)
            {
                throw new ArgumentNullException(nameof(queries));
            }
            _queries = new List<(Occur, FullTextQuery)>(queries);
            foreach (var (_, q) in _queries)
            {
                if (q == null)
                {
                    throw new ArgumentException("Sub-queries must not be null.", nameof(queries));
                }
            }
        }

        /// <summary>The list of sub-queries with their occurrence requirements.</summary>
        public IReadOnlyList<(Occur Occur, FullTextQuery Query)> Queries => _queries;

        internal override string QueryText => string.Empty;

        internal override JsonNode ToJsonNode()
        {
            var should = new JsonArray();
            var must = new JsonArray();
            var mustNot = new JsonArray();
            foreach (var (occur, query) in _queries)
            {
                switch (occur)
                {
                    case Occur.Should:
                        should.Add(query.ToJsonNode());
                        break;
                    case Occur.Must:
                        must.Add(query.ToJsonNode());
                        break;
                    case Occur.MustNot:
                        mustNot.Add(query.ToJsonNode());
                        break;
                }
            }
            var inner = new JsonObject
            {
                ["should"] = should,
                ["must"] = must,
                ["must_not"] = mustNot,
            };
            return new JsonObject { ["boolean"] = inner };
        }
    }
}
