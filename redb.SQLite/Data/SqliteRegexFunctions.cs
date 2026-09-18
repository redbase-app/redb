using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;

namespace redb.SQLite.Data
{
    /// <summary>
    /// Regular expressions for queries, on one connection. SQLite has none of its own - <c>X REGEXP Y</c> calls a user
    /// function that nobody registers - so the query builder translates a LINQ <c>Regex.IsMatch</c> or
    /// <c>Regex.Replace</c> to these functions, and the expression runs with the .NET semantics it has in memory.
    ///
    /// <para>
    /// NULL in, NULL out, like the built-ins: a NULL match is neither true nor false, so a row with a NULL value is not
    /// selected by the predicate or by its negation. The match timeout is the process default of <see cref="Regex"/>
    /// (<c>REGEX_DEFAULT_MATCH_TIMEOUT</c>), the same an in-memory match uses.
    /// </para>
    /// </summary>
    internal static class SqliteRegexFunctions
    {
        internal static void Install(SqliteConnection connection)
        {
            connection.CreateFunction<string?, string?, long?>(
                "redb_regexp",
                (input, pattern) => input is null || pattern is null
                    ? (long?)null
                    : Regex.IsMatch(input, pattern) ? 1L : 0L,
                isDeterministic: true);

            connection.CreateFunction<string?, string?, long?>(
                "redb_regexp_i",
                (input, pattern) => input is null || pattern is null
                    ? (long?)null
                    : Regex.IsMatch(input, pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant) ? 1L : 0L,
                isDeterministic: true);

            connection.CreateFunction<string?, string?, string?, string?>(
                "redb_regexp_replace",
                (input, pattern, replacement) => input is null || pattern is null || replacement is null
                    ? null
                    : Regex.Replace(input, pattern, replacement),
                isDeterministic: true);
        }
    }
}
