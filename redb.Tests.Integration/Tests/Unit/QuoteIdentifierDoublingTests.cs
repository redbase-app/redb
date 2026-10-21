using AwesomeAssertions;
using redb.MSSql.Sql;
using redb.Postgres.Sql;
using redb.SQLite.Sql;
using Xunit;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// A quoted identifier must escape its own closing quote by doubling it. Without that, a name that
/// carries a closing quote ends the identifier and the remainder becomes SQL (SQLI-PoC p.7). The
/// correct pattern already lived in <c>MssqlQueryProvider.QuoteGwndIdent</c>; the dialects did not
/// follow it.
/// </summary>
public class QuoteIdentifierDoublingTests
{
    [Fact]
    public void MsSql_DoublesClosingBracket()
        => new MsSqlDialect().QuoteIdentifier("a]b; SELECT 1--").Should().Be("[a]]b; SELECT 1--]");

    [Fact]
    public void Postgres_DoublesDoubleQuote()
        => new PostgreSqlDialect().QuoteIdentifier("a\"b").Should().Be("\"a\"\"b\"");

    [Fact]
    public void Sqlite_DoublesDoubleQuote()
        => new SqliteDialect().QuoteIdentifier("a\"b").Should().Be("\"a\"\"b\"");

    [Fact]
    public void PlainNameIsUnchanged()
    {
        new MsSqlDialect().QuoteIdentifier("Age").Should().Be("[Age]");
        new PostgreSqlDialect().QuoteIdentifier("Age").Should().Be("\"Age\"");
        new SqliteDialect().QuoteIdentifier("Age").Should().Be("\"Age\"");
    }
}
