using redb.Core;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// SQLite is the one provider where the fix replaces a built-in function rather than adding a clause
/// to the SQL, so the replacement has to reproduce SQLite's own LIKE semantics exactly. Folding the
/// case is the easy half; the wildcards, the ESCAPE form and NULL propagation are where a
/// hand-written matcher silently diverges.
///
/// <para>
/// These run raw SQL on purpose. Going through LINQ would test the query builder as well and would
/// not reach the ESCAPE form at all, which is emitted only by the user-search path.
/// </para>
/// </summary>
[Collection("SqliteCollation")]
public class SqliteLikeSemanticsTests
{
    private readonly IRedbService _redb;

    public SqliteLikeSemanticsTests(SqliteCollationFixture fixture) => _redb = fixture.Redb;

    private Task<long?> ScalarAsync(string sql) => _redb.Context.ExecuteScalarAsync<long?>(sql);

    // ---------------------------------------------------------------- folding

    [Theory]
    [InlineData("'ПРИВЕТ' LIKE '%привет%'")]
    [InlineData("'привет' LIKE '%ПРИВЕТ%'")]
    [InlineData("'ΑΘΗΝΑ' LIKE '%αθηνα%'")]
    [InlineData("'ŐSZ' LIKE '%ősz%'")]
    [InlineData("'HELLO' LIKE '%hello%'")]
    public async Task Folds_EveryScript(string expression)
        => (await ScalarAsync($"SELECT ({expression})")).Should().Be(1);

    [Fact]
    public async Task DoesNotMatch_UnrelatedText()
        => (await ScalarAsync("SELECT ('ПРОЧЕЕ' LIKE '%привет%')")).Should().Be(0);

    // ---------------------------------------------------------------- wildcards

    [Theory]
    [InlineData("'abc' LIKE 'abc'", 1)]
    [InlineData("'abc' LIKE 'a%'", 1)]
    [InlineData("'abc' LIKE '%c'", 1)]
    [InlineData("'abc' LIKE '%b%'", 1)]
    [InlineData("'abc' LIKE 'a_c'", 1)]
    [InlineData("'abc' LIKE 'a_'", 0)]      // _ is exactly one character
    [InlineData("'abc' LIKE '%'", 1)]
    [InlineData("'' LIKE '%'", 1)]          // % matches the empty string
    [InlineData("'abc' LIKE ''", 0)]
    [InlineData("'abc' LIKE 'abcd'", 0)]
    [InlineData("'aXbXc' LIKE 'a%b%c'", 1)] // backtracking across two wildcards
    [InlineData("'aaa' LIKE '%a'", 1)]
    [InlineData("'abc' LIKE '%%%'", 1)]     // consecutive wildcards collapse
    public async Task Wildcards_MatchSqliteSemantics(string expression, long expected)
        => (await ScalarAsync($"SELECT ({expression})")).Should().Be(expected);

    // ---------------------------------------------------------------- ESCAPE

    /// <summary>
    /// The three-argument form. REDB emits it on the user-search path, where a literal underscore in
    /// an email address must not become a wildcard. Registering only the two-argument override would
    /// leave this on the ASCII-only built-in, silently.
    /// </summary>
    [Theory]
    [InlineData(@"'a_c' LIKE 'a\_c' ESCAPE '\'", 1)]   // escaped _ is literal and matches
    [InlineData(@"'aXc' LIKE 'a\_c' ESCAPE '\'", 0)]   // ... and no longer matches any character
    [InlineData(@"'a%c' LIKE 'a\%c' ESCAPE '\'", 1)]
    [InlineData(@"'aXc' LIKE 'a\%c' ESCAPE '\'", 0)]
    [InlineData(@"'ПРИВЕТ_МИР' LIKE '%\_мир' ESCAPE '\'", 1)]  // escape and folding together
    public async Task Escape_MakesWildcardsLiteral(string expression, long expected)
        => (await ScalarAsync($"SELECT ({expression})")).Should().Be(expected);

    // ---------------------------------------------------------------- code points

    /// <summary>
    /// In SQLite a character is a CODE POINT, not a UTF-16 code unit, so <c>_</c> matches an emoji
    /// as one character. The first version of the override counted units and returned 0 here while
    /// the built-in returned 1 — found by running the same expression against a connection without
    /// the override, which is the only way to catch a divergence like this.
    /// </summary>
    [Theory]
    [InlineData("'a😀b' LIKE 'a_b'", 1)]
    [InlineData("'a😀b' LIKE 'a%b'", 1)]
    [InlineData("'a😀b' LIKE 'a__b'", 0)]     // one code point, not two
    [InlineData("'😀' LIKE '_'", 1)]
    [InlineData("'ПРИВЕТ😀' LIKE '%привет_'", 1)]   // folding and code points together
    public async Task Wildcards_CountCodePointsNotUtf16Units(string expression, long expected)
        => (await ScalarAsync($"SELECT ({expression})")).Should().Be(expected);

    /// <summary>
    /// A pattern ending with the escape character matches nothing, which is what the built-in does.
    /// The override originally treated the dangling escape as a literal and returned 1.
    /// </summary>
    [Fact]
    public async Task Escape_TrailingEscapeCharacterMatchesNothing()
        => (await ScalarAsync(@"SELECT ('a\' LIKE 'a\' ESCAPE '\')")).Should().Be(0);

    /// <summary>NOT LIKE runs through the same overridden function and must stay consistent with it.</summary>
    [Theory]
    [InlineData("'ПРИВЕТ' NOT LIKE '%привет%'", 0)]
    [InlineData("'ПРОЧЕЕ' NOT LIKE '%привет%'", 1)]
    public async Task NotLike_IsConsistentWithLike(string expression, long expected)
        => (await ScalarAsync($"SELECT ({expression})")).Should().Be(expected);

    // ---------------------------------------------------------------- NULL

    /// <summary>SQLite's like() yields NULL when either side is NULL; the override must not turn that into 0.</summary>
    [Theory]
    [InlineData("SELECT (NULL LIKE '%a%') IS NULL")]
    [InlineData("SELECT ('abc' LIKE NULL) IS NULL")]
    public async Task Null_PropagatesRatherThanBecomingFalse(string sql)
        => (await ScalarAsync(sql)).Should().Be(1);

    // ---------------------------------------------------------------- lower / upper

    [Theory]
    [InlineData("lower('ПРИВЕТ')", "привет")]
    [InlineData("upper('привет')", "ПРИВЕТ")]
    [InlineData("lower('ŐSZ')", "ősz")]
    [InlineData("lower('HELLO')", "hello")]
    public async Task LowerUpper_FoldEveryScript(string expression, string expected)
    {
        var actual = await _redb.Context.ExecuteScalarAsync<string>($"SELECT {expression}");
        actual.Should().Be(expected);
    }

    /// <summary>
    /// KNOWN BOUNDARY. SQLite maps the case-sensitive and case-insensitive operators onto the same
    /// LIKE — the native extension says so in as many words ("both map to LIKE"), and it has always
    /// been that way because SQLite's LIKE is case-insensitive for ASCII regardless. Enabling the
    /// feature therefore widens that collapse to every script: a case-SENSITIVE Contains starts
    /// matching a differently-cased Cyrillic string too. Opt-in, and documented, but real.
    /// </summary>
    [Fact]
    public async Task KnownBoundary_CaseSensitiveOperatorsAreAlsoFolded()
        => (await ScalarAsync("SELECT ('ПРИВЕТ' LIKE '%привет%')")).Should().Be(1);
}
