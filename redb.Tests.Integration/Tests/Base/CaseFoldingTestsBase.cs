using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Case-insensitive text search across scripts, with the feature ON.
///
/// <para>
/// Case folding is driven by the database's own rules and those cover only ASCII in two of the three
/// providers. The point of this suite is not Cyrillic: it is that a single setting fixes every script
/// whose case mapping is one character to one character, and that three specific classes are NOT
/// fixed by it and never will be. Both halves are asserted, because a suite that only proves the
/// happy path lets an over-promise ship in the documentation.
/// </para>
///
/// <para>
/// On PostgreSQL these tests are meaningful only against a database created with
/// <c>LC_CTYPE=C</c> (see <c>PostgresCollationFixture</c>). On the ordinary test database every one
/// of them passes with the feature removed, which is exactly how the defect went unnoticed.
/// </para>
/// </summary>
public abstract class CaseFoldingTestsBase
{
    protected readonly IRedbService Redb;
    private bool _seeded;

    protected CaseFoldingTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>
    /// One row per script, upper-cased at the source so a lower-case needle has to fold to find it.
    /// </summary>
    private static readonly (string Label, string Text)[] Rows =
    {
        ("cyrillic",  "ПРИВЕТ МИР"),
        ("greek",     "ΑΘΗΝΑ ΠΟΛΗ"),
        ("hungarian", "ŐSZ ÚTON JÁRTAM"),
        ("polish",    "ŻÓŁW ŁÓDŹ"),
        ("czech",     "ŘEKA ČESKÁ"),
        ("french",    "ÉTÉ À PARIS"),
        ("german",    "STRAßE GROß"),
        ("turkish",   "İSTANBUL"),
        ("umlaut",    "MÜLLER"),
        ("ascii",     "HELLO WORLD"),
    };

    protected async Task EnsureSeededAsync()
    {
        if (_seeded) return;

        // Scheme name is the CLR FullName: the positional [RedbScheme("...")] argument is the alias.
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = "
            + $"'{typeof(CollationProps).FullName}')");

        var objects = Rows.Select(r => new RedbObject<CollationProps>
        {
            name = r.Label,
            Props = new CollationProps { Text = r.Text, Label = r.Label }
        }).ToList();

        await Redb.SaveAsync(objects);
        _seeded = true;
    }

    private async Task<List<string>> FindAsync(string needle)
    {
        var results = await Redb.Query<CollationProps>()
            .Where(c => c.Text.Contains(needle, StringComparison.OrdinalIgnoreCase))
            .Take(50).ToListAsync();

        return results.Select(r => r.Props.Label).OrderBy(x => x).ToList();
    }

    // =====================================================================
    // Fixed by the setting: every one-to-one case mapping
    // =====================================================================

    /// <summary>
    /// The whole point in one test: this is not about Cyrillic. Each of these scripts maps case one
    /// character to one character, and one setting covers all of them with no per-language work.
    /// </summary>
    [Theory]
    [InlineData("привет", "cyrillic")]
    [InlineData("αθηνα", "greek")]
    [InlineData("ősz úton", "hungarian")]
    [InlineData("żółw", "polish")]
    [InlineData("řeka česká", "czech")]
    [InlineData("été à paris", "french")]
    [InlineData("hello", "ascii")]
    public async Task ContainsIgnoreCase_FoldsEveryOneToOneScript(string needle, string expectedLabel)
    {
        await EnsureSeededAsync();
        (await FindAsync(needle)).Should().Contain(expectedLabel);
    }

    /// <summary>The reverse direction: an upper-case needle against the same rows.</summary>
    [Theory]
    [InlineData("ПРИВЕТ", "cyrillic")]
    [InlineData("ŻÓŁW", "polish")]
    [InlineData("ÉTÉ", "french")]
    public async Task ContainsIgnoreCase_FoldsInBothDirections(string needle, string expectedLabel)
    {
        await EnsureSeededAsync();
        (await FindAsync(needle)).Should().Contain(expectedLabel);
    }

    [Fact]
    public async Task StartsWithIgnoreCase_Folds()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<CollationProps>()
            .Where(c => c.Text.StartsWith("привет", StringComparison.OrdinalIgnoreCase))
            .Take(50).ToListAsync();

        results.Select(r => r.Props.Label).Should().Contain("cyrillic");
    }

    [Fact]
    public async Task EndsWithIgnoreCase_Folds()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<CollationProps>()
            .Where(c => c.Text.EndsWith("мир", StringComparison.OrdinalIgnoreCase))
            .Take(50).ToListAsync();

        results.Select(r => r.Props.Label).Should().Contain("cyrillic");
    }

    /// <summary>
    /// ToLower is the other half of "one solution covers everything". Fixing the search while
    /// leaving ToLower ASCII-only would let a query find a row that a later comparison rejects.
    /// </summary>
    [Fact]
    public async Task ToLower_Folds()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<CollationProps>()
            .Where(c => c.Text.ToLower().Contains("привет"))
            .Take(50).ToListAsync();

        results.Select(r => r.Props.Label).Should().Contain("cyrillic");
    }

    [Fact]
    public async Task ToUpper_Folds()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<CollationProps>()
            .Where(c => c.Text.ToUpper().Contains("ПРИВЕТ"))
            .Take(50).ToListAsync();

        results.Select(r => r.Props.Label).Should().Contain("cyrillic");
    }

    /// <summary>A needle matching nothing must still match nothing. Folding is not widening.</summary>
    [Fact]
    public async Task ContainsIgnoreCase_DoesNotMatchUnrelatedRows()
    {
        await EnsureSeededAsync();
        (await FindAsync("привет")).Should().NotContain("greek").And.NotContain("ascii");
    }

    /// <summary>The same word with ß on both sides matches everywhere: that mapping is one to one.</summary>
    [Fact]
    public async Task GermanSharpS_MatchesItself()
    {
        await EnsureSeededAsync();
        (await FindAsync("straße")).Should().Contain("german");
    }

    /// <summary>With the diacritic present it matches, which is the case-folding half working.</summary>
    [Fact]
    public async Task Diacritics_MatchWhenPresent()
    {
        await EnsureSeededAsync();
        (await FindAsync("müller")).Should().Contain("umlaut");
    }

    // =====================================================================
    // Boundaries. Provider-specific on purpose — see the remark below.
    // =====================================================================

    /// <summary>
    /// German ß against SS, Turkish İ against i, and diacritics are the three things a collation
    /// cannot fold uniformly. What each provider actually does with them differs, and that surprised
    /// this suite: SQL Server's CI_AS folds ß to SS and İ to i, while the ICU root collation used on
    /// PostgreSQL folds neither. Asserting one answer for all three providers would have been wrong,
    /// so each states its own and the differences are the documentation.
    ///
    /// <para>
    /// Diacritics are the one boundary that holds everywhere: "muller" never finds "Müller". That is
    /// not case folding at all, and on PostgreSQL it is refused outright — a nondeterministic
    /// collation, which is what accent-insensitivity needs, cannot be used with ILIKE.
    /// </para>
    /// </summary>
    [Fact]
    public async Task KnownBoundary_Diacritics_AreNotFolded()
    {
        await EnsureSeededAsync();
        (await FindAsync("muller")).Should().NotContain("umlaut");
    }

    /// <summary>
    /// Does this provider fold ß to SS? Overridden per provider rather than assumed; see
    /// <see cref="KnownBoundary_Diacritics_AreNotFolded"/> for why the boundaries are not shared.
    /// </summary>
    protected abstract bool FoldsSharpSToDoubleS { get; }

    /// <summary>Does this provider fold the Turkish dotted capital İ to a plain i?</summary>
    protected abstract bool FoldsTurkishDottedI { get; }

    [Fact]
    public async Task Boundary_GermanSharpS_AgainstDoubleS()
    {
        await EnsureSeededAsync();
        var found = await FindAsync("strasse");

        if (FoldsSharpSToDoubleS)
            found.Should().Contain("german", "this provider's collation expands ß to SS");
        else
            found.Should().NotContain("german",
                "pattern matching folds character by character and cannot change a string's length, " +
                "even though UPPER on the same database does expand ß to SS");
    }

    [Fact]
    public async Task Boundary_TurkishDottedI()
    {
        await EnsureSeededAsync();
        var found = await FindAsync("istanbul");

        if (FoldsTurkishDottedI)
            found.Should().Contain("turkish", "this provider's collation folds İ to i");
        else
            found.Should().NotContain("turkish",
                "İ (U+0130) folds to i with a combining dot, not to plain i. Unfixable by one " +
                "setting anyway: Turkish wants I→ı where every other locale wants I→i");
    }
}
