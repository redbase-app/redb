using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// The other half of the contract: with <c>StringCollation</c> unset, behaviour on the very same
/// C-ctype database is exactly what it was before the feature existed.
///
/// <para>
/// This is the test that makes the feature safe to ship. It runs against a database where the
/// defect is real, and asserts that REDB still reproduces it when nobody asked for a fix. Without
/// it, "the default is unchanged" would be a claim rather than a fact, and an existing deployment
/// could silently start returning different rows after an upgrade.
/// </para>
///
/// <para>
/// It is also the mirror of <see cref="Tests.Base.CaseFoldingTestsBase"/>: the same rows, the same
/// query, the opposite expectation. Should someone later make the folding unconditional, both
/// suites cannot stay green, and this one is the one that will say so.
/// </para>
/// </summary>
[Collection("PostgresNoCollation")]
public class PostgresNoCollationTests
{
    private readonly IRedbService _redb;

    public PostgresNoCollationTests(PostgresNoCollationFixture fixture) => _redb = fixture.Redb;

    private async Task SeedAsync()
    {
        await _redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = "
            + $"'{typeof(CollationProps).FullName}')");

        await _redb.SaveAsync(new List<RedbObject<CollationProps>>
        {
            new() { name = "cyrillic", Props = new CollationProps { Text = "ПРИВЕТ МИР", Label = "cyrillic" } },
            new() { name = "ascii",    Props = new CollationProps { Text = "HELLO WORLD", Label = "ascii" } },
        });
    }

    private async Task<List<string>> FindAsync(string needle)
    {
        var results = await _redb.Query<CollationProps>()
            .Where(c => c.Text.Contains(needle, StringComparison.OrdinalIgnoreCase))
            .Take(50).ToListAsync();

        return results.Select(r => r.Props.Label).ToList();
    }

    /// <summary>
    /// The defect, reproduced deliberately. On LC_CTYPE=C the database folds ASCII and nothing else,
    /// so a lower-case Cyrillic needle finds nothing. This is not a wish, it is the documented
    /// behaviour of an unconfigured PostgreSQL, and REDB must not change it behind anyone's back.
    /// </summary>
    [Fact]
    public async Task WithoutSetting_CyrillicSearchStillFindsNothing()
    {
        await SeedAsync();
        (await FindAsync("привет")).Should().BeEmpty();
    }

    /// <summary>ASCII keeps working, which is what made the defect easy to miss.</summary>
    [Fact]
    public async Task WithoutSetting_AsciiSearchStillWorks()
    {
        await SeedAsync();
        (await FindAsync("hello")).Should().Contain("ascii");
    }

    /// <summary>An exactly-cased Cyrillic needle matches: the text itself is intact, only folding is absent.</summary>
    [Fact]
    public async Task WithoutSetting_ExactCaseCyrillicStillMatches()
    {
        await SeedAsync();
        (await FindAsync("ПРИВЕТ")).Should().Contain("cyrillic");
    }
}
