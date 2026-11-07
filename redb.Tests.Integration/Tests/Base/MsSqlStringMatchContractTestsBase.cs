using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// String matching on SQL Server against the contract in COLLATION.md (BR-6): plain Contains / StartsWith /
/// EndsWith follow the column collation, and the *IgnoreCase forms fold case - not diacritics. SQL Server Pro broke
/// both (review MP-10): the plain forms were forced case-sensitive (COLLATE Latin1_General_CS_AS) and the
/// IgnoreCase forms accent-insensitive (COLLATE Latin1_General_CI_AI), so Free and Pro answered the same query
/// differently on the same database. The facts assume the default case-insensitive, accent-sensitive collation of
/// the test database and check it first.
/// </summary>
public abstract class MsSqlStringMatchContractTestsBase
{
    protected readonly IRedbService Redb;

    protected MsSqlStringMatchContractTestsBase(IRedbService redb) => Redb = redb;

    private async Task<string> SeedAsync()
    {
        var collation = await Redb.Context.ExecuteScalarAsync<string>(
            "SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))");
        collation.Should().Contain("_CI_AS", "precondition: the test database folds case and keeps accents");

        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        await Redb.SaveAsync(new RedbObject<SimpleProps> { name = $"zebra-{tag}", Props = new SimpleProps { Title = $"ZEBRA-{tag}" } });
        await Redb.SaveAsync(new RedbObject<SimpleProps> { name = $"muller-{tag}", Props = new SimpleProps { Title = $"Müller-{tag}" } });
        return tag;
    }

    [Fact]
    public async Task PlainContains_FollowsTheColumnCollation()
    {
        var tag = await SeedAsync();

        var found = await Redb.Query<SimpleProps>().Where(x => x.Title.Contains($"zebra-{tag}")).ToListAsync();

        found.Should().ContainSingle("a case-insensitive column matches regardless of case");
    }

    [Fact]
    public async Task ContainsIgnoreCase_FoldsCaseButNotAccents()
    {
        var tag = await SeedAsync();

        var withUmlaut = await Redb.Query<SimpleProps>()
            .Where(x => x.Title.Contains($"MÜLLER-{tag}", StringComparison.OrdinalIgnoreCase)).ToListAsync();
        var withoutUmlaut = await Redb.Query<SimpleProps>()
            .Where(x => x.Title.Contains($"muller-{tag}", StringComparison.OrdinalIgnoreCase)).ToListAsync();

        withUmlaut.Should().ContainSingle("case is folded");
        withoutUmlaut.Should().BeEmpty("diacritics are not folded - that is a different feature (COLLATION.md)");
    }
}
