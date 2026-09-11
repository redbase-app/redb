using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// BR-7 (Tsak report, fixed 2026-09-02): the StartsWith/EndsWith/Contains sugar built LIKE
/// patterns with the operand spliced raw - '%'/'_' in the operand matched as wildcards (a
/// superset), '[' on MSSQL opened a character class (a WRONG SUBSET no post-filter can restore),
/// and a backslash on PostgreSQL acted as LIKE's own escape character. The contract now: the
/// operand of every sugar operator is a LITERAL, identically on all three providers - escaping
/// happens where the pattern is assembled (the SQL/native builders), so direct facet-JSON callers
/// are covered too. The raw <c>$like</c>/<c>$ilike</c>/<c>$arrayMatches</c> operators keep
/// interpreting the caller's pattern on purpose.
/// </summary>
public abstract class LikeEscapingTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected LikeEscapingTestsBase(IRedbService redb) => Redb = redb;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
        => await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'LikeProbe')");

    private async Task SeedAsync(params string[] values)
    {
        await Redb.SyncSchemeAsync<LikeProbeProps>();
        foreach (var v in values)
            await Redb.SaveAsync(new RedbObject<LikeProbeProps>
                { name = "probe", Props = new LikeProbeProps { S = v } });
    }

    [Fact]
    public async Task StartsWith_PercentInTheOperand_IsLiteral()
    {
        await SeedAsync("100%x", "1000x");
        var rs = await Redb.Query<LikeProbeProps>().Where(p => p.S!.StartsWith("100%")).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("100%x");
    }

    [Fact]
    public async Task Contains_Underscore_IsLiteral()
    {
        await SeedAsync("a_c", "abc");
        var rs = await Redb.Query<LikeProbeProps>().Where(p => p.S!.Contains("_")).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("a_c");
    }

    [Fact]
    public async Task EndsWith_Underscore_IsLiteral()
    {
        await SeedAsync("x_", "xy");
        var rs = await Redb.Query<LikeProbeProps>().Where(p => p.S!.EndsWith("_")).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("x_");
    }

    [Fact]
    public async Task StartsWith_OpeningBracket_IsLiteral()
    {
        // '[' opens a character class in T-SQL LIKE: StartsWith("x[1]") used to return 'x1y' and
        // not 'x[1]y' on MSSQL - a wrong subset. PostgreSQL and SQLite treat '[' literally anyway.
        await SeedAsync("x[1]y", "x1y");
        var rs = await Redb.Query<LikeProbeProps>().Where(p => p.S!.StartsWith("x[1]")).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("x[1]y");
    }

    [Fact]
    public async Task Contains_Backslash_IsLiteral()
    {
        // On PostgreSQL a backslash in the operand used to act as LIKE's own escape character:
        // Contains("\") built '%\%' - "everything containing a PERCENT".
        await SeedAsync("a\\b", "a%b");
        var rs = await Redb.Query<LikeProbeProps>().Where(p => p.S!.Contains("\\")).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("a\\b");
    }

    [Fact]
    public async Task StartsWithIgnoreCase_Metacharacters_AreLiteral()
    {
        await SeedAsync("100%X", "1000X");
        var rs = await Redb.Query<LikeProbeProps>()
            .Where(p => p.S!.StartsWith("100%x", StringComparison.OrdinalIgnoreCase)).ToListAsync();
        rs.Select(r => r.Props.S).Should().Equal("100%X");
    }
}
