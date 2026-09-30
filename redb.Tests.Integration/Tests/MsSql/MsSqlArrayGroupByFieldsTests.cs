using redb.Core;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// <c>dbo.pvt_build_array_groupby_sql</c> on SQL Server Free skipped a key or aggregate field the array item does
/// not have: a grouping on one known and one unknown key grouped on the known one, an unknown aggregate vanished
/// from the result, and when no key resolved the function returned the flat list of items instead of groups.
/// The core refuses such keys before they reach SQL (GRP-9), so the function is called directly - the SQL is the
/// public surface for callers that submit the JSON themselves.
/// </summary>
[Collection("MsSql")]
public class MsSqlArrayGroupByFieldsTests
{
    private readonly IRedbService _redb;

    public MsSqlArrayGroupByFieldsTests(MsSqlFixture fixture) => _redb = fixture.Redb;

    private async Task<string?> BuildAsync(string groupByJson, string aggregationsJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_array_groupby_sql(" + scheme.Id + ", N'Contacts', NULL, $1, $2, NULL, N'flat')",
            new object[] { groupByJson, aggregationsJson });
    }

    private const string CountAll = "[{\"field\":\"*\",\"func\":\"COUNT\",\"alias\":\"Count\"}]";

    [Fact]
    public async Task AKeyTheItemDoesNotHave_RefusesTheQuery()
    {
        var sql = await BuildAsync("[{\"field\":\"Type\",\"alias\":\"Type\"},{\"field\":\"NoSuchField\",\"alias\":\"X\"}]", CountAll);

        sql.Should().BeNull("grouping on the known key alone is a different grouping");
    }

    [Fact]
    public async Task NoKeyTheItemHas_RefusesTheQuery()
    {
        var sql = await BuildAsync("[{\"field\":\"NoSuchField\",\"alias\":\"X\"}]", CountAll);

        sql.Should().BeNull("a flat list of items is not a grouping");
    }

    [Fact]
    public async Task AnAggregateOnAFieldTheItemDoesNotHave_RefusesTheQuery()
    {
        var sql = await BuildAsync("[{\"field\":\"Type\",\"alias\":\"Type\"}]",
            "[{\"field\":\"NoSuchField\",\"func\":\"MAX\",\"alias\":\"M\"}]");

        sql.Should().BeNull("the aggregate would be missing from the result");
    }

    [Fact]
    public async Task KnownKeysAndAggregates_AreBuilt()
    {
        var sql = await BuildAsync("[{\"field\":\"Type\",\"alias\":\"Type\"}]",
            "[{\"field\":\"Value\",\"func\":\"MAX\",\"alias\":\"M\"}]");

        sql.Should().Contain("GROUP BY").And.Contain("AS [M]");
    }
}
