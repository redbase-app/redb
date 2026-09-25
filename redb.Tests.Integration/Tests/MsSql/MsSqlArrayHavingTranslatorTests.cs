using redb.Core;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// The HAVING translator of the SQL Server Free array grouping (<c>dbo.pvt_build_array_having_expr</c>) turned a
/// node it did not know into <c>1=1</c>: the condition was dropped and every group came back. The core's HAVING
/// parser writes no such node today, so the function is called directly - the SQL is the public surface for
/// callers that submit the JSON themselves.
/// </summary>
[Collection("MsSql")]
public class MsSqlArrayHavingTranslatorTests
{
    private readonly IRedbService _redb;

    public MsSqlArrayHavingTranslatorTests(MsSqlFixture fixture) => _redb = fixture.Redb;

    private async Task<string?> BuildAsync(string havingJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_array_groupby_sql(" + scheme.Id + ", N'Contacts', NULL, "
            + "N'[{\"field\":\"Type\",\"alias\":\"Type\"}]', N'[{\"field\":\"*\",\"func\":\"COUNT\",\"alias\":\"Count\"}]', $1, N'flat')",
            new object[] { havingJson });
    }

    [Fact]
    public async Task AHavingNodeItDoesNotKnow_RefusesTheQuery()
    {
        var sql = await BuildAsync("{\"$regex\":[{\"$count\":\"*\"},{\"$const\":\"x\"}]}");

        sql.Should().BeNull("a HAVING the translator cannot read must not be replaced by 1=1");
    }

    [Fact]
    public async Task AHavingItKnows_IsEmitted()
    {
        var sql = await BuildAsync("{\"$gt\":[{\"$count\":\"*\"},{\"$const\":1}]}");

        sql.Should().Contain("HAVING (COUNT(*) > 1)");
    }
}
