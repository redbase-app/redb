using redb.Core;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// The SQL Server Free filter and aggregate builders (review SQ-5). Where PostgreSQL raises, SQL Server
/// substituted: an operator it did not know became <c>1=1</c> (the condition vanished) or <c>1=0</c> (no rows, and
/// every row under <c>$not</c>); an unknown comparison became <c>=</c>; a value that was not a number became 0; an
/// aggregate it could not compile became a NULL column. Each case now fails through <c>dbo.pvt_fail</c> with a
/// message that starts with "redb:". The core does not write most of these shapes, so the functions are called
/// directly - they are the public surface for callers that submit the JSON themselves.
/// </summary>
[Collection("MsSql")]
public class MsSqlFilterRefusalTests
{
    private readonly IRedbService _redb;

    public MsSqlFilterRefusalTests(MsSqlFixture fixture) => _redb = fixture.Redb;

    private async Task<string?> BuildQueryAsync(string filterJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_query_sql(" + scheme.Id + ", $1, NULL, 0, NULL, 10, 0, N'flat', NULL, 1, 1, NULL)",
            new object[] { filterJson });
    }

    private async Task<string?> BuildAggregateAsync(string aggregationsJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_aggregate_sql(" + scheme.Id + ", NULL, $1, N'flat')", new object[] { aggregationsJson });
    }

    [Theory]
    [InlineData("{\"$foo\":1}")]                                        // unknown top-level operator: was 1=1
    [InlineData("{\"Department\":{\"$foo\":\"x\"}}")]                   // unknown field operator: was 1=0
    [InlineData("{\"Department\":{\"$in\":\"x\"}}")]                    // $in without a list: was 1=0
    [InlineData("{\"0$:name\":{\"$arrayContains\":\"x\"}}")]            // array operator on a base field: was 1=0
    [InlineData("{\"$level\":\"abc\"}")]                                // level that is not a number: was 0
    [InlineData("{\"Department.$length\":{\"$foo\":3}}")]               // unknown comparison: was =
    [InlineData("{\"Department.$length\":{\"$eq\":\"abc\"}}")]          // length that is not a number: was 0
    public async Task AFilterItCannotRead_IsRefused(string filterJson)
    {
        var build = async () => await BuildQueryAsync(filterJson);

        await build.Should().ThrowAsync<Exception>().WithMessage("*redb:*");
    }

    [Theory]
    [InlineData("{\"Department\":{\"$eq\":\"x\"}}")]
    [InlineData("{\"$and\":[]}")]
    [InlineData("{\"Department\":{\"$eq\":null}}")]
    [InlineData("{\"$level\":1}")]
    public async Task AFilterItReads_IsBuilt(string filterJson)
    {
        (await BuildQueryAsync(filterJson)).Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task AnAggregateOnAFieldTheSchemeDoesNotHave_IsRefused()
    {
        var build = async () => await BuildAggregateAsync("[{\"alias\":\"s\",\"$sum\":{\"$field\":\"NoSuchField\"}}]");

        await build.Should().ThrowAsync<Exception>().WithMessage("*redb:*");
    }

    [Fact]
    public async Task AnAggregateItReads_IsBuilt()
    {
        (await BuildAggregateAsync("[{\"alias\":\"s\",\"$sum\":{\"$field\":\"Salary\"}}]")).Should().Contain("SUM(");
    }
}
