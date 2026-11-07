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

        sql.Should().Be("--error: \"NoSuchField\" is not a field of the elements of \"Contacts\"",
            "grouping on the known key alone is a different grouping");
    }

    [Fact]
    public async Task NoKeyTheItemHas_RefusesTheQuery()
    {
        var sql = await BuildAsync("[{\"field\":\"NoSuchField\",\"alias\":\"X\"}]", CountAll);

        sql.Should().Be("--error: \"NoSuchField\" is not a field of the elements of \"Contacts\"",
            "a flat list of items is not a grouping");
    }

    [Fact]
    public async Task AnAggregateOnAFieldTheItemDoesNotHave_RefusesTheQuery()
    {
        var sql = await BuildAsync("[{\"field\":\"Type\",\"alias\":\"Type\"}]",
            "[{\"field\":\"NoSuchField\",\"func\":\"MAX\",\"alias\":\"M\"}]");

        sql.Should().Be("--error: \"NoSuchField\" is not a field of the elements of \"Contacts\"",
            "the aggregate would be missing from the result");
    }

    [Fact]
    public async Task KnownKeysAndAggregates_AreBuilt()
    {
        var sql = await BuildAsync("[{\"field\":\"Type\",\"alias\":\"Type\"}]",
            "[{\"field\":\"Value\",\"func\":\"MAX\",\"alias\":\"M\"}]");

        sql.Should().Contain("GROUP BY").And.Contain("AS [M]");
    }

    private async Task<string?> BuildOnAsync(string arrayPath, string groupByJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_array_groupby_sql(" + scheme.Id + ", $1, NULL, $2, NULL, NULL, N'flat')",
            new object[] { arrayPath, groupByJson });
    }

    [Theory]
    [InlineData("Contacts", "[{\"field\":\"$foo\",\"alias\":\"x\"}]", "--error: \"$foo\" is not a field of the elements of \"Contacts\"")]
    [InlineData("Contacts", "[{\"field\":\"$element\",\"alias\":\"x\"}]", "--error: \"$element\" names the element itself*")]
    [InlineData("Skills", "[{\"field\":\"Name\",\"alias\":\"x\"}]", "*simple values and have no field \"Name\"*")]
    [InlineData("Contacts.Metadata", "[{\"field\":\"Type\",\"alias\":\"x\"}]", "--error: \"Type\" is not a field of the elements of \"Contacts.Metadata\"")]
    [InlineData("PhoneDirectory", "[{\"field\":\"$element\",\"alias\":\"x\"}]", "*dictionary*")]
    [InlineData("HomeAddress", "[{\"field\":\"City\",\"alias\":\"x\"}]", "--error: \"HomeAddress\" is not an array")]
    public async Task WhatItCannotAnswer_ComesBackAsAnError(string arrayPath, string groupByJson, string message)
    {
        // A14: a scalar function cannot raise; the provider throws the text after "--error:".
        (await BuildOnAsync(arrayPath, groupByJson)).Should().Match(message);
    }
}
