using redb.Core;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// A14: the free module's array grouping refuses, with a reason, what it cannot answer instead of reading it as a
/// field with no rows: an unknown '$' token, '$element' on an array of classes, a field of an array of simple values,
/// a field the elements do not have, a dictionary in the array path. The core refuses these forms before they reach
/// SQL, so the function is called directly - the SQL is the public surface for callers that submit the JSON themselves.
/// </summary>
[Collection("Postgres")]
public class PostgresArrayGroupModuleGuardTests
{
    private readonly IRedbService _redb;

    public PostgresArrayGroupModuleGuardTests(PostgresFixture fixture) => _redb = fixture.Redb;

    private async Task<string?> BuildAsync(string arrayPath, string groupByJson)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT pvt_build_array_groupby_sql(" + scheme.Id + ", $1::text, NULL::jsonb, $2::jsonb, NULL::jsonb)",
            new object[] { arrayPath, groupByJson });
    }

    [Theory]
    [InlineData("Contacts", "[{\"field\":\"$foo\",\"alias\":\"x\"}]", "*\"$foo\" is not a field of the elements of \"Contacts\"*")]
    [InlineData("Contacts", "[{\"field\":\"$element\",\"alias\":\"x\"}]", "*\"$element\" names the element itself*")]
    [InlineData("Skills", "[{\"field\":\"Name\",\"alias\":\"x\"}]", "*simple values and have no field \"Name\"*")]
    [InlineData("Contacts.Metadata", "[{\"field\":\"Type\",\"alias\":\"x\"}]", "*\"Type\" is not a field of the elements of \"Contacts.Metadata\"*")]
    [InlineData("PhoneDirectory", "[{\"field\":\"$element\",\"alias\":\"x\"}]", "*dictionary*")]
    [InlineData("HomeAddress", "[{\"field\":\"City\",\"alias\":\"x\"}]", "*\"HomeAddress\" is not an array*")]
    public async Task WhatItCannotAnswer_IsRefusedWithAReason(string arrayPath, string groupByJson, string message)
    {
        var act = async () => await BuildAsync(arrayPath, groupByJson);
        (await act.Should().ThrowAsync<Exception>()).Which.Message.Should().Match(message);
    }

    [Fact]
    public async Task AnInnerArrayAndTheElementOfASimpleArray_AreBuilt()
    {
        (await BuildAsync("Contacts.Metadata", "[{\"field\":\"Label\",\"alias\":\"Label\"}]")).Should().Contain("GROUP BY");
        (await BuildAsync("Skills", "[{\"field\":\"$element\",\"alias\":\"Skill\"}]")).Should().Contain("GROUP BY");
    }
}
