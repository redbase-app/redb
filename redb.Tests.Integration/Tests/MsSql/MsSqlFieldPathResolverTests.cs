using redb.Core;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// <c>dbo.pvt_resolve_field_path</c> on SQL Server Free. T-SQL keeps a variable's old value when
/// <c>SELECT @v = ...</c> finds no row, so a nested child the scheme does not have resolved to its parent:
/// <c>Contacts[].NoSuchField</c> came back as the Contacts array itself, and whatever read it - a filter, a sort,
/// a grouping - silently used the wrong structure.
/// </summary>
[Collection("MsSql")]
public class MsSqlFieldPathResolverTests
{
    private readonly IRedbService _redb;

    public MsSqlFieldPathResolverTests(MsSqlFixture fixture) => _redb = fixture.Redb;

    private async Task<string?> ResolveAsync(string path)
    {
        var scheme = await _redb.SyncSchemeAsync<EmployeeProps>();
        return await _redb.Context.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_resolve_field_path(" + scheme.Id + ", $1)", new object[] { path });
    }

    [Fact]
    public async Task ANestedChildTheSchemeDoesNotHave_IsNotResolved()
    {
        (await ResolveAsync("Contacts[].NoSuchField")).Should().BeNull();
    }

    [Fact]
    public async Task ANestedChildTheSchemeHas_IsResolved()
    {
        var parent = await ResolveAsync("Contacts[]");
        var child = await ResolveAsync("Contacts[].Type");

        child.Should().NotBeNull().And.Contain("\"db_type\":\"String\"");
        child.Should().NotBe(parent);
    }
}
