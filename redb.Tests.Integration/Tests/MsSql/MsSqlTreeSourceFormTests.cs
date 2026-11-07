using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// Free SQL Server module: a tree query with a Props filter calls the walk (pvt_tree_descendants) once and reads the
/// values of each object through OUTER APPLY. The pivot joined to the objects ran the walk on both sides, and a filter
/// over a 100 thousand node subtree took 3.3 s instead of 1.8 s (E204).
/// </summary>
[Collection("MsSql")]
public class MsSqlTreeSourceFormTests
{
    private readonly IRedbService _redb;

    public MsSqlTreeSourceFormTests(MsSqlFixture fixture) => _redb = fixture.Redb;

    [Fact]
    public async Task TreeQueryWithAPropsFilter_WalksTheTreeOnce()
    {
        await _redb.SyncSchemeAsync<EmployeeProps>();
        var root = new TreeRedbObject<EmployeeProps> { name = "form-" + Guid.NewGuid().ToString("N")[..8], Props = TestDataFactory.CreateEmployee(1).Props };
        root.id = await _redb.SaveAsync(root);

        var sql = await _redb.TreeQuery<EmployeeProps>(root.id)
            .Where(e => e.Salary >= 60000m && e.Salary < 90000m)
            .ToSqlStringAsync();

        sql.Should().Contain("OUTER APPLY (");
        System.Text.RegularExpressions.Regex.Matches(sql, @"pvt_tree_descendants\(").Count.Should().Be(1, sql);
    }
}
