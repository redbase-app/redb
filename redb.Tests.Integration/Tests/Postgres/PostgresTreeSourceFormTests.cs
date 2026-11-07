using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// Free PostgreSQL module: a tree query restricts the objects to the walk as o._id = ANY(ARRAY(SELECT _id FROM
/// _pvt_tree ...)). The pivot starts from the objects, so the walk is read twice; as an IN subquery the recursive CTE,
/// estimated at a few hundred rows, became the inner side of nested loops, and a Props filter over a 100 thousand node
/// subtree ran for minutes (E204). The array is computed once and both reads become index lookups.
/// </summary>
[Collection("Postgres")]
public class PostgresTreeSourceFormTests
{
    private readonly IRedbService _redb;

    public PostgresTreeSourceFormTests(PostgresFixture fixture) => _redb = fixture.Redb;

    [Fact]
    public async Task TreeQueryWithAPropsFilter_ReadsTheWalkAsAnArray()
    {
        await _redb.SyncSchemeAsync<EmployeeProps>();
        var root = new TreeRedbObject<EmployeeProps> { name = "form-" + Guid.NewGuid().ToString("N")[..8], Props = TestDataFactory.CreateEmployee(1).Props };
        root.id = await _redb.SaveAsync(root);

        var sql = await _redb.TreeQuery<EmployeeProps>(root.id)
            .Where(e => e.Salary >= 60000m && e.Salary < 90000m)
            .ToSqlStringAsync();

        sql.Should().Contain("= ANY(ARRAY(SELECT _id FROM _pvt_tree");
        sql.Should().NotContain("IN (SELECT _id FROM _pvt_tree");
    }

    [Fact]
    public async Task TreeQueryWithALevel_ReadsTheWalkDepthAsAnArray()
    {
        await _redb.SyncSchemeAsync<EmployeeProps>();
        var root = new TreeRedbObject<EmployeeProps> { name = "form-" + Guid.NewGuid().ToString("N")[..8], Props = TestDataFactory.CreateEmployee(1).Props };
        root.id = await _redb.SaveAsync(root);

        var sql = await _redb.TreeQuery<EmployeeProps>(root.id)
            .Where(e => e.Salary > 0m)
            .WhereLevel(1)
            .ToSqlStringAsync();

        sql.Should().Contain("= ANY(ARRAY(SELECT _id FROM _pvt_tree GROUP BY _id HAVING");
        sql.Should().NotContain("IN (SELECT _id FROM _pvt_tree");
    }
}
