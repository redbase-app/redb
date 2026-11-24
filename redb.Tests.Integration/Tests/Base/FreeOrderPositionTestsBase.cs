using redb.Core;
using redb.Core.Query;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The inner statement of a Free query returns _ord next to _id, the position of the row in the order of the query,
/// and the list read sorts by it after the join. Numbering the rows outside the statement (ROW_NUMBER() OVER (ORDER BY
/// (SELECT NULL)) on SQL Server, row_number() OVER () on PostgreSQL) relied on the plan reading them in order: on SQL
/// Server OrderByDescending(key).DistinctBy(key) came back ascending.
/// </summary>
public abstract class FreeOrderPositionTestsBase
{
    protected readonly IRedbService Redb;

    protected FreeOrderPositionTestsBase(IRedbService redb) => Redb = redb;

    public static IEnumerable<object[]> Cases() =>
        new[] { "order", "order-desc", "distinct-by", "page", "no-order", "distinct" }.Select(c => new object[] { c });

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task InnerStatement_OrdGivesTheOrderOfTheListRead(string kind)
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        // Salaries out of id order, three departments.
        var salaries = new[] { 500m, 100m, 900m, 300m, 700m, 200m, 800m };
        await Redb.SaveAsync(salaries.Select((s, i) =>
            TestDataFactory.CreateEmployee(i, salary: s, department: $"{tag}-D{i % 3}")).ToList());
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department.StartsWith(tag));

        var query = kind switch
        {
            "order" => Tagged().OrderBy(e => e.Salary),
            "order-desc" => Tagged().OrderByDescending(e => e.Salary),
            "distinct-by" => Tagged().OrderByDescending(e => e.Department).DistinctBy(e => e.Department),
            "page" => Tagged().OrderBy(e => e.Salary).Skip(2).Take(3),
            "no-order" => Tagged(),
            "distinct" => Tagged().Distinct(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        var listed = (await query.ToListAsync()).Select(o => o.Id).ToList();
        var inner = await query.ToSqlStringAsync();
        var byOrd = await Redb.Context.QueryScalarListAsync<long>(
            "SELECT t._id FROM (\n" + inner + "\n) t ORDER BY t._ord", Array.Empty<object>());

        listed.Should().NotBeEmpty();
        byOrd.Should().Equal(listed, "_ord of the inner statement is the order the list read returns");
    }
}
