using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Windows of the Pro providers: the shape that reads the shown fields after Take/Skip, and the SQL preview (the Free
/// providers run windows through a SQL function and have no preview).
/// </summary>
public abstract class ProWindowTestsBase : WindowTestsBase
{
    protected ProWindowTestsBase(IRedbService redb) : base(redb) { }

    [Fact]
    public async Task Window_SkipTake_ShownFieldsAreReadForTheRowsKept()
    {
        // The window is computed over the fields it needs; the fields only shown (FirstName, HomeAddress.City) are read
        // afterwards for the rows Skip/Take keep. Each row must carry its own fields, and the window values must be
        // those of the whole filtered set. Two departments of this test only, so parallel seeding changes no rank.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var (a, b) = ($"win-a-{tag}", $"win-b-{tag}");
        await Redb.SaveAsync(Enumerable.Range(0, 24)
            .Select(i => TestDataFactory.CreateEmployee(i, firstName: $"W{tag}{i:D2}", salary: 40000m + (i % 7) * 1000m,
                department: i % 2 == 0 ? a : b, city: $"City{i % 5}"))
            .ToList());

        var truth = await Redb.Query<EmployeeProps>().Where(e => e.Department == a || e.Department == b).ToListAsync();
        truth.Should().HaveCount(24);
        var byId = truth.ToDictionary(o => o.Id);

        var rows = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == a || e.Department == b)
            .Skip(5).Take(10)
            .WithWindow(w => w.PartitionBy(e => e.Department).OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Id, e.Props.FirstName, City = e.Props.HomeAddress!.City, e.Props.Department,
                Rank = Win.Rank(), Total = Win.Sum(e.Props.Salary)
            });

        rows.Should().HaveCount(10);
        rows.Select(r => r.Id).Should().OnlyHaveUniqueItems();
        foreach (var r in rows)
        {
            var t = byId[r.Id];
            var department = truth.Where(o => o.Props.Department == t.Props.Department).Select(o => o.Props.Salary).ToList();
            r.FirstName.Should().Be(t.Props.FirstName);
            r.City.Should().Be(t.Props.HomeAddress!.City);
            r.Department.Should().Be(t.Props.Department);
            r.Rank.Should().Be(1 + department.Count(s => s > t.Props.Salary));
            r.Total.Should().Be(department.Where(s => s >= t.Props.Salary).Sum(), "a running sum within the department");
        }
    }

    [Fact]
    public async Task Window_SqlPreview_CarriesTheFilter()
    {
        // ToSqlStringAsync of a window after Where used to drop the filter: the preview is the SQL that runs.
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 70123m)
            .Take(20)
            .WithWindow(w => w.PartitionBy(e => e.Department).OrderByDesc(e => e.Salary))
            .ToSqlStringAsync(e => new { e.Props.FirstName, Rank = Win.RowNumber() });

        sql.Should().Contain("70123");
    }
}
