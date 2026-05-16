using redb.Core;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class OrderingTestsBase
{
    protected readonly IRedbService Redb;

    protected OrderingTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 10);
    }

    [Fact]
    public async Task OrderBy_Ascending_SortsByField()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Age)
            .ToListAsync();

        results.Should().HaveCountGreaterThanOrEqualTo(10);
        var ages = results.Select(r => r.Props.Age).ToList();
        ages.Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task OrderByDescending_SortsByFieldDesc()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .OrderByDescending(e => e.Salary)
            .ToListAsync();

        var salaries = results.Select(r => r.Props.Salary).ToList();
        salaries.Should().BeInDescendingOrder();
    }

    [Fact]
    public async Task OrderBy_ThenBy_MultipleSort()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Department)
            .ThenBy(e => e.Age)
            .ToListAsync();

        results.Should().NotBeEmpty();

        // Within each department group, ages should be ascending
        var grouped = results.GroupBy(r => r.Props.Department);
        foreach (var group in grouped)
        {
            var ages = group.Select(r => r.Props.Age).ToList();
            ages.Should().BeInAscendingOrder($"Ages in department {group.Key} should be sorted");
        }
    }

    [Fact]
    public async Task Skip_Take_ReturnsPage()
    {
        var ids = await SeedAsync();

        var page = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Age)
            .Skip(3)
            .Take(4)
            .ToListAsync();

        page.Should().HaveCount(4);
    }
}
