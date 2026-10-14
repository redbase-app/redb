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
            .Take(50)
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
            .Take(50)
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
            .Take(50)
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

    // CountAsync and AnyAsync answer for the rows ToListAsync of the same query returns. They used
    // to answer for the whole filtered set: Take(3).CountAsync() over ten rows returned ten
    // (found 2026-10-12; every provider did it, the count SQL leaves LIMIT and OFFSET out).

    [Fact]
    public async Task CountAsync_FollowsTakeAndSkip()
    {
        var ids = await SeedAsync();
        var query = Redb.Query<EmployeeProps>().WhereRedb(o => ids.Contains(o.Id));

        (await query.CountAsync()).Should().Be(10);
        (await query.Take(3).CountAsync()).Should().Be(3);
        (await query.Take(50).CountAsync()).Should().Be(10);
        (await query.Skip(4).CountAsync()).Should().Be(6);
        (await query.Skip(4).Take(3).CountAsync()).Should().Be(3);
        (await query.Skip(8).Take(5).CountAsync()).Should().Be(2);
        (await query.Skip(10).CountAsync()).Should().Be(0);
        (await query.Skip(100).Take(5).CountAsync()).Should().Be(0);
        (await query.Take(0).CountAsync()).Should().Be(0);
    }

    [Fact]
    public async Task CountAsync_Paged_MatchesToListAsync()
    {
        var ids = await SeedAsync();
        var page = Redb.Query<EmployeeProps>()
            .WhereRedb(o => ids.Contains(o.Id))
            .Where(e => e.Age >= 0)
            .OrderBy(e => e.Age)
            .Skip(2)
            .Take(5);

        var rows = await page.ToListAsync();

        (await page.CountAsync()).Should().Be(rows.Count);
        (await page.Select(o => o.Id).CountAsync()).Should().Be(rows.Count, "a projection counts the rows of its source");
    }

    [Fact]
    public async Task AnyAsync_FollowsTakeAndSkip()
    {
        var ids = await SeedAsync();
        var query = Redb.Query<EmployeeProps>().WhereRedb(o => ids.Contains(o.Id));

        (await query.AnyAsync()).Should().BeTrue();
        (await query.Take(0).AnyAsync()).Should().BeFalse();
        (await query.Skip(9).AnyAsync()).Should().BeTrue();
        (await query.Skip(10).AnyAsync()).Should().BeFalse();
    }

    [Fact]
    public async Task AllAsync_OnPagedQuery_StillChecksEveryRow()
    {
        // Counts cut to the same page size are equal as soon as enough rows match; All has to
        // compare the counts of the whole set.
        var ids = await SeedAsync();
        var query = Redb.Query<EmployeeProps>().WhereRedb(o => ids.Contains(o.Id));
        var ages = (await query.ToListAsync()).Select(r => r.Props.Age).OrderBy(a => a).ToList();
        var youngest = ages[0];
        var threshold = ages[5];

        (await query.Take(3).AllAsync(e => e.Age >= youngest)).Should().BeTrue();
        (await query.Take(3).AllAsync(e => e.Age > threshold)).Should().Be(ages.All(a => a > threshold));
    }
}
