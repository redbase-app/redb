using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class AggregationTestsBase
{
    protected readonly IRedbService Redb;
    protected readonly bool IsPro;

    protected AggregationTestsBase(IRedbService redb, bool isPro = false)
    {
        Redb = redb;
        IsPro = isPro;
    }

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 15);
    }

    [Fact]
    public async Task CountAsync_ReturnsTotal()
    {
        var ids = await SeedAsync();

        var count = await Redb.Query<EmployeeProps>().CountAsync();

        count.Should().BeGreaterThanOrEqualTo(15);
    }

    [Fact]
    public async Task CountAsync_WithFilter_Counts()
    {
        var ids = await SeedAsync();

        var count = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .CountAsync();

        count.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task AnyAsync_WithData_ReturnsTrue()
    {
        var ids = await SeedAsync();

        var any = await Redb.Query<EmployeeProps>().AnyAsync();

        any.Should().BeTrue();
    }

    [Fact]
    public async Task AnyAsync_NoMatch_ReturnsFalse()
    {
        var any = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 99_999_999m)
            .AnyAsync();

        any.Should().BeFalse();
    }

    [Fact]
    public async Task SumAsync_SumsSalaries()
    {
        var ids = await SeedAsync();

        var sum = await Redb.Query<EmployeeProps>()
            .SumAsync(e => e.Salary);

        sum.Should().BeGreaterThan(0m);
    }

    [Fact]
    public async Task AverageAsync_AveragesAge()
    {
        var ids = await SeedAsync();

        var avg = await Redb.Query<EmployeeProps>()
            .AverageAsync(e => e.Age);

        avg.Should().BeGreaterThan(0m);
    }

    [Fact]
    public async Task MinAsync_FindsMinSalary()
    {
        var ids = await SeedAsync();

        var min = await Redb.Query<EmployeeProps>()
            .MinAsync(e => e.Salary);

        min.Should().NotBeNull();
        min.Should().BeGreaterThan(0m);
    }

    [Fact]
    public async Task MaxAsync_FindsMaxSalary()
    {
        var ids = await SeedAsync();

        var max = await Redb.Query<EmployeeProps>()
            .MaxAsync(e => e.Salary);

        max.Should().NotBeNull();
        max!.Value.Should().BeGreaterThan(0m);
    }

    [Fact]
    public async Task AggregateAsync_CustomProjection()
    {
        var ids = await SeedAsync();

        var result = await Redb.Query<EmployeeProps>()
            .AggregateAsync(e => new
            {
                TotalSalary = Agg.Sum(e.Props.Salary),
                AvgAge = Agg.Average(e.Props.Age),
                MinSalary = Agg.Min(e.Props.Salary),
                MaxSalary = Agg.Max(e.Props.Salary),
                Count = Agg.Count()
            });

        result.Should().NotBeNull();
        result!.TotalSalary.Should().BeGreaterThan(0m);
        result.Count.Should().BeGreaterThanOrEqualTo(15);
        result.MinSalary.Should().BeLessThanOrEqualTo(result.MaxSalary);
    }

    [Fact]
    public async Task Select_ProjectsFields()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.Age)
            .Take(5)
            .Select(e => new { e.Props.FirstName, e.Props.Salary })
            .ToListAsync();

        results.Should().HaveCount(5);
        results.Should().AllSatisfy(r =>
        {
            r.FirstName.Should().NotBeEmpty();
            r.Salary.Should().BeGreaterThan(0m);
        });
    }

    [Fact]
    public async Task Distinct_RemovesDuplicateDepartments()
    {
        var ids = await SeedAsync();

        var departments = await Redb.Query<EmployeeProps>()
            .Select(e => e.Props.Department)
            .Distinct()
            .ToListAsync();

        departments.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task DistinctBy_RemovesDuplicatesByField()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .DistinctBy(e => e.Department)
            .ToListAsync();

        var depts = results.Select(r => r.Props.Department).ToList();
        depts.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task WhereIn_FiltersMultipleValues()
    {
        var ids = await SeedAsync();

        var targets = new[] { "Engineering", "Marketing" };
        var results = await Redb.Query<EmployeeProps>()
            .WhereIn(e => e.Department, targets)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Department.Should().BeOneOf(targets));
    }

    [Fact]
    public async Task SumAsync_WithFilter_SumsFiltered()
    {
        var ids = await SeedAsync();

        var totalSum = await Redb.Query<EmployeeProps>()
            .SumAsync(e => e.Salary);

        var remoteSum = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .SumAsync(e => e.Salary);

        var officeSum = await Redb.Query<EmployeeProps>()
            .Where(e => !e.IsRemote)
            .SumAsync(e => e.Salary);

        (remoteSum + officeSum).Should().Be(totalSum);
    }

    [Fact]
    public async Task AllAsync_PositiveSalary_ReturnsTrue()
    {
        var ids = await SeedAsync();

        var all = await Redb.Query<EmployeeProps>()
            .AllAsync(e => e.Salary > 0);

        all.Should().BeTrue();
    }

    // ===== FILTER-AWARE base-field (RedbAsync) aggregations =====
    // These cover the gap that let a defect ship: base-field aggregations (SumRedbAsync / AggregateRedbAsync)
    // routed the filter through the filterJson provider overload, which Pro drops → aggregate over the WHOLE
    // scheme. Free was fine. Runs on all six fixtures (Free + Pro × 3), so both paths are checked.

    [Fact]
    public async Task SumRedbAsync_WithFilter_ScopesToFilteredObjects()
    {
        var ids = await SeedAsync();
        var oneId = ids[0];

        var allSum = await Redb.Query<EmployeeProps>().SumRedbAsync(o => o.Id);
        var filteredSum = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Id == oneId)
            .SumRedbAsync(o => o.Id);

        filteredSum.Should().Be(oneId, "the filter must scope the base-field sum to the one matching object");
        filteredSum.Should().NotBe(allSum, "a dropped filter would sum Id over the whole scheme");
    }

    [Fact]
    public async Task AggregateRedbAsync_WithFilter_ScopesToFilteredObjects()
    {
        var ids = await SeedAsync();
        var oneId = ids[0];

        var stats = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Id == oneId)
            .AggregateRedbAsync(o => new { Count = Agg.Count(), SumId = Agg.Sum(o.Id) });

        ((int)stats.Count).Should().Be(1, "the filter must scope the base-field aggregate to one object");
        ((long)stats.SumId).Should().Be(oneId);
    }

    [Fact]
    public async Task SumRedbAsync_EmptySelection_ReturnsZero_DoesNotThrow()
    {
        // The second defect the filter fix exposed: once WhereRedb actually narrows, a filter matching
        // nothing yields an EMPTY selection → SUM is NULL in SQL → reading it as a number threw
        // InvalidOperationException. Before the filter fix the selection was never empty (filter was
        // dropped), so this path was unreachable. "A ledger with no movements" must sum to 0, not throw.
        await SeedAsync();

        var sum = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Id == -1)   // no object has id -1 → empty selection
            .SumRedbAsync(o => o.Id);

        sum.Should().Be(0m);
    }

    [Fact]
    public async Task AverageRedbAsync_EmptySelection_ReturnsZero_DoesNotThrow()
    {
        await SeedAsync();

        var avg = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Id == -1)
            .AverageRedbAsync(o => o.Id);

        avg.Should().Be(0m);
    }

    [Fact]
    public async Task GetStatisticsAsync_WithFilter_ScopesToFilteredObjects()
    {
        // GetStatisticsAsync is the Props path, but shares the same filterJson-drop defect on Pro.
        var ids = await SeedAsync();

        var allStats = await Redb.Query<EmployeeProps>().GetStatisticsAsync(e => e.Salary);
        var remoteStats = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .GetStatisticsAsync(e => e.Salary);

        remoteStats.Count.Should().BeLessThan(allStats.Count,
            "the filter must scope the statistics; a dropped filter would count the whole scheme");
        remoteStats.Count.Should().BeGreaterThan(0);
    }
}
