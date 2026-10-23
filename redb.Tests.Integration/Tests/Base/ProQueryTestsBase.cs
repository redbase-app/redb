using redb.Core;
using redb.Core.Query;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Query features: deep nested, arithmetic, string functions, Math.
/// Free and Pro must behave identically on the LINQ → query path
/// (they differ only in materialization and save).
/// </summary>
public abstract class ProQueryTestsBase
{
    protected readonly IRedbService Redb;
    protected readonly bool IsPro;

    protected ProQueryTestsBase(IRedbService redb, bool isPro)
    {
        Redb = redb;
        IsPro = isPro;
    }

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 20);
    }

    [Fact]
    public async Task DeepNested_OneLevel_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.City == "London")
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.HomeAddress!.City.Should().Be("London"));
    }

    [Fact]
    public async Task DeepNested_TwoLevels_Filters()
    {
        var ids = await SeedAsync();
        var idSet = ids.ToHashSet();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(e => idSet.Contains(e.Id))
            .Where(e => e.HomeAddress!.Building!.Floor > 10)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.HomeAddress!.Building!.Floor.Should().BeGreaterThan(10));
    }

    [Fact]
    public async Task Arithmetic_Multiply_AnnualSalary()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary * 12 > 1_000_000m)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            (r.Props.Salary * 12).Should().BeGreaterThan(1_000_000m));
    }

    [Fact]
    public async Task Arithmetic_MultiField_Scoring()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age * 1000 + e.Salary > 120_000m)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            (r.Props.Age * 1000 + r.Props.Salary).Should().BeGreaterThan(120_000m));
    }

    [Fact]
    public async Task Arithmetic_Division_MonthlySalary()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Salary / 12m) > 7000m)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            (r.Props.Salary / 12m).Should().BeGreaterThan(7000m));
    }

    [Fact]
    public async Task MathAbs_InWhere_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Salary - 80000m) < 10000m)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            Math.Abs(r.Props.Salary - 80000m).Should().BeLessThan(10000m));
    }

    [Fact]
    public async Task String_ToLower_Contains()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.ToLower().Contains("last"))
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.LastName.ToLower().Should().Contain("last"));
    }

    [Fact]
    public async Task String_Trim_Length()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.Trim().Length > 3)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.FirstName.Trim().Length.Should().BeGreaterThan(3));
    }

    [Fact]
    public async Task String_ToUpper_Length()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.ToUpper().Length > 5)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task String_Length_Property()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Length > 6)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.LastName.Length.Should().BeGreaterThan(6));
    }

    [Fact]
    public async Task DistinctBy_KeepsTheOrderingByTheKeyAndByAnotherField_PagesAndCounts()
    {
        // OrderByDescending(key).DistinctBy(key) came back ascending on SQLite and in no order on SQL Server, and an
        // ordering by another Props field failed there ("pvt_cte.Salary"). Nine employees, three departments.
        var tag = Guid.NewGuid().ToString("N")[..8];
        await Redb.SaveAsync(Enumerable.Range(0, 9)
            .Select(i => TestDataFactory.CreateEmployee(i, salary: 100m * (i + 1), department: $"{tag}-D{i % 3}")).ToList());
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department.StartsWith(tag));
        string D(int k) => $"{tag}-D{k}";
        async Task<List<string>> Departments(IRedbQueryable<EmployeeProps> q) =>
            (await q.ToListAsync()).Select(e => e.Props.Department).ToList();

        (await Departments(Tagged().OrderByDescending(e => e.Department).DistinctBy(e => e.Department)))
            .Should().Equal(new[] { D(2), D(1), D(0) }, "the key descending");
        (await Departments(Tagged().OrderBy(e => e.Department).DistinctBy(e => e.Department)))
            .Should().Equal(new[] { D(0), D(1), D(2) }, "the key ascending");
        (await Departments(Tagged().OrderBy(e => e.Salary).DistinctBy(e => e.Department)))
            .Should().Equal(new[] { D(0), D(1), D(2) }, "ordered by another field: DistinctBy sorts by its key first");
        (await Departments(Tagged().Where(e => e.Salary > 700m).OrderByDescending(e => e.Department).DistinctBy(e => e.Department)))
            .Should().Equal(new[] { D(2), D(1) }, "the filter keeps employees 8 and 9 only");
        (await Departments(Tagged().OrderByDescending(e => e.Department).DistinctBy(e => e.Department).Skip(1).Take(1)))
            .Should().Equal(new[] { D(1) }, "the page of the distinct rows");

        (await Tagged().DistinctBy(e => e.Department).CountAsync()).Should().Be(3);
        (await Tagged().Where(e => e.Salary > 700m).DistinctBy(e => e.Department).CountAsync()).Should().Be(2);
    }
}
