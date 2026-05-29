using redb.Core;
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
}
