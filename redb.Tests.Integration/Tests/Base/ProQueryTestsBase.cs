using redb.Core;
using redb.Core.Exceptions;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Pro query features: deep nested, arithmetic, string functions, Math.
/// On Free edition, Pro-only features must throw RedbProRequiredException.
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

    // ─── Deep nested (1 level = Free, 2+ levels = Pro) ───

    [Fact]
    public async Task DeepNested_OneLevel_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.City == "London")
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.HomeAddress!.City.Should().Be("London"));
    }

    [Fact]
    public async Task DeepNested_TwoLevels_Filters()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.HomeAddress!.Building!.Floor > 10)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.Building!.Floor > 10)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.HomeAddress!.Building!.Floor.Should().BeGreaterThan(10));
    }

    // ─── Arithmetic (Pro only) ───

    [Fact]
    public async Task Arithmetic_Multiply_AnnualSalary()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.Salary * 12 > 1_000_000m)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary * 12 > 1_000_000m)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            (r.Props.Salary * 12).Should().BeGreaterThan(1_000_000m));
    }

    [Fact]
    public async Task Arithmetic_MultiField_Scoring()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.Age * 1000 + e.Salary > 120_000m)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age * 1000 + e.Salary > 120_000m)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            (r.Props.Age * 1000 + r.Props.Salary).Should().BeGreaterThan(120_000m));
    }

    [Fact]
    public async Task Arithmetic_Division_MonthlySalary()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => (e.Salary / 12m) > 7000m)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Salary / 12m) > 7000m)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            (r.Props.Salary / 12m).Should().BeGreaterThan(7000m));
    }

    // ─── Math functions (Pro only) ───

    [Fact]
    public async Task MathAbs_InWhere_Filters()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => Math.Abs(e.Salary - 80000m) < 10000m)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => Math.Abs(e.Salary - 80000m) < 10000m)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            Math.Abs(r.Props.Salary - 80000m).Should().BeLessThan(10000m));
    }

    // ─── String functions (Pro only) ───

    [Fact]
    public async Task String_ToLower_Contains()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.LastName.ToLower().Contains("last"))
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.ToLower().Contains("last"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.LastName.ToLower().Should().Contain("last"));
    }

    [Fact]
    public async Task String_Trim_Length()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.FirstName.Trim().Length > 3)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.Trim().Length > 3)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.FirstName.Trim().Length.Should().BeGreaterThan(3));
    }

    [Fact]
    public async Task String_ToUpper_Length()
    {
        var ids = await SeedAsync();

        if (!IsPro)
        {
            var act = () => Redb.Query<EmployeeProps>()
                .Where(e => e.Department.ToUpper().Length > 5)
                .ToListAsync();
            await act.Should().ThrowAsync<RedbProRequiredException>();
            return;
        }

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.ToUpper().Length > 5)
            .ToListAsync();

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task String_Length_Property()
    {
        var ids = await SeedAsync();

        // String.Length is a Free feature (E172_StringLength)
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Length > 6)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.LastName.Length.Should().BeGreaterThan(6));
    }
}
