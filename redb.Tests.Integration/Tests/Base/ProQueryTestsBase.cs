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

    [Fact]
    public async Task DistinctRedb_OnePerBaseFields_WithAPage()
    {
        // DistinctRedb partitions by a list of columns of the object, not one key (example E139). Six employees under two
        // names, everything else of the object equal: two rows.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var employees = Enumerable.Range(0, 6)
            .Select(i => TestDataFactory.CreateEmployee(i, salary: 100m * (i + 1), department: $"{tag}-dist")).ToList();
        for (var i = 0; i < employees.Count; i++)
            employees[i].name = $"{tag}-name{i % 2}";
        await Redb.SaveAsync(employees);
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department == $"{tag}-dist");

        var rows = await Tagged().DistinctRedb().Take(100).ToListAsync();
        rows.Select(r => r.name).Should().BeEquivalentTo(new[] { $"{tag}-name0", $"{tag}-name1" });
        (await Tagged().DistinctRedb().CountAsync()).Should().Be(2);
    }

    [Fact]
    public async Task Distinct_OnePerPropsHash_CountsOrdersAndPages()
    {
        // Distinct() keeps one object per Props hash (Pro; Free deduplicates by object). Three employees with the props
        // of index 0 and two with those of index 1: two rows.
        if (!IsPro) return;
        var tag = Guid.NewGuid().ToString("N")[..8];
        await Redb.SaveAsync(new[] { 0, 0, 0, 1, 1 }
            .Select(k => TestDataFactory.CreateEmployee(k, department: $"{tag}-hash")).ToList());
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department == $"{tag}-hash");
        var low = TestDataFactory.CreateEmployee(0).Props.Salary;
        var high = TestDataFactory.CreateEmployee(1).Props.Salary;

        (await Tagged().Distinct().ToListAsync()).Select(e => e.Props.Salary).Should().BeEquivalentTo(new[] { low, high });
        (await Tagged().Distinct().CountAsync()).Should().Be(2);
        (await Tagged().Distinct().OrderByDescending(e => e.Salary).Take(1).ToListAsync())
            .Select(e => e.Props.Salary).Should().Equal(high);
        (await Tagged().Distinct().OrderBy(e => e.Salary).Skip(1).ToListAsync())
            .Select(e => e.Props.Salary).Should().Equal(high);
    }

    [Fact]
    public async Task DistinctRedb_WithAnOrderingAndSkip()
    {
        // Six employees under two names: one row per name, the first saved of each (salaries 100 and 200).
        var tag = Guid.NewGuid().ToString("N")[..8];
        var employees = Enumerable.Range(0, 6)
            .Select(i => TestDataFactory.CreateEmployee(i, salary: 100m * (i + 1), department: $"{tag}-dr")).ToList();
        for (var i = 0; i < employees.Count; i++)
            employees[i].name = $"{tag}-name{i % 2}";
        await Redb.SaveAsync(employees);
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department == $"{tag}-dr");

        var ordered = await Tagged().DistinctRedb().OrderBy(e => e.Salary).ToListAsync();
        ordered.Select(e => e.name).Should().Equal($"{tag}-name0", $"{tag}-name1");
        (await Tagged().DistinctRedb().OrderBy(e => e.Salary).Skip(1).ToListAsync())
            .Select(e => e.name).Should().Equal($"{tag}-name1");
    }

    [Fact]
    public async Task DistinctByRedb_CountsOrdersAndPages()
    {
        // Six employees under three names: one per name, in the order of the key.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var employees = Enumerable.Range(0, 6)
            .Select(i => TestDataFactory.CreateEmployee(i, salary: 100m * (i + 1), department: $"{tag}-dbr")).ToList();
        for (var i = 0; i < employees.Count; i++)
            employees[i].name = $"{tag}-n{i % 3}";
        await Redb.SaveAsync(employees);
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department == $"{tag}-dbr");

        (await Tagged().DistinctByRedb(o => o.Name).CountAsync()).Should().Be(3);
        (await Tagged().DistinctByRedb(o => o.Name).ToListAsync()).Select(e => e.name)
            .Should().BeEquivalentTo(new[] { $"{tag}-n0", $"{tag}-n1", $"{tag}-n2" });
        (await Tagged().OrderBy(e => e.Salary).DistinctByRedb(o => o.Name).Skip(1).Take(1).ToListAsync())
            .Select(e => e.name).Should().Equal($"{tag}-n1");
    }

    [Fact]
    public async Task DistinctBy_ThenAnyAllWindowGroupBy_CurrentBehaviour()
    {
        // What these combinations do today; the intended meaning is not defined yet (see BUGS_FOUND). Department A: the
        // first saved has salary 100, the second 900; department B: 900.
        var tag = Guid.NewGuid().ToString("N")[..8];
        await Redb.SaveAsync(new[] { ("A", 100m), ("A", 900m), ("B", 900m) }
            .Select((x, i) => TestDataFactory.CreateEmployee(i, salary: x.Item2, department: $"{tag}-{x.Item1}")).ToList());
        IRedbQueryable<EmployeeProps> Tagged() => Redb.Query<EmployeeProps>().Where(e => e.Department.StartsWith(tag));

        (await Tagged().DistinctBy(e => e.Department).AnyAsync()).Should().BeTrue();
        // AllAsync after DistinctBy is left out: the providers disagree today (BUGS_FOUND §9). PostgreSQL asks whether
        // every kept row passes (false here: the kept row of A has salary 100), SQL Server and SQLite whether every key
        // has a row that passes (true).
        // A window and a grouping read every row: the distinct is not applied.
        (await Tagged().DistinctBy(e => e.Department).WithWindow(w => w.OrderByDesc(x => x.Salary))
            .SelectAsync(x => new { x.Id, R = global::redb.Core.Query.Aggregation.Win.RowNumber() })).Should().HaveCount(3);
        (await Tagged().DistinctBy(e => e.Department).GroupBy(x => x.Department)
            .SelectAsync(g => new { g.Key, N = global::redb.Core.Query.Aggregation.Agg.Count(g) })).Sum(g => g.N).Should().Be(3);
    }
}
