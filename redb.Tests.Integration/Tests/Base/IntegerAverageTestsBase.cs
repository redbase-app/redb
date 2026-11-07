using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The average of an integer field keeps its fraction on every form: an engine that keeps the type of the operand
/// (T-SQL AVG over bigint, integer division in a native function) returns 32 for 32.25. Each test compares with the
/// same LINQ over the seeded objects in memory, flat and over a tree.
///
/// Seed: two departments. Ages 30, 31, 33, 35 (average 32.25) and 40, 41, 43 (41.333...), all seven 36.142857...
/// SkillLevels elements 1, 2, 2, 4, 2, 1, 3, 3 (average 2.25). None of these is a whole number or a half.
/// </summary>
public abstract class IntegerAverageTestsBase
{
    protected readonly IRedbService Redb;

    protected IntegerAverageTestsBase(IRedbService redb) => Redb = redb;

    private static readonly (int Dept, int Age, int[] Levels)[] Rows =
    [
        (0, 30, [1, 2]), (0, 31, [2]), (0, 33, [4]), (0, 35, [2]),
        (1, 40, [1]), (1, 41, [3, 3]), (1, 43, []),
    ];

    private const double Precision = 1e-6;

    private sealed record Seeded(long RootId, List<long> Ids, List<EmployeeProps> Props);

    private async Task<Seeded> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "ia-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"{tag}-root",
            Props = TestDataFactory.CreateEmployee(0, department: $"{tag}-root").Props
        };
        root.id = await Redb.SaveAsync(root);

        var ids = new List<long>();
        var props = new List<EmployeeProps>();
        for (var i = 0; i < Rows.Length; i++)
        {
            var p = TestDataFactory.CreateEmployee(i + 1, firstName: $"{tag}-{i}", department: $"{tag}-d{Rows[i].Dept}").Props;
            p.Age = Rows[i].Age;
            p.SkillLevels = Rows[i].Levels;
            ids.Add(await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"{tag}-{i}", Props = p }, root));
            props.Add(p);
        }
        return new Seeded(root.id, ids, props);
    }

    private IRedbQueryable<EmployeeProps> Employees(Seeded s, bool tree)
    {
        if (tree)
            return Redb.TreeQuery<EmployeeProps>(s.RootId).WhereRedb(o => o.Id != s.RootId);
        var idSet = s.Ids.ToHashSet();
        return Redb.Query<EmployeeProps>().WhereRedb(o => idSet.Contains(o.Id));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AverageAsync_IntegerField_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var avg = await Employees(s, tree).AverageAsync(e => e.Age);

        ((double)avg).Should().BeApproximately(s.Props.Average(p => p.Age), Precision);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AverageRedbAsync_BaseId_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var avg = await Employees(s, tree).AverageRedbAsync(o => o.Id);

        ((double)avg).Should().BeApproximately(s.Ids.Average(), Precision);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AggregateRedbAsync_BaseId_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var r = await Employees(s, tree).AggregateRedbAsync(o => new { Avg = Agg.Average(o.Id) });

        ((double)r.Avg).Should().BeApproximately(s.Ids.Average(), Precision);
    }


    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AggregateAsync_IntegerFieldAndArrayElements_KeepTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var r = await Employees(s, tree).AggregateAsync(x => new
        {
            AvgAge = Agg.Average(x.Props.Age),
            AvgLevel = Agg.Average(x.Props.SkillLevels!.Select(v => v)),
            Count = Agg.Count()
        });

        r.Count.Should().Be(s.Props.Count);
        ((double)r.AvgAge).Should().BeApproximately(s.Props.Average(p => p.Age), Precision);
        ((double)r.AvgLevel).Should().BeApproximately(s.Props.SelectMany(p => p.SkillLevels!).Average(), Precision);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GroupBy_AverageOfIntegerField_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var groups = await Employees(s, tree).GroupBy(e => e.Department)
            .SelectAsync(g => new { Key = g.Key, Avg = Agg.Average(g, e => e.Age) });

        AssertPerDepartment(s, groups.Select(g => (g.Key, (double)g.Avg)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GroupedWindow_AverageOfIntegerField_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var groups = await Employees(s, tree).GroupBy(e => e.Department).WithWindow(w => w.OrderBy(g => g.Key))
            .SelectAsync(g => new { Key = g.Key, Avg = Agg.Average(g, e => e.Age), Row = Win.RowNumber() });

        AssertPerDepartment(s, groups.Select(g => (g.Key, (double)g.Avg)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Window_AverageOfIntegerField_KeepsTheFraction(bool tree)
    {
        var s = await SeedAsync();

        var rows = await Employees(s, tree).WithWindow(w => w.PartitionBy(e => e.Department))
            .SelectAsync(e => new { e.Props.Department, Avg = Win.Avg(e.Props.Age) });

        rows.Should().HaveCount(s.Props.Count);
        AssertPerDepartment(s, rows.Select(r => (r.Department, (double)r.Avg)));
    }

    private static void AssertPerDepartment(Seeded s, IEnumerable<(string Department, double Avg)> actual)
    {
        var truth = s.Props.GroupBy(p => p.Department).ToDictionary(g => g.Key, g => g.Average(p => p.Age));
        var list = actual.ToList();
        list.Select(a => a.Department).Distinct().Should().BeEquivalentTo(truth.Keys);
        foreach (var (department, avg) in list)
            avg.Should().BeApproximately(truth[department], Precision, department);
    }
}
