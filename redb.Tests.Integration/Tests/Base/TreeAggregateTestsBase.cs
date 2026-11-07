using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// An aggregate over a tree query ranges over the subtree of that query, not over the scheme.
///
/// SumAsync, AverageAsync, MinAsync, MaxAsync, GetStatisticsAsync, AggregateAsync and the *RedbAsync
/// twins of a tree query used to be answered by the flat provider, which knows the scheme and the
/// filter but not the root: a sum under one manager came back as the sum over every employee in
/// the database. GroupBy and windows over the same tree were right, so the same Where gave
/// different object sets depending on the terminal operation.
///
/// The scheme holds employees seeded by other tests, which is what makes these facts meaningful:
/// a whole-scheme answer cannot match the four children seeded here.
/// </summary>
public abstract class TreeAggregateTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeAggregateTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>A root with four children earning 1000, 2000, 3000 and 4000; two of them know "tree-skill".</summary>
    private async Task<long> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];

        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"agg-root-{tag}",
            Props = TestDataFactory.CreateEmployee(0, department: $"Root-{tag}", salary: 50_000m).Props
        };
        root.id = await Redb.SaveAsync(root);

        for (var i = 1; i <= 4; i++)
        {
            var props = TestDataFactory.CreateEmployee(i, department: $"Dept-{tag}", salary: 1000m * i).Props;
            props.Skills = i % 2 == 0 ? ["tree-skill", "other"] : ["other"];
            await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"agg-child-{tag}-{i}", Props = props }, root);
        }

        return root.id;
    }

    [Fact]
    public async Task SingleAggregates_RangeOverTheSubtree()
    {
        var rootId = await SeedAsync();
        IRedbQueryable<EmployeeProps> Tree() => Redb.TreeQuery<EmployeeProps>(rootId);

        (await Tree().CountAsync()).Should().Be(4, "precondition: the subtree is the four children");

        (await Tree().SumAsync(e => e.Salary)).Should().Be(10_000m);
        (await Tree().AverageAsync(e => e.Salary)).Should().Be(2_500m);
        (await Tree().MinAsync(e => e.Salary)).Should().Be(1_000m);
        (await Tree().MaxAsync(e => e.Salary)).Should().Be(4_000m);
    }

    [Fact]
    public async Task SingleAggregates_HonourTheFilterInsideTheSubtree()
    {
        var rootId = await SeedAsync();
        IRedbQueryable<EmployeeProps> Tree() => Redb.TreeQuery<EmployeeProps>(rootId).Where(e => e.Salary > 1_500m);

        (await Tree().SumAsync(e => e.Salary)).Should().Be(9_000m);
        (await Tree().MinAsync(e => e.Salary)).Should().Be(2_000m);
    }

    [Fact]
    public async Task BatchAggregates_RangeOverTheSubtree()
    {
        var rootId = await SeedAsync();

        var all = await Redb.TreeQuery<EmployeeProps>(rootId)
            .AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary), Max = Agg.Max(x.Props.Salary) });
        all.Count.Should().Be(4);
        all.Sum.Should().Be(10_000m);
        all.Max.Should().Be(4_000m);

        var filtered = await Redb.TreeQuery<EmployeeProps>(rootId)
            .Where(e => e.Salary > 1_500m)
            .AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary) });
        filtered.Count.Should().Be(3);
        filtered.Sum.Should().Be(9_000m);

        var statistics = await Redb.TreeQuery<EmployeeProps>(rootId).GetStatisticsAsync(e => e.Salary);
        statistics.Count.Should().Be(4);
        statistics.Sum.Should().Be(10_000m);
        statistics.Min.Should().Be(1_000m);
        statistics.Max.Should().Be(4_000m);
    }

    [Fact]
    public async Task BaseFieldAggregates_RangeOverTheSubtree()
    {
        var rootId = await SeedAsync();
        var ids = (await Redb.TreeQuery<EmployeeProps>(rootId).ToListAsync()).Select(o => o.Id).ToList();
        ids.Should().HaveCount(4);

        (await Redb.TreeQuery<EmployeeProps>(rootId).SumRedbAsync(o => o.Id)).Should().Be(ids.Sum());

        var batch = await Redb.TreeQuery<EmployeeProps>(rootId)
            .AggregateRedbAsync(o => new { Count = Agg.Count(), MaxId = Agg.Max(o.Id) });
        batch.Count.Should().Be(4);
        batch.MaxId.Should().Be(ids.Max());
    }

    [Fact]
    public async Task ArrayContains_FiltersAndAggregatesInsideTheSubtree()
    {
        var rootId = await SeedAsync();
        IRedbQueryable<EmployeeProps> Tree() => Redb.TreeQuery<EmployeeProps>(rootId).Where(e => e.Skills!.Contains("tree-skill"));

        (await Tree().CountAsync()).Should().Be(2, "children 2 and 4 carry the skill");
        (await Tree().ToListAsync()).Should().HaveCount(2);
        (await Tree().SumAsync(e => e.Salary)).Should().Be(6_000m);

        var batch = await Tree().AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary) });
        batch.Count.Should().Be(2);
        batch.Sum.Should().Be(6_000m);
    }
}
