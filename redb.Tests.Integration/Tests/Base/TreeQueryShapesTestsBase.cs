using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Tree queries and groupings that returned a plausible wrong answer, or none, where the flat twin was right
/// (review 2026-09-24: FRM-2, TQP-1, GRP-9, TGR-1, TQB-10). Each fact compares with what the query must return,
/// not with what the engine happens to produce.
/// </summary>
public abstract class TreeQueryShapesTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeQueryShapesTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>A root employee with four children in two departments; every employee carries contacts.</summary>
    private async Task<long> SeedEmployeeTreeAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<EmployeeProps>
        {
            name = $"tree-root-{tag}",
            Props = TestDataFactory.CreateEmployee(0, department: $"Root-{tag}", salary: 5000m).Props
        };
        root.id = await Redb.SaveAsync(root);
        for (var i = 1; i <= 4; i++)
        {
            var child = new TreeRedbObject<EmployeeProps>
            {
                name = $"tree-child-{tag}-{i}",
                Props = TestDataFactory.CreateEmployee(i, department: i % 2 == 0 ? $"A-{tag}" : $"B-{tag}", salary: 1000m * i).Props
            };
            await Redb.CreateChildAsync(child, root);
        }
        return root.id;
    }

    [Fact]
    public async Task TreeWindow_WithAFrame_SumsTheWholeFrame()
    {
        // FRM-2: the Pro tree window read the frame as three strings while the core writes objects, found nothing, and
        // dropped the frame without a word - a sum over the whole tree came back as a running total.
        var rootId = await SeedEmployeeTreeAsync();

        var rows = await Redb.TreeQuery<EmployeeProps>(rootId)
            .WithWindow(w => w
                .OrderBy(e => e.Salary)
                .Frame(Frame.Rows().UnboundedPreceding().AndUnboundedFollowing()))
            .SelectAsync(e => new { e.Props.Salary, Total = Win.Sum(e.Props.Salary) });

        rows.Should().HaveCountGreaterThan(1, "precondition: the tree holds several employees");
        var total = rows.Sum(r => r.Salary);
        rows.Should().AllSatisfy(r => r.Total.Should().BeApproximately(total, 0.01m,
            "a frame over the whole tree sums every salary in it"));
    }

    [Fact]
    public async Task TreeArrayGrouping_Having_WithoutWhere_FiltersTheGroups()
    {
        // TQP-1: without a Where the tree took a path that dropped the HAVING - every group came back.
        var rootId = await SeedEmployeeTreeAsync();

        var rows = await Redb.TreeQuery<EmployeeProps>(rootId)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 1_000_000)
            .SelectAsync(g => new { Type = g.Key, Count = Agg.Count(g) });

        rows.Should().BeEmpty("no contact type is that frequent, so HAVING leaves no group");
    }

    [Fact]
    public async Task TreeGrouping_CompositeKey_WithAMemberItCannotRead_IsRefused()
    {
        // GRP-9: a key member the parser did not understand was skipped without a word, and the grouping ran on
        // fewer fields - other groups, other totals, no error.
        var rootId = await SeedEmployeeTreeAsync();

        var grouping = async () => await Redb.TreeQuery<EmployeeProps>(rootId)
            .GroupBy(e => new { e.Department, Initial = e.FirstName.Substring(0, 1) })
            .SelectAsync(g => new { g.Key.Department, Count = Agg.Count(g) });

        await grouping.Should().ThrowAsync<NotSupportedException>().WithMessage("*Initial*");
    }

    [Fact]
    public async Task ArrayGrouping_CompositeKey_WithAMemberItCannotRead_IsRefused()
    {
        // GRP-9, the array grouping's copy of the same parser.
        await SeedEmployeeTreeAsync();

        var grouping = async () => await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, Length = c.Value.Length })
            .SelectAsync(g => new { g.Key.Type, Count = Agg.Count(g) });

        await grouping.Should().ThrowAsync<NotSupportedException>().WithMessage("*Length*");
    }

    [Fact]
    public async Task TreeGrouping_Having_DoesNotLeakIntoASiblingQuery()
    {
        // TGR-1: the tree grouping's Having changed the builder itself, so a second query branched from the same
        // grouping inherited the first one's condition. The flat grouping was fixed as G-4.
        var rootId = await SeedEmployeeTreeAsync();
        var grouped = Redb.TreeQuery<EmployeeProps>(rootId).GroupBy(e => e.Department);
        var strict = grouped.Having(g => Agg.Count(g) > 1_000_000);
        var loose = grouped.Having(g => Agg.Count(g) > 0);

        var looseRows = await loose.SelectAsync(g => new { Department = g.Key, Count = Agg.Count(g) });
        looseRows.Should().NotBeEmpty("the sibling's condition must not leak into this query");

        var strictRows = await strict.SelectAsync(g => new { Department = g.Key, Count = Agg.Count(g) });
        strictRows.Should().BeEmpty();
    }

    private IRedbProjectedQueryable<EmployeeRow> ProjectTree(long rootId)
        => ((TreeQueryableBase<EmployeeProps>)Redb.TreeQuery<EmployeeProps>(rootId))
            .Select(e => new EmployeeRow(e.Props.Salary, e.Props.Department));

    public sealed record EmployeeRow(decimal Salary, string Department);

    [Fact]
    public async Task TreeProjection_TakeAfterOrderBy_TakesFromTheSortedRows()
    {
        // TPQ-1: OrderBy ran in memory after the projection, Take went to SQL before it - the page was cut from
        // the unsorted rows (the first child, salary 1000) and then sorted.
        var rootId = await SeedEmployeeTreeAsync();

        var top = await ProjectTree(rootId).OrderByDescending(x => x.Salary).Take(1).ToListAsync();

        top.Select(x => x.Salary).Should().Equal(4000m);
    }

    [Fact]
    public async Task TreeProjection_TakeAfterWhere_TakesFromTheFilteredRows()
    {
        // TPQ-1: the filter ran on the page SQL had already cut.
        var rootId = await SeedEmployeeTreeAsync();

        var rows = await ProjectTree(rootId).Where(x => x.Salary > 2500m).Take(1).ToListAsync();

        rows.Should().ContainSingle().Which.Salary.Should().BeGreaterThan(2500m);
    }

    [Fact]
    public async Task TreeProjection_Distinct_IsOverTheProjectedRows()
    {
        // TPQ-1: Distinct went to the source query, where every node is distinct, so projected duplicates stayed.
        var rootId = await SeedEmployeeTreeAsync();

        var departments = await ((TreeQueryableBase<EmployeeProps>)Redb.TreeQuery<EmployeeProps>(rootId))
            .Select(e => e.Props.Department).Distinct().ToListAsync();

        departments.Should().OnlyHaveUniqueItems().And.HaveCount(2);
    }

    [Fact]
    public async Task TreePropsDepth_SurvivesWithMaxDepth()
    {
        // TQB-10: WithMaxDepth rebuilt the tree context by hand and dropped eight of its settings - the props depth,
        // the lazy references, the projection and the distinct among them - so the query quietly lost them.
        await Redb.SyncSchemeAsync<LazyNodeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<LazyNodeProps> { name = $"depth-root-{tag}", Props = new LazyNodeProps { Label = "root" } };
        root.id = await Redb.SaveAsync(root);
        for (var i = 0; i < 3; i++)
        {
            var leafId = await Redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"depth-leaf-{tag}-{i}", Props = new LazyNodeProps { Label = $"leaf-{i}" } });
            var child = new TreeRedbObject<LazyNodeProps>
            {
                name = $"depth-child-{tag}-{i}",
                Props = new LazyNodeProps { Label = $"child-{i}", Next = new RedbObject<LazyNodeProps> { id = leafId } }
            };
            await Redb.CreateChildAsync(child, root);
        }

        var shallow = await Redb.TreeQuery<LazyNodeProps>(root.id).WithPropsDepth(1).ToListAsync();
        var shallowWithDepth = await Redb.TreeQuery<LazyNodeProps>(root.id).WithPropsDepth(1).WithMaxDepth(5).ToListAsync();

        var children = shallow.Where(o => o.Props?.Next != null).ToList();
        children.Should().NotBeEmpty("precondition: the children reference a leaf each");
        children.Should().OnlyContain(o => !o.Props.Next!.IsPropsLoaded, "precondition: props depth 1 leaves references as stubs");
        shallowWithDepth.Where(o => o.Props?.Next != null).Should().OnlyContain(o => !o.Props.Next!.IsPropsLoaded,
            "a depth limit on the tree does not undo the props depth set before it");
    }
}
