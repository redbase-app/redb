using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The tree part of a query (WhereLevel, maxDepth, the tree filters, WhereHasAncestor / WhereHasDescendant) reaches every
/// analytic exit: SumAsync, AggregateAsync, CountAsync, AnyAsync, GroupBy, WithWindow, GroupBy.WithWindow, GroupByArray. The reference is the list of the same query: the analytics count the nodes the
/// list returns, so a level means the same thing everywhere (depth from the seed, the seed itself, maxDepth).
/// </summary>
public abstract class TreeLevelAnalyticsTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeLevelAnalyticsTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>A root, 3 children, 2 grandchildren under each, 1 great-grandchild under each grandchild.</summary>
    private async Task<(long Root, List<long> Children, string Tag)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "lv-" + Guid.NewGuid().ToString("N")[..8];
        var n = 0;
        async Task<TreeRedbObject<EmployeeProps>> Node(TreeRedbObject<EmployeeProps>? parent, int level)
        {
            n++;
            var p = TestDataFactory.CreateEmployee(n, firstName: $"{tag}-{n}", department: tag, salary: 1000m * n,
                position: level % 2 == 0 ? "Even" : "Odd").Props;
            var node = new TreeRedbObject<EmployeeProps> { name = $"{tag}-{n}", Props = p };
            if (parent == null) node.id = await Redb.SaveAsync(node);
            else node.id = await Redb.CreateChildAsync(node, parent);
            return node;
        }
        var root = await Node(null, 0);
        var children = new List<long>();
        for (var a = 0; a < 3; a++)
        {
            var child = await Node(root, 1);
            children.Add(child.id);
            for (var b = 0; b < 2; b++)
            {
                var grand = await Node(child, 2);
                await Node(grand, 3);
            }
        }
        return (root.id, children, tag);
    }

    /// <summary>The case and the number of nodes its list returns.</summary>
    private static readonly Dictionary<string, int> Expected = new()
    {
        ["level1"] = 3, ["level2plus"] = 12, ["depth2"] = 9, ["depth2-above0"] = 9, ["multi-depth1"] = 4, ["none"] = 0,
        ["children-depth2"] = 2, ["children-depth1"] = 2, ["leaves-depth3"] = 6, ["leaves-depth2"] = 0,
        ["descof-depth1"] = 2, ["descof-under-depth2"] = 4, ["ancestor-depth1"] = 12, ["descendant-depth1"] = 6,
        ["lv-children"] = 2, ["lv-descof"] = 4, ["lv-leaves"] = 6, ["lv-two-sets"] = 2, ["lv-multi"] = 8,
        ["lv-multi-nodepth"] = 8, ["lv-depth1"] = 3, ["lv-scheme"] = 15,
        ["multi-depth2-level2plus"] = 4, ["seed-level1"] = 2, ["seed-depth2-level2"] = 2
    };

    public static IEnumerable<object[]> Cases() => Expected.Keys.Select(c => new object[] { c });

    private IRedbQueryable<EmployeeProps> Source((long Root, List<long> Children, string Tag) s, string kind) => kind switch
    {
        "level1" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereLevel(1),
        "level2plus" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereLevel(l => l >= 2),
        "depth2" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 2),
        "depth2-above0" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 2).WhereLevel(l => l > 0),
        // Two of the children as roots, one level below them: their grandchildren of the seed tree.
        "multi-depth1" => Redb.TreeQuery<EmployeeProps>(s.Children.Take(2), maxDepth: 1),
        "none" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereLevel(9),
        // WhereChildrenOf and WhereDescendantsOf replace the start of the query; its maxDepth then counts from their
        // node. The children of the first child: 2, whatever the depth of TreeQuery(root).
        "children-depth2" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 2).WhereChildrenOf(s.Children[0]),
        "children-depth1" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 1).WhereChildrenOf(s.Children[0]),
        // A leaf is a node without children in the whole tree; the leaves of the seed tree lie at depth 3, so a walk
        // down to depth 2 has none.
        "leaves-depth3" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 3).WhereLeaves(),
        "leaves-depth2" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 2).WhereLeaves(),
        "descof-depth1" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereDescendantsOf(s.Children[0], maxDepth: 1),
        // Depth 2 from the first child: its grandchildren and great-grandchildren of the seed tree.
        "descof-under-depth2" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 2).WhereDescendantsOf(s.Children[0]),
        // Odd is levels 1 and 3. The matching ancestors are looked for down to depth 1 below the root (the children),
        // and everything below them is returned: grandchildren and great-grandchildren.
        "ancestor-depth1" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereHasAncestor<EmployeeProps>(a => a.Position == "Odd", maxDepth: 1),
        // A child at level 3: the grandchildren; without the depth also the children.
        "descendant-depth1" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereHasDescendant<EmployeeProps>(d => d.Position == "Odd", maxDepth: 1),
        // WhereLevel over every form of the tree source. l >= 1 keeps every node below the start in each of them.
        "lv-children" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereChildrenOf(s.Children[0]).WhereLevel(l => l >= 1),
        "lv-descof" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereDescendantsOf(s.Children[0]).WhereLevel(l => l >= 1),
        "lv-leaves" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereLeaves().WhereLevel(l => l >= 1),
        "lv-two-sets" => Redb.TreeQuery<EmployeeProps>(s.Root).WhereDescendantsOf(s.Children[0]).WhereLeaves().WhereLevel(l => l >= 1),
        "lv-multi" => Redb.TreeQuery<EmployeeProps>(s.Children.Take(2), maxDepth: 3).WhereLevel(l => l >= 1),
        "lv-multi-nodepth" => Redb.TreeQuery<EmployeeProps>(s.Children.Take(2)).WhereLevel(l => l >= 1),
        "lv-depth1" => Redb.TreeQuery<EmployeeProps>(s.Root, maxDepth: 1).WhereLevel(l => l >= 1),
        "lv-scheme" => Redb.TreeQuery<EmployeeProps>().Where(e => e.Department == s.Tag).WhereLevel(l => l >= 1),
        // Levels count from the node the query starts from (0), as maxDepth does: depth 2 below two children is their
        // great-grandchildren of the seed tree, level 1 below the first child its two grandchildren.
        "multi-depth2-level2plus" => Redb.TreeQuery<EmployeeProps>(s.Children.Take(2), maxDepth: 2).WhereLevel(l => l >= 2),
        "seed-level1" => Redb.TreeQuery<EmployeeProps>(s.Children[0]).WhereLevel(1),
        "seed-depth2-level2" => Redb.TreeQuery<EmployeeProps>(s.Children[0], maxDepth: 2).WhereLevel(2),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task EveryAnalyticExit_CountsTheNodesTheListReturns(string kind)
    {
        var seeded = await SeedAsync();
        var nodes = (await Source(seeded, kind).ToListAsync()).Select(o => o.Props).ToList();
        nodes.Should().HaveCount(Expected[kind], "precondition: the list of the level");

        using var scope = new AwesomeAssertions.Execution.AssertionScope();

        (await Source(seeded, kind).CountAsync()).Should().Be(nodes.Count, kind);
        (await Source(seeded, kind).AnyAsync()).Should().Be(nodes.Count > 0, kind);
        (await Source(seeded, kind).SumAsync(e => e.Salary)).Should().Be(nodes.Sum(e => e.Salary), kind);

        var batch = await Source(seeded, kind).AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary), Max = Agg.Max(x.Props.Salary) });
        batch.Count.Should().Be(nodes.Count, kind);
        batch.Sum.Should().Be(nodes.Sum(e => e.Salary), kind);
        batch.Max.Should().Be(nodes.Count > 0 ? nodes.Max(e => e.Salary) : 0m, kind);

        var groups = await Source(seeded, kind).GroupBy(e => e.Position)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, e => e.Salary) });
        groups.Select(g => (g.Key, g.Count, g.Sum)).Should().BeEquivalentTo(
            nodes.GroupBy(e => e.Position).Select(g => (g.Key, g.Count(), g.Sum(e => e.Salary))), kind);

        var rows = await Source(seeded, kind).WithWindow(w => w.OrderBy(e => e.Salary))
            .SelectAsync(e => new { e.Props.Salary, Row = Win.RowNumber() });
        rows.Select(r => r.Salary).Should().BeEquivalentTo(nodes.Select(e => e.Salary), kind);

        var ranked = await Source(seeded, kind).GroupBy(e => e.Position).WithWindow(w => w.OrderBy(g => g.Key))
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });
        ranked.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            nodes.GroupBy(e => e.Position).Select(g => (g.Key, g.Count())), kind);

        var elements = await Source(seeded, kind).GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) });
        elements.Select(g => (g.Key, g.Count)).Should().BeEquivalentTo(
            nodes.SelectMany(e => e.Contacts ?? []).GroupBy(c => c.Type).Select(g => (g.Key, g.Count())), kind);
    }
}
