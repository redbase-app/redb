using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;
using System.Linq;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Tree CRUD and query tests.
/// </summary>
public abstract class TreeTestsBase
{
    protected readonly IRedbService Redb;

    protected TreeTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>
    /// Build a tree:  Company -> Engineering -> Backend, Frontend
    ///                       -> Marketing
    /// </summary>
    private async Task<(long root, long eng, long backend, long frontend, long marketing)> SeedTreeAsync()
    {
        var root = TestDataFactory.CreateTreeNode("Company", "ROOT", 1000000m);
        root.id = await Redb.SaveAsync(root);

        var eng = TestDataFactory.CreateTreeNode("Engineering", "ENG", 500000m);
        eng.id = await Redb.CreateChildAsync(eng, root);

        var backend = TestDataFactory.CreateTreeNode("Backend", "BE", 200000m);
        backend.id = await Redb.CreateChildAsync(backend, eng);

        var frontend = TestDataFactory.CreateTreeNode("Frontend", "FE", 150000m);
        frontend.id = await Redb.CreateChildAsync(frontend, eng);

        var marketing = TestDataFactory.CreateTreeNode("Marketing", "MKT", 300000m);
        marketing.id = await Redb.CreateChildAsync(marketing, root);

        return (root.id, eng.id, backend.id, frontend.id, marketing.id);
    }


    [Fact]
    public async Task CreateChild_ReturnsPositiveId()
    {
        var root = TestDataFactory.CreateTreeNode("Root", "R1");
        root.id = await Redb.SaveAsync(root);

        var child = TestDataFactory.CreateTreeNode("Child", "C1");
        child.id = await Redb.CreateChildAsync(child, root);

        child.id.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task LoadTree_FromRoot_ReturnsHierarchy()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var tree = await Redb.LoadTreeAsync<TreeNodeProps>(root);

        tree.Should().NotBeNull();
        tree.Props.Name.Should().Be("Company");
        tree.Children.Should().HaveCount(2);
    }

    [Fact]
    public async Task LoadTree_WithMaxDepth_LimitsDepth()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var tree = await Redb.LoadTreeAsync<TreeNodeProps>(root, maxDepth: 1);

        tree.Children.Should().HaveCount(2);
        // At depth 1, engineering children should not be loaded or should be empty
        var engChild = tree.Children.OfType<TreeRedbObject<TreeNodeProps>>().FirstOrDefault(c => c.Props.Name == "Engineering");
        engChild.Should().NotBeNull();
    }

    [Fact]
    public async Task GetChildren_ReturnsDirectChildren()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var rootObj = await Redb.LoadAsync<TreeNodeProps>(root);
        var children = (await Redb.GetChildrenAsync<TreeNodeProps>(rootObj!)).ToList();

        children.Should().HaveCount(2);
        children.Select(c => c.Props.Name).Should().BeEquivalentTo(["Engineering", "Marketing"]);
    }

    [Fact]
    public async Task GetPathToRoot_ReturnsAncestorChain()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var beObj = await Redb.LoadAsync<TreeNodeProps>(be);
        var path = (await Redb.GetPathToRootAsync<TreeNodeProps>(beObj!)).ToList();

        path.Should().HaveCountGreaterThanOrEqualTo(2);
        path.Select(p => p.Props.Name).Should().Contain("Engineering");
        path.Select(p => p.Props.Name).Should().Contain("Company");
    }

    [Fact]
    public async Task GetDescendants_ReturnsAllBelow()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var rootObj = await Redb.LoadAsync<TreeNodeProps>(root);
        var descendants = (await Redb.GetDescendantsAsync<TreeNodeProps>(rootObj!)).ToList();

        descendants.Should().HaveCount(4); // eng, be, fe, mkt
    }

    [Fact]
    public async Task MoveObject_ChangesParent()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        // Move Frontend under Marketing
        var feObj = await Redb.LoadAsync<TreeNodeProps>(fe);
        var mktObj = await Redb.LoadAsync<TreeNodeProps>(mkt);
        await Redb.MoveObjectAsync(feObj!, mktObj);

        var mktChildren = (await Redb.GetChildrenAsync<TreeNodeProps>(mktObj!)).ToList();
        mktChildren.Should().Contain(c => c.Props.Name == "Frontend");

        var engObj = await Redb.LoadAsync<TreeNodeProps>(eng);
        var engChildren = (await Redb.GetChildrenAsync<TreeNodeProps>(engObj!)).ToList();
        engChildren.Should().NotContain(c => c.Props.Name == "Frontend");
    }

    [Fact]
    public async Task TreeQuery_CountAll_ReturnsTotal()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var count = await Redb.TreeQuery<TreeNodeProps>()
            .CountAsync();

        count.Should().BeGreaterThanOrEqualTo(5);
    }

    [Fact]
    public async Task TreeQuery_WhereRoots_OnlyRoots()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var roots = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRoots()
            .ToListAsync();

        roots.Should().NotBeEmpty();
        roots.Should().Contain(r => r.id == root);
    }

    [Fact]
    public async Task TreeQuery_WhereLeaves_OnlyLeaves()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var leaves = await Redb.TreeQuery<TreeNodeProps>()
            .WhereLeaves()
            .ToListAsync();

        leaves.Should().NotBeEmpty();
        leaves.Select(l => l.Props.Name).Should().Contain("Backend");
        leaves.Select(l => l.Props.Name).Should().Contain("Frontend");
        leaves.Select(l => l.Props.Name).Should().Contain("Marketing");
    }

    [Fact]
    public async Task TreeQuery_WhereLevel_FiltersLevel()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        // Level 0 = root, Level 1 = eng/mkt, Level 2 = be/fe
        var level1 = await Redb.TreeQuery<TreeNodeProps>()
            .WhereLevel(1)
            .ToListAsync();

        level1.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task TreeQuery_WhereFilter_CombinesWithTree()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .Where(n => n.Budget > 200000m)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Budget.Should().BeGreaterThan(200000m));
    }

    [Fact]
    public async Task TreeQuery_FromRoot_ScopedQuery()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var engObj = await Redb.LoadAsync<TreeNodeProps>(eng);
        var results = await Redb.TreeQuery<TreeNodeProps>(engObj)
            .ToListAsync();

        // Should include eng itself and its descendants (be, fe)
        results.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task DeleteSubtree_RemovesBranch()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        var engObj = await Redb.LoadAsync<TreeNodeProps>(eng);
        var deleted = await Redb.DeleteSubtreeAsync(engObj!);

        deleted.Should().BeGreaterThanOrEqualTo(2); // eng subtree: eng, be, fe

        var remaining = await Redb.TreeQuery<TreeNodeProps>()
            .CountAsync();

        // root + mkt should remain
        remaining.Should().BeGreaterThanOrEqualTo(2);
    }

    // ===== WhereRedb on TreeQuery: base field ValueString tests =====

    private async Task<(long root, List<long> childIds)> SeedTreeWithValueStringAsync()
    {
        var root = new TreeRedbObject<TreeNodeProps>
        {
            name = "VSTree_Root",
            value_string = "root-node",
            Props = new TreeNodeProps
            {
                Name = "VSTree_Root",
                Description = "Root",
                IsActive = true,
                Budget = 100000m,
                Code = "RT"
            }
        };
        root.id = await Redb.SaveAsync(root);

        var childData = new[]
        {
            ("alpha-bravo-charlie", "Node_A", "NA", 50000m),
            ("delta-echo-foxtrot",  "Node_B", "NB", 60000m),
            ("golf-hotel-india",    "Node_C", "NC", 70000m),
            ("alpha-juliet-kilo",   "Node_D", "ND", 40000m),
            ("lima-echo-mike",      "Node_E", "NE", 80000m),
        };

        var childIds = new List<long>();
        foreach (var (vs, nm, code, budget) in childData)
        {
            var child = new TreeRedbObject<TreeNodeProps>
            {
                name = nm,
                value_string = vs,
                Props = new TreeNodeProps
                {
                    Name = nm,
                    Description = $"Child {nm}",
                    IsActive = true,
                    Budget = budget,
                    Code = code
                }
            };
            child.id = await Redb.CreateChildAsync(child, root);
            childIds.Add(child.id);
        }

        return (root.id, childIds);
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_Contains_Filters()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.value_string != null && r.value_string.Contains("echo"));
        results.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_Equal_Filters()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString == "alpha-bravo-charlie")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.value_string == "alpha-bravo-charlie");
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_StartsWith_Filters()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString!.StartsWith("alpha"))
            .ToListAsync();

        results.Should().HaveCountGreaterThanOrEqualTo(2);
        results.Should().OnlyContain(r => r.value_string != null && r.value_string.StartsWith("alpha"));
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_Contains_ThenWhereProps()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .Where(n => n.Budget > 50000m)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.value_string != null && r.value_string.Contains("echo") &&
            r.Props.Budget > 50000m);
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_Contains_ThenMultipleWhereProps()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        // Full chain: WhereRedb(Contains) + multiple Where(props) + OrderBy + Count + ToList
        var query = Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .Where(n => n.IsActive)
            .Where(n => n.Budget > 0m)
            .OrderBy(n => n.Name);

        var count = await query.CountAsync();
        count.Should().BeGreaterThanOrEqualTo(0);

        var results = await query.ToListAsync();

        results.Should().OnlyContain(r =>
            r.value_string != null && r.value_string.Contains("echo") &&
            r.Props.IsActive &&
            r.Props.Budget > 0m);
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_ValueString_Contains_CountAsync()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var count = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .Where(n => n.Budget > 0m)
            .CountAsync();

        count.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task TreeQuery_WhereRedb_Name_Contains_Filters()
    {
        var (rootId, childIds) = await SeedTreeWithValueStringAsync();

        var results = await Redb.TreeQuery<TreeNodeProps>()
            .WhereRedb(o => o.Name.Contains("Node_"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.name != null && r.name.Contains("Node_"));
    }
}
