using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Enums;
using redb.Core.Models.Users;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The tree services that read a subtree or a chain of ancestors in one statement (TreeProviderBase): the same nodes in
/// the same order as the walk one parent at a time, a cycle in _id_parent as an error, and the permission check of every
/// node with its subtree left out under a refused one.
/// </summary>
public abstract class ProTreeTestsBase : TreeTestsBase
{
    protected ProTreeTestsBase(IRedbService redb) : base(redb) { }

    /// <summary>
    /// root -> c, a, b (saved in that order, so the order of the children is by name, not by id), each with two
    /// children saved z then y.
    /// </summary>
    private async Task<TreeRedbObject<TreeNodeProps>> SeedUnsortedTreeAsync(string tag)
    {
        var root = TestDataFactory.CreateTreeNode($"walk-{tag}", $"W-{tag}");
        root.id = await Redb.SaveAsync(root);
        foreach (var name in new[] { "c", "a", "b" })
        {
            var child = TestDataFactory.CreateTreeNode($"{name}-{tag}", $"{name}-{tag}");
            child.id = await Redb.CreateChildAsync(child, root);
            foreach (var leaf in new[] { "z", "y" })
                await Redb.CreateChildAsync(TestDataFactory.CreateTreeNode($"{name}{leaf}-{tag}", $"{name}{leaf}-{tag}"), child);
        }
        return root;
    }

    /// <summary>The walk one parent at a time, through GetChildrenAsync: a child, everything below it, the next child.</summary>
    private async Task<List<long>> WalkByChildrenAsync(IRedbObject parent)
    {
        var ids = new List<long>();
        foreach (var child in await Redb.GetChildrenAsync<TreeNodeProps>(parent))
        {
            ids.Add(child.id);
            ids.AddRange(await WalkByChildrenAsync(child));
        }
        return ids;
    }

    [Fact]
    public async Task Descendants_AndLoadTree_AreTheWalkByChildren()
    {
        var root = await SeedUnsortedTreeAsync(Guid.NewGuid().ToString("N")[..8]);
        var expected = await WalkByChildrenAsync(root);
        expected.Should().HaveCount(9);

        (await Redb.GetDescendantsAsync<TreeNodeProps>(root)).Select(d => d.id).Should().Equal(expected);

        var tree = await Redb.LoadTreeAsync<TreeNodeProps>(root);
        IEnumerable<long> Flatten(TreeRedbObject<TreeNodeProps> node) =>
            node.Children.Cast<TreeRedbObject<TreeNodeProps>>().SelectMany(c => Flatten(c).Prepend(c.id));
        Flatten(tree).Should().Equal(expected);
        tree.Children.Cast<TreeRedbObject<TreeNodeProps>>().Should().AllSatisfy(c => c.Parent.Should().BeSameAs(tree));

        var depthOne = await Redb.LoadTreeAsync<TreeNodeProps>(root, maxDepth: 1);
        depthOne.Children.Should().HaveCount(3);
        depthOne.Children.Should().AllSatisfy(c => c.Children.Should().BeEmpty());

        var polymorphic = await Redb.GetPolymorphicDescendantsAsync(root);
        polymorphic.Select(d => d.Id).Should().Equal(expected);
    }

    [Fact]
    public async Task Window_TakesSkipAndOrderOfTheQuery_ShownFieldsBelongToTheirRows()
    {
        // The window over a tree used to ignore Take, Skip and OrderBy and return every node. Now the rows are the
        // page of the query, in its order; the window values are those of the whole filtered subtree.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = TestDataFactory.CreateTreeNode($"win-{tag}", $"WIN-{tag}");
        root.id = await Redb.SaveAsync(root);
        var nodes = new List<TreeRedbObject<TreeNodeProps>>();
        for (var i = 0; i < 12; i++)
        {
            var node = TestDataFactory.CreateTreeNode($"win-{tag}-{i:D2}", $"C{i:D2}-{tag}", 1000m * ((i * 7) % 12 + 1));
            node.id = await Redb.CreateChildAsync(node, i < 6 || nodes.Count == 0 ? root : nodes[i % 6]);
            nodes.Add(node);
        }
        var set = nodes.Where(n => n.Props.Budget > 2000m).ToList();

        var rows = await Redb.TreeQuery<TreeNodeProps>(root.id)
            .Where(n => n.Budget > 2000m)
            .OrderByDescending(n => n.Budget)
            .Skip(2).Take(4)
            .WithWindow(w => w.OrderByDesc(n => n.Budget))
            .SelectAsync(n => new { n.Id, n.Props.Name, n.Props.Code, n.Props.Budget, Rank = Win.Rank() });

        var expected = set.OrderByDescending(n => n.Props.Budget).Skip(2).Take(4).ToList();
        rows.Select(r => r.Budget).Should().Equal(expected.Select(n => n.Props.Budget), "the page of the ordering");
        foreach (var r in rows)
        {
            var node = nodes.Single(n => n.id == r.Id);
            r.Name.Should().Be(node.Props.Name);
            r.Code.Should().Be(node.Props.Code);
            r.Rank.Should().Be(1 + set.Count(n => n.Props.Budget > node.Props.Budget), "ranked over the whole filtered subtree");
        }

        var buckets = await Redb.TreeQuery<TreeNodeProps>(root.id).Take(9)
            .WithWindow(w => w.OrderByDesc(n => n.Budget))
            .SelectAsync(n => new { n.Id, Bucket = Win.Ntile(3) });
        buckets.Should().HaveCount(9);
        buckets.Select(b => b.Bucket).Distinct().Should().BeEquivalentTo(new[] { 1, 2, 3 }, "Win.Ntile(3) gives three buckets");
    }

    [Fact]
    public async Task Any_StopsAtTheFirstNode_AndAgreesWithCount()
    {
        // Any over a tree asks for one node instead of counting the subtree; the answer must stay the count above zero,
        // whatever the filter, ordering, distinct or page of the query.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = TestDataFactory.CreateTreeNode($"any-{tag}", $"ANY-{tag}");
        root.id = await Redb.SaveAsync(root);
        var nodes = new List<TreeRedbObject<TreeNodeProps>>();
        for (var i = 0; i < 12; i++)
        {
            var node = TestDataFactory.CreateTreeNode($"any-{tag}-{i:D2}", $"C{i:D2}-{tag}", 1000m * ((i * 7) % 12 + 1));
            node.id = await Redb.CreateChildAsync(node, i < 6 || nodes.Count == 0 ? root : nodes[i % 6]);
            nodes.Add(node);
        }
        var leaf = nodes[^1];

        var queries = new (string Name, IRedbQueryable<TreeNodeProps> Query, bool Expected)[]
        {
            ("whole subtree", Redb.TreeQuery<TreeNodeProps>(root.id), true),
            ("a filter some nodes pass", Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Budget > 10000m), true),
            ("a filter no node passes", Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Budget > 1000000m), false),
            ("ordered", Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Budget > 2000m).OrderByDescending(n => n.Budget), true),
            ("distinct by a field", Redb.TreeQuery<TreeNodeProps>(root.id).DistinctBy(n => n.Budget), true),
            ("ordered, no node passes", Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Budget < 0m).OrderBy(n => n.Code), false),
            ("below a leaf", Redb.TreeQuery<TreeNodeProps>(leaf.id), false),
            ("skip to the last node", Redb.TreeQuery<TreeNodeProps>(root.id).Skip(nodes.Count - 1), true),
            ("skip past the last node", Redb.TreeQuery<TreeNodeProps>(root.id).Skip(nodes.Count), false),
            ("take none", Redb.TreeQuery<TreeNodeProps>(root.id).Take(0), false),
        };
        foreach (var (name, query, expected) in queries)
        {
            (await query.AnyAsync()).Should().Be(expected, name);
            (await query.CountAsync() > 0).Should().Be(expected, $"{name}: the count agrees");
        }

        (await Redb.TreeQuery<TreeNodeProps>(root.id).AnyAsync(n => n.Code == $"C05-{tag}")).Should().BeTrue("a predicate one node passes");
        (await Redb.TreeQuery<TreeNodeProps>(root.id).AnyAsync(n => n.Code == $"none-{tag}")).Should().BeFalse("a predicate no node passes");
    }

    [Fact]
    public async Task TreeFilters_Combined_AllApply_InAnyOrder()
    {
        // root -> a, b; a -> a1, a2; a2 -> a21. Leaves: a1, a21, b.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var nodes = new Dictionary<string, TreeRedbObject<TreeNodeProps>>();
        async Task<long> Add(string name, string? parent)
        {
            var node = TestDataFactory.CreateTreeNode($"{name}-{tag}", $"{name}-{tag}");
            node.id = parent == null ? await Redb.SaveAsync(node) : await Redb.CreateChildAsync(node, nodes[parent]);
            nodes[name] = node;
            return node.id;
        }
        var root = await Add("root", null);
        var a = await Add("a", "root");
        var b = await Add("b", "root");
        var a1 = await Add("a1", "a");
        var a2 = await Add("a2", "a");
        var a21 = await Add("a21", "a2");

        async Task<long[]> Ids(Func<IRedbQueryable<TreeNodeProps>, IRedbQueryable<TreeNodeProps>> shape) =>
            (await shape(Redb.TreeQuery<TreeNodeProps>(root)).ToListAsync()).Select(n => n.id).OrderBy(i => i).ToArray();
        long[] Set(params long[] ids) => ids.OrderBy(i => i).ToArray();

        // Each alone.
        (await Ids(q => q.WhereChildrenOf(a))).Should().Equal(Set(a1, a2), "children of a");
        (await Ids(q => q.WhereLeaves())).Should().Equal(Set(a1, a21, b), "leaves of the subtree");
        (await Ids(q => q.WhereDescendantsOf(a))).Should().Equal(Set(a1, a2, a21), "descendants of a");

        // Together: every filter applies, whichever comes first.
        (await Ids(q => q.WhereChildrenOf(a).WhereLeaves())).Should().Equal(Set(a1), "children of a that are leaves");
        (await Ids(q => q.WhereLeaves().WhereChildrenOf(a))).Should().Equal(Set(a1), "leaves that are children of a");
        (await Ids(q => q.WhereDescendantsOf(a).WhereLeaves())).Should().Equal(Set(a1, a21), "descendants of a that are leaves");
        (await Ids(q => q.WhereLeaves().WhereDescendantsOf(a))).Should().Equal(Set(a1, a21), "leaves below a");
        (await Ids(q => q.WhereChildrenOf(a).WhereChildrenOf(b))).Should().BeEmpty("no node is a child of both");
        (await Ids(q => q.WhereDescendantsOf(a2).WhereChildrenOf(a))).Should().BeEmpty("a21 is below a2 but not a child of a");
    }

    [Fact]
    public async Task DistinctBy_WithFilterOrderAndPage_OneNodePerKey()
    {
        // DistinctBy over a subtree on a Props field, with a filter, an ordering by the key and a page. With the
        // planner on SQL Server reads it over the streams, SQLite over a pivot the nodes drive.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = TestDataFactory.CreateTreeNode($"dist-{tag}", $"DIST-{tag}");
        root.id = await Redb.SaveAsync(root);
        var nodes = new List<TreeRedbObject<TreeNodeProps>>();
        for (var i = 0; i < 12; i++)
        {
            var node = TestDataFactory.CreateTreeNode($"dist-{tag}-{i:D2}", $"D{i:D2}-{tag}", 1000m * (i % 4 + 1));
            node.id = await Redb.CreateChildAsync(node, i < 4 ? root : nodes[i % 4]);
            nodes.Add(node);
        }
        IRedbQueryable<TreeNodeProps> Q() => Redb.TreeQuery<TreeNodeProps>(root.id);

        var filtered = await Q().Where(n => n.Budget > 1000m).DistinctBy(n => n.Budget).ToListAsync();
        filtered.Select(n => n.Props.Budget).Should().OnlyHaveUniqueItems().And.BeEquivalentTo(new[] { 2000m, 3000m, 4000m });
        filtered.Should().AllSatisfy(n => nodes.Single(x => x.id == n.id).Props.Budget.Should().Be(n.Props.Budget));

        (await Q().Where(n => n.Budget > 1000m).OrderByDescending(n => n.Budget).DistinctBy(n => n.Budget).ToListAsync())
            .Select(n => n.Props.Budget).Should().Equal(4000m, 3000m, 2000m);
        (await Q().OrderBy(n => n.Budget).DistinctBy(n => n.Budget).Take(2).ToListAsync())
            .Select(n => n.Props.Budget).Should().Equal(1000m, 2000m);
        (await Q().Where(n => n.Budget > 1000m).DistinctBy(n => n.Budget).CountAsync()).Should().Be(3);
    }

    [Fact]
    public async Task ParentChains_OfTreeListRootListAndLoadWithParents_ReachTheRoot_DeeperThan100Levels()
    {
        // ToTreeListAsync, ToRootListAsync and LoadWithParentsAsync read the ancestors of the objects in one statement.
        // A chain of 120 levels: SQL Server stops a recursive statement at 100 unless told otherwise.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = TestDataFactory.CreateTreeNode($"chain-{tag}", $"CHAIN-{tag}");
        root.id = await Redb.SaveAsync(root);
        var chain = new List<long> { root.id };
        var parent = root;
        for (var i = 1; i <= 120; i++)
        {
            var node = TestDataFactory.CreateTreeNode($"chain-{tag}-{i:D3}", $"L{i:D3}-{tag}");
            node.id = await Redb.CreateChildAsync(node, parent);
            chain.Add(node.id);
            parent = node;
        }

        List<long> Up(ITreeRedbObject node)
        {
            var ids = new List<long>();
            for (var current = node; current != null; current = current.Parent)
                ids.Add(current.Id);
            return ids;
        }
        IEnumerable<long> Expected(int level) => Enumerable.Range(0, level + 1).Select(l => chain[level - l]);

        var deep = (await Redb.LoadWithParentsAsync<TreeNodeProps>(new[] { chain[120], chain[60] })).ToDictionary(n => n.id);
        deep.Keys.Should().BeEquivalentTo(new[] { chain[120], chain[60] });
        Up(deep[chain[120]]).Should().Equal(Expected(120), "the whole chain of 120 levels up to the root");
        Up(deep[chain[60]]).Should().Equal(Expected(60));

        var picked = await Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Code == $"L003-{tag}" || n.Code == $"L005-{tag}").ToTreeListAsync();
        picked.Select(n => n.id).Should().BeEquivalentTo(new[] { chain[3], chain[5] });
        foreach (var node in picked)
            Up(node).Should().Equal(Expected(chain.IndexOf(node.id)));

        var roots = await Redb.TreeQuery<TreeNodeProps>(root.id).Where(n => n.Code == $"L004-{tag}").ToRootListAsync();
        roots.Select(r => r.Id).Should().Equal(root.id);
    }

    [Fact]
    public async Task CycleInParents_IsAnError_NotAHang()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var a = TestDataFactory.CreateTreeNode($"cycle-a-{tag}", $"CA-{tag}");
        a.id = await Redb.SaveAsync(a);
        var b = TestDataFactory.CreateTreeNode($"cycle-b-{tag}", $"CB-{tag}");
        b.id = await Redb.CreateChildAsync(b, a);
        var c = TestDataFactory.CreateTreeNode($"cycle-c-{tag}", $"CC-{tag}");
        c.id = await Redb.CreateChildAsync(c, b);

        // a -> b -> c -> a: written past the services, which refuse it.
        await Redb.Context.ExecuteAsync($"UPDATE _objects SET _id_parent = {c.id} WHERE _id = {a.id}");
        try
        {
            var descendants = () => Redb.GetDescendantsAsync<TreeNodeProps>(a);
            (await descendants.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("Cyclic");

            var path = () => Redb.GetPathToRootAsync<TreeNodeProps>(b);
            (await path.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("Cyclic");

            var delete = () => Redb.DeleteSubtreeAsync(a);
            (await delete.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("Cyclic");
        }
        finally
        {
            await Redb.Context.ExecuteAsync($"UPDATE _objects SET _id_parent = NULL WHERE _id = {a.id}");
        }

        (await Redb.DeleteSubtreeAsync(a)).Should().Be(3);
    }

    [Fact]
    public async Task MoveUnderOwnDescendant_IsRefused()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var root = await SeedUnsortedTreeAsync(tag);
        var grandchild = (await Redb.GetDescendantsAsync<TreeNodeProps>(root)).Last();

        var move = () => Redb.MoveObjectAsync(root, grandchild);
        (await move.Should().ThrowAsync<InvalidOperationException>()).Which.Message.Should().Contain("cyclic");
        (await Redb.GetPathToRootAsync<TreeNodeProps>(grandchild)).First().id.Should().Be(root.id, "nothing moved");
    }

    [Fact]
    public async Task Permissions_ARefusedBranchIsLeftOutWithItsSubtree()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var user = await Redb.UserProvider.CreateUserAsync(
            new CreateUserRequest { Login = $"tree-perm-{tag}", Name = $"tree-perm-{tag}", Password = "tree-perm-password-1" });

        var root = TestDataFactory.CreateTreeNode($"perm-{tag}", $"P-{tag}");
        root.id = await Redb.SaveAsync(root);
        var allowed = TestDataFactory.CreateTreeNode($"perm-a-{tag}", $"PA-{tag}");
        allowed.id = await Redb.CreateChildAsync(allowed, root);
        var allowedChild = TestDataFactory.CreateTreeNode($"perm-a1-{tag}", $"PA1-{tag}");
        allowedChild.id = await Redb.CreateChildAsync(allowedChild, allowed);
        var refused = TestDataFactory.CreateTreeNode($"perm-b-{tag}", $"PB-{tag}");
        refused.id = await Redb.CreateChildAsync(refused, root);
        await Redb.CreateChildAsync(TestDataFactory.CreateTreeNode($"perm-b1-{tag}", $"PB1-{tag}"), refused);

        // Reading granted on the root (inherited below it); the refused branch has a row of its own without reading.
        await Redb.GrantPermissionAsync(user, root, PermissionAction.Select);
        await Redb.GrantPermissionAsync(user, refused, PermissionAction.Delete);

        var previousCheck = Redb.Configuration.DefaultCheckPermissionsOnLoad;
        var previousUser = Redb.SecurityContext.CurrentUser;
        Redb.Configuration.DefaultCheckPermissionsOnLoad = true;
        Redb.SecurityContext.SetCurrentUser(user);
        try
        {
            (await Redb.GetDescendantsAsync<TreeNodeProps>(root)).Select(d => d.id)
                .Should().Equal(allowed.id, allowedChild.id);

            var tree = await Redb.LoadTreeAsync<TreeNodeProps>(root);
            tree.Children.Select(c => c.Id).Should().Equal(allowed.id);

            (await Redb.GetPolymorphicDescendantsAsync(root, user)).Select(d => d.Id)
                .Should().Equal(allowed.id, allowedChild.id);

            (await Redb.GetPathToRootAsync<TreeNodeProps>(allowedChild)).Select(p => p.id)
                .Should().Equal(root.id, allowed.id, allowedChild.id);
        }
        finally
        {
            Redb.SecurityContext.SetCurrentUser(previousUser);
            Redb.Configuration.DefaultCheckPermissionsOnLoad = previousCheck;
        }
    }
}
