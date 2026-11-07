using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Tests for polymorphic tree API: LoadPolymorphicTreeAsync, GetPolymorphicChildrenAsync,
/// GetPolymorphicDescendantsAsync, GetPolymorphicPathToRootAsync.
/// Uses 3 different scheme types in one tree:
///   OrgRoot (level 0) → Division (level 1) → Team (level 2)
/// </summary>
public abstract class PolymorphicTreeTestsBase
{
    protected readonly IRedbService Redb;

    protected PolymorphicTreeTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>
    /// Build a polymorphic tree:
    ///   Acme Corp (OrgRoot)
    ///     ├── Engineering (Division)
    ///     │   ├── Backend Team (Team)
    ///     │   └── Frontend Team (Team)
    ///     └── Sales (Division)
    ///         └── EMEA Team (Team)
    /// </summary>
    private async Task<PolyTreeIds> SeedPolymorphicTreeAsync()
    {
        var root = new TreeRedbObject<OrgRootProps>
        {
            name = "Acme Corp",
            Props = new OrgRootProps
            {
                OrgName = "Acme Corp",
                Country = "US",
                FoundedYear = 2010
            }
        };
        root.id = await Redb.SaveAsync(root);

        var engineering = new TreeRedbObject<DivisionProps>
        {
            name = "Engineering",
            Props = new DivisionProps
            {
                DivisionName = "Engineering",
                Head = "Alice",
                AnnualBudget = 500000m
            }
        };
        engineering.id = await Redb.CreateChildAsync(engineering, root);

        var sales = new TreeRedbObject<DivisionProps>
        {
            name = "Sales",
            Props = new DivisionProps
            {
                DivisionName = "Sales",
                Head = "Bob",
                AnnualBudget = 300000m
            }
        };
        sales.id = await Redb.CreateChildAsync(sales, root);

        var backend = new TreeRedbObject<TeamProps>
        {
            name = "Backend Team",
            Props = new TeamProps
            {
                TeamName = "Backend Team",
                MemberCount = 8,
                Technologies = ["C#", "PostgreSQL", "Docker"]
            }
        };
        backend.id = await Redb.CreateChildAsync(backend, engineering);

        var frontend = new TreeRedbObject<TeamProps>
        {
            name = "Frontend Team",
            Props = new TeamProps
            {
                TeamName = "Frontend Team",
                MemberCount = 5,
                Technologies = ["TypeScript", "React"]
            }
        };
        frontend.id = await Redb.CreateChildAsync(frontend, engineering);

        var emea = new TreeRedbObject<TeamProps>
        {
            name = "EMEA Team",
            Props = new TeamProps
            {
                TeamName = "EMEA Team",
                MemberCount = 3,
                Technologies = ["CRM", "Salesforce"]
            }
        };
        emea.id = await Redb.CreateChildAsync(emea, sales);

        return new PolyTreeIds(root.id, engineering.id, sales.id, backend.id, frontend.id, emea.id);
    }

    private record PolyTreeIds(long Root, long Engineering, long Sales, long Backend, long Frontend, long Emea);

    // ─── Helper to get typed Props from polymorphic tree nodes ───

    private static TProps GetProps<TProps>(ITreeRedbObject node) where TProps : class, new()
    {
        if (node is RedbObject<TProps> typed)
            return typed.Props;

        // TreeRedbObjectDynamic wraps source object
        var sourceField = node.GetType().GetProperty("SourceObject");
        if (sourceField?.GetValue(node) is RedbObject<TProps> source)
            return source.Props;

        throw new InvalidOperationException(
            $"Cannot extract {typeof(TProps).Name} from {node.GetType().Name} (id={node.Id})");
    }

    // ═══════════════════════════════════════════════════════
    // LoadPolymorphicTreeAsync
    // ═══════════════════════════════════════════════════════

    [Fact]
    public async Task LoadPolymorphicTree_ReturnsFullHierarchy()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.Should().NotBeNull();
        tree.Id.Should().Be(ids.Root);
        tree.Name.Should().Be("Acme Corp");
        tree.Children.Should().HaveCount(2);
    }

    [Fact]
    public async Task LoadPolymorphicTree_ChildrenHaveDifferentSchemes()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        // Root scheme differs from children schemes
        var childSchemeIds = tree.Children.Select(c => c.SchemeId).Distinct().ToList();
        childSchemeIds.Should().HaveCount(1); // both children are DivisionProps
        tree.SchemeId.Should().NotBe(childSchemeIds[0]); // root is OrgRootProps, different
    }

    [Fact]
    public async Task LoadPolymorphicTree_GrandchildrenAreTeams()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        var allGrandchildren = tree.Children.SelectMany(c => c.Children).ToList();
        allGrandchildren.Should().HaveCount(3); // backend, frontend, emea

        var names = allGrandchildren.Select(g => g.Name).OrderBy(n => n).ToList();
        names.Should().BeEquivalentTo(["Backend Team", "EMEA Team", "Frontend Team"]);
    }

    [Fact]
    public async Task LoadPolymorphicTree_WithMaxDepth1_LoadsOnlyDivisions()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!, maxDepth: 1);

        tree.Children.Should().HaveCount(2);
        // At maxDepth=1, grandchildren should not be loaded
        tree.Children.Should().AllSatisfy(c => c.Children.Should().BeEmpty());
    }

    [Fact]
    public async Task LoadPolymorphicTree_ParentReferencesAreSet()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.Parent.Should().BeNull(); // root has no parent
        foreach (var division in tree.Children)
        {
            division.Parent.Should().BeSameAs(tree);
            foreach (var team in division.Children)
            {
                team.Parent.Should().BeSameAs(division);
            }
        }
    }

    [Fact]
    public async Task LoadPolymorphicTree_LevelIsCorrect()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.Level.Should().Be(0);
        foreach (var division in tree.Children)
        {
            division.Level.Should().Be(1);
            foreach (var team in division.Children)
            {
                team.Level.Should().Be(2);
            }
        }
    }

    [Fact]
    public async Task LoadPolymorphicTree_IsLeafCorrectForLeaves()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.IsLeaf.Should().BeFalse();
        var allLeaves = tree.Children.SelectMany(c => c.Children).ToList();
        allLeaves.Should().AllSatisfy(l => l.IsLeaf.Should().BeTrue());
    }

    [Fact]
    public async Task LoadPolymorphicTree_PropsAreTyped()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        // Root has OrgRootProps
        var rootProps = GetProps<OrgRootProps>(tree);
        rootProps.OrgName.Should().Be("Acme Corp");
        rootProps.Country.Should().Be("US");
        rootProps.FoundedYear.Should().Be(2010);

        // Engineering division has DivisionProps
        var engNode = tree.Children.First(c => c.Name == "Engineering");
        var engProps = GetProps<DivisionProps>(engNode);
        engProps.DivisionName.Should().Be("Engineering");
        engProps.Head.Should().Be("Alice");
        engProps.AnnualBudget.Should().Be(500000m);

        // Backend team has TeamProps
        var backendNode = engNode.Children.First(c => c.Name == "Backend Team");
        var backendProps = GetProps<TeamProps>(backendNode);
        backendProps.TeamName.Should().Be("Backend Team");
        backendProps.MemberCount.Should().Be(8);
        backendProps.Technologies.Should().BeEquivalentTo(["C#", "PostgreSQL", "Docker"]);
    }

    // ═══════════════════════════════════════════════════════
    // GetPolymorphicChildrenAsync
    // ═══════════════════════════════════════════════════════

    [Fact]
    public async Task GetPolymorphicChildren_ReturnsDirectChildren()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var children = (await Redb.GetPolymorphicChildrenAsync(rootObj!)).ToList();

        children.Should().HaveCount(2);
        children.Select(c => c.Name).Should().BeEquivalentTo(["Engineering", "Sales"]);
    }

    [Fact]
    public async Task GetPolymorphicChildren_OfDivision_ReturnsTeams()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var engObj = await Redb.LoadAsync<DivisionProps>(ids.Engineering);

        var children = (await Redb.GetPolymorphicChildrenAsync(engObj!)).ToList();

        children.Should().HaveCount(2);
        children.Select(c => c.Name).Should().BeEquivalentTo(["Backend Team", "Frontend Team"]);
    }

    [Fact]
    public async Task GetPolymorphicChildren_OfLeaf_ReturnsEmpty()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var backendObj = await Redb.LoadAsync<TeamProps>(ids.Backend);

        var children = (await Redb.GetPolymorphicChildrenAsync(backendObj!)).ToList();

        children.Should().BeEmpty();
    }

    [Fact]
    public async Task GetPolymorphicChildren_PropsAreTyped()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var engObj = await Redb.LoadAsync<DivisionProps>(ids.Engineering);

        var children = (await Redb.GetPolymorphicChildrenAsync(engObj!)).ToList();

        var backend = children.First(c => c.Name == "Backend Team");
        var props = GetProps<TeamProps>(backend);
        props.MemberCount.Should().Be(8);
        props.Technologies.Should().Contain("C#");
    }

    // ═══════════════════════════════════════════════════════
    // GetPolymorphicDescendantsAsync
    // ═══════════════════════════════════════════════════════

    [Fact]
    public async Task GetPolymorphicDescendants_ReturnsAllBelow()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var descendants = (await Redb.GetPolymorphicDescendantsAsync(rootObj!)).ToList();

        descendants.Should().HaveCount(5); // 2 divisions + 3 teams
    }

    [Fact]
    public async Task GetPolymorphicDescendants_OfDivision_ReturnsOnlySubtree()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var engObj = await Redb.LoadAsync<DivisionProps>(ids.Engineering);

        var descendants = (await Redb.GetPolymorphicDescendantsAsync(engObj!)).ToList();

        descendants.Should().HaveCount(2); // backend + frontend
        descendants.Select(d => d.Name).Should().BeEquivalentTo(["Backend Team", "Frontend Team"]);
    }

    [Fact]
    public async Task GetPolymorphicDescendants_ContainsMixedSchemes()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var descendants = (await Redb.GetPolymorphicDescendantsAsync(rootObj!)).ToList();

        var distinctSchemes = descendants.Select(d => d.SchemeId).Distinct().ToList();
        distinctSchemes.Should().HaveCount(2); // DivisionProps + TeamProps
    }

    [Fact]
    public async Task GetPolymorphicDescendants_WithMaxDepth1_OnlyDirectChildren()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var descendants = (await Redb.GetPolymorphicDescendantsAsync(rootObj!, maxDepth: 1)).ToList();

        descendants.Should().HaveCount(2); // only 2 divisions, no teams
        descendants.Select(d => d.Name).Should().BeEquivalentTo(["Engineering", "Sales"]);
    }

    // ═══════════════════════════════════════════════════════
    // GetPolymorphicPathToRootAsync
    // ═══════════════════════════════════════════════════════

    [Fact]
    public async Task GetPolymorphicPathToRoot_FromLeaf_ReturnsFullPath()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var backendObj = await Redb.LoadAsync<TeamProps>(ids.Backend);

        var path = (await Redb.GetPolymorphicPathToRootAsync(backendObj!)).ToList();

        // Path: Backend Team → Engineering → Acme Corp (or some subset depending on implementation)
        path.Should().HaveCountGreaterThanOrEqualTo(2);
        path.Select(p => p.Name).Should().Contain("Engineering");
        path.Select(p => p.Name).Should().Contain("Acme Corp");
    }

    [Fact]
    public async Task GetPolymorphicPathToRoot_CrossesSchemes()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var backendObj = await Redb.LoadAsync<TeamProps>(ids.Backend);

        var path = (await Redb.GetPolymorphicPathToRootAsync(backendObj!)).ToList();

        var distinctSchemes = path.Select(p => p.SchemeId).Distinct().ToList();
        // Path crosses at least 2 scheme types: DivisionProps → OrgRootProps
        distinctSchemes.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task GetPolymorphicPathToRoot_FromRoot_ReturnsEmpty()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var path = (await Redb.GetPolymorphicPathToRootAsync(rootObj!)).ToList();

        // Root has no ancestors — path should be empty or contain only root itself
        path.Should().HaveCountLessThanOrEqualTo(1);
    }

    // ═══════════════════════════════════════════════════════
    // Navigation helpers on ITreeRedbObject
    // ═══════════════════════════════════════════════════════

    [Fact]
    public async Task GetSubtree_ReturnsAllNodesInDepthFirst()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        var subtree = tree.GetSubtree().ToList();

        subtree.Should().HaveCount(6); // root + 2 divisions + 3 teams
    }

    [Fact]
    public async Task SubtreeSize_ReturnsCorrectCount()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.SubtreeSize.Should().Be(6);

        var engNode = tree.Children.First(c => c.Name == "Engineering");
        engNode.SubtreeSize.Should().Be(3); // eng + backend + frontend
    }

    [Fact]
    public async Task MaxDepth_ReturnsCorrectValue()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        tree.MaxDepth.Should().Be(2); // root → division → team
    }

    [Fact]
    public async Task GetBreadcrumbs_FormatsCorrectly()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);
        var engNode = tree.Children.First(c => c.Name == "Engineering");
        var backendNode = engNode.Children.First(c => c.Name == "Backend Team");

        var breadcrumbs = backendNode.GetBreadcrumbs();

        breadcrumbs.Should().Contain("Acme Corp");
        breadcrumbs.Should().Contain("Engineering");
        breadcrumbs.Should().Contain("Backend Team");
    }

    [Fact]
    public async Task Ancestors_ReturnsPathUpToRoot()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);
        var engNode = tree.Children.First(c => c.Name == "Engineering");
        var backendNode = engNode.Children.First(c => c.Name == "Backend Team");

        var ancestors = backendNode.Ancestors.ToList();

        ancestors.Should().HaveCount(2);
        ancestors[0].Name.Should().Be("Engineering");
        ancestors[1].Name.Should().Be("Acme Corp");
    }

    [Fact]
    public async Task Descendants_ReturnsAllBelow()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);

        var descendants = tree.Descendants.ToList();

        descendants.Should().HaveCount(5); // 2 divisions + 3 teams
    }

    [Fact]
    public async Task IsDescendantOf_CorrectForCrossScheme()
    {
        var ids = await SeedPolymorphicTreeAsync();
        var rootObj = await Redb.LoadAsync<OrgRootProps>(ids.Root);

        var tree = await Redb.LoadPolymorphicTreeAsync(rootObj!);
        var engNode = tree.Children.First(c => c.Name == "Engineering");
        var backendNode = engNode.Children.First(c => c.Name == "Backend Team");

        backendNode.IsDescendantOf(tree).Should().BeTrue();
        backendNode.IsDescendantOf(engNode).Should().BeTrue();
        tree.IsDescendantOf(backendNode).Should().BeFalse();
    }
}
