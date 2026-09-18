using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A load returns what it was asked for (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §11, found while listing the
/// materialization roads for §4.1): the Props depth a query names, the unique key of an object served as a tree node,
/// and the Props of a reference that carries no hash while the props cache is on.
/// </summary>
public abstract class MaterializationFidelityTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(Action<RedbServiceConfiguration> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.SkipHashValidationOnCacheCheck = false;
                configure(c);
                // A props cache lives per domain for the process: one domain per suite and per cache setting.
                c.CacheDomain = $"fidelity-{GetType().Name}-{(c.EnablePropsCache ? "cache" : "nocache")}";
            });
        });
        return services.BuildServiceProvider();
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.SyncSchemeAsync<TreeNodeProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    /// <summary>root → mid → leaf.</summary>
    private static async Task<(long RootId, long MidId)> SeedChainAsync(IRedbService redb, string tag)
    {
        var leafId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"fidelity-leaf-{tag}", Props = new LazyNodeProps { Label = $"leaf-{tag}" } });
        var midId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"fidelity-mid-{tag}", Props = new LazyNodeProps { Label = $"mid-{tag}", Next = new RedbObject<LazyNodeProps> { id = leafId } } });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"fidelity-root-{tag}", Props = new LazyNodeProps { Label = $"root-{tag}", Next = new RedbObject<LazyNodeProps> { id = midId } } });
        return (rootId, midId);
    }

    private static void AssertDepth(RedbObject<LazyNodeProps> root, int depth, string tag, string road)
    {
        root.Props.Label.Should().Be($"root-{tag}", $"{road}: the object's own Props are loaded");
        var mid = root.Props.Next!;
        if (depth == 1)
        {
            mid.IsPropsLoaded.Should().BeFalse($"{road}: depth 1 stops at the object's own Props - the reference is a stub");
            return;
        }
        mid.IsPropsLoaded.Should().BeTrue($"{road}: depth {depth} loads the first reference");
        mid.Props.Next!.IsPropsLoaded.Should().BeFalse($"{road}: depth {depth} stops before the second reference");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Query_WithPropsDepth_LoadsTheDepthItNames(int depth)
    {
        await using var sp = Build(_ => { });
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var (rootId, _) = await SeedChainAsync(redb, tag);

        var rows = await redb.Query<LazyNodeProps>().WhereRedb(o => o.Id == rootId).WithPropsDepth(depth).ToListAsync();

        AssertDepth(rows.Should().ContainSingle().Subject, depth, tag, "Query");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task TreeQuery_WithPropsDepth_LoadsTheDepthItNames(int depth)
    {
        await using var sp = Build(_ => { });
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var (rootId, _) = await SeedChainAsync(redb, tag);

        var rows = await redb.TreeQuery<LazyNodeProps>().WhereRedb(o => o.Id == rootId).WithPropsDepth(depth).ToListAsync();

        AssertDepth(rows.Should().ContainSingle().Subject, depth, tag, "TreeQuery");
    }

    [Fact]
    public async Task TreeNodes_CarryTheirUniqueKey_OnEveryTreeRoad()
    {
        await using var sp = Build(_ => { });
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var parent = new TreeRedbObject<TreeNodeProps>
            { name = $"fidelity-parent-{tag}", ValueUnique = $"P-{tag}", Props = new TreeNodeProps { Name = "parent", Code = $"P-{tag}" } };
        parent.id = await redb.SaveAsync(parent);
        var child = new TreeRedbObject<TreeNodeProps>
            { name = $"fidelity-child-{tag}", ValueUnique = $"C-{tag}", Props = new TreeNodeProps { Name = "child", Code = $"C-{tag}" } };
        child.id = await redb.CreateChildAsync(child, parent);

        var children = (await redb.GetChildrenAsync<TreeNodeProps>(parent)).ToList();
        children.Should().ContainSingle().Which.ValueUnique.Should().Be($"C-{tag}", "GetChildrenAsync");

        var tree = await redb.LoadTreeAsync<TreeNodeProps>(parent.id);
        tree.ValueUnique.Should().Be($"P-{tag}", "LoadTreeAsync: the root");
        tree.Children.Should().ContainSingle().Which.Should().BeAssignableTo<Core.Models.Contracts.IRedbObject>()
            .Which.ValueUnique.Should().Be($"C-{tag}", "LoadTreeAsync: a child");

        var queried = await redb.TreeQuery<TreeNodeProps>(parent.id).ToListAsync();
        queried.Should().Contain(o => o.id == child.id, "precondition: the tree query reaches the child");
        queried.Single(o => o.id == child.id).ValueUnique.Should().Be($"C-{tag}", "TreeQuery");
    }

    [Fact]
    public async Task ReferenceWithoutAHash_LoadsItsProps_WhileThePropsCacheIsOn()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var (_, midId) = await SeedChainAsync(redb, tag);

        // A reference built in user code: an id and no hash - nothing the cache could match it by.
        var holder = new RedbObject<LazyNodeProps>
            { Props = new LazyNodeProps { Next = new RedbObject<LazyNodeProps> { id = midId } } };
        holder.Props.Next!.hash.Should().BeNull("precondition: the reference carries no hash");

        await redb.LoadReferencesAsync(holder, p => p.Next);

        holder.Props.Next.IsPropsLoaded.Should().BeTrue("a reference the cache cannot match is loaded from the database, not dropped");
        holder.Props.Next.Props.Label.Should().Be($"mid-{tag}");
    }
}
