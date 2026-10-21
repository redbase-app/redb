using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A reference whose target left for the trash loads as null in its place, in a collection too: an array keeps its
/// length and a dictionary keeps the key. The PostgreSQL and SQLite Free builders and every Pro materializer agree; the
/// MSSQL Free builder dropped the element (STRING_AGG skips the NULL of the nested get_object_json) and the dictionary
/// key with it. Found by the Free host of <see cref="ListItemObjectWalkTestsBase"/> (plan
/// docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md).
/// </summary>
public abstract class TrashedReferenceTargetTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    /// <summary>
    /// The Pro props cache: a query caches the nested objects of its rows by their ids, and a cached graph is checked
    /// against the rows of its loaded references before it is served. The Free loaders do neither yet (Free parity).
    /// </summary>
    protected virtual bool ProPropsCache => false;

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(bool propsCache = false, string? cacheDomain = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = propsCache;
                c.CacheDomain = cacheDomain ?? (propsCache ? "trashed-reference-target-cached" : "trashed-reference-target");
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, long KeptId);

    private static async Task<Seed> SeedAsync(ServiceProvider sp)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var kept = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"trashref-kept-{tag}", Props = new CtProbeChildProps { Tag = "kept" } });
        var gone = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"trashref-gone-{tag}", Props = new CtProbeChildProps { Tag = "gone" } });
        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps>
        {
            name = $"trashref-root-{tag}",
            Props = new CtProbeProps
            {
                Label = "root",
                Refs = [new RedbObject<CtProbeChildProps> { id = kept }, new RedbObject<CtProbeChildProps> { id = gone }],
                RefDict = new()
                {
                    ["kept"] = new RedbObject<CtProbeChildProps> { id = kept },
                    ["gone"] = new RedbObject<CtProbeChildProps> { id = gone }
                }
            }
        });
        await redb.SoftDeleteAsync(new[] { gone });
        return new Seed(rootId, kept);
    }

    private async Task<(CtProbeProps Props, Seed Seed)> LoadAsync()
    {
        await using var sp = Build();
        var seed = await SeedAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var root = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<CtProbeProps>(seed.RootId, depth: 10);
        return (root!.Props, seed);
    }

    [Fact]
    public async Task Load_TrashedTargetInArray_IsNullInItsPlace()
    {
        var (props, seed) = await LoadAsync();

        props.Refs.Should().HaveCount(2, "the array keeps its length: a trashed target is null in its place, never dropped");
        props.Refs![0].id.Should().Be(seed.KeptId);
        props.Refs[0].Props.Tag.Should().Be("kept");
        props.Refs[1].Should().BeNull("the target left for the trash");
    }

    [Fact]
    public async Task Load_TrashedTargetInDictionary_KeepsItsKeyWithNull()
    {
        var (props, seed) = await LoadAsync();

        props.RefDict.Should().NotBeNull();
        props.RefDict!.Keys.Should().BeEquivalentTo(new[] { "kept", "gone" },
            "the dictionary keeps the key of a trashed target, with null as its value");
        props.RefDict["kept"].id.Should().Be(seed.KeptId);
        props.RefDict["kept"].Props.Tag.Should().Be("kept");
        props.RefDict["gone"].Should().BeNull("the target left for the trash");
    }

    /// <summary>
    /// With the props cache on, an object loaded before its reference went to the trash is not served from the
    /// cache with the trashed target still materialized inside: its hash does not move, so the read checks the
    /// loaded objects of the graph against their rows, the object reaching the target through another one
    /// (A -> B -> C) included.
    /// </summary>
    [Fact]
    public async Task Load_WithPropsCache_ReferenceTrashedAfterCaching_IsNull()
    {
        if (!ProPropsCache) return;

        await using var sp = Build(propsCache: true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<GraphNodeProps>();

        var tag = Guid.NewGuid().ToString("N")[..8];
        var c = await redb.SaveAsync(new RedbObject<GraphNodeProps> { name = $"trashref-c-{tag}", Props = new GraphNodeProps { Title = "c" } });
        var b = await redb.SaveAsync(new RedbObject<GraphNodeProps>
        {
            name = $"trashref-b-{tag}",
            Props = new GraphNodeProps { Title = "b", Left = new RedbObject<GraphNodeProps> { id = c } }
        });
        var a = await redb.SaveAsync(new RedbObject<GraphNodeProps>
        {
            name = $"trashref-a-{tag}",
            Props = new GraphNodeProps { Title = "a", Left = new RedbObject<GraphNodeProps> { id = b } }
        });

        // Loaded and cached with the whole chain.
        (await redb.LoadAsync<GraphNodeProps>(b, depth: 10))!.Props.Left!.Props.Title.Should().Be("c");
        (await redb.LoadAsync<GraphNodeProps>(a, depth: 10))!.Props.Left!.Props.Left!.Props.Title.Should().Be("c");

        await redb.SoftDeleteAsync(new[] { c });

        (await redb.LoadAsync<GraphNodeProps>(b, depth: 10))!.Props.Left
            .Should().BeNull("the direct referrer is dropped from the cache and reloaded");
        (await redb.LoadAsync<GraphNodeProps>(a, depth: 10))!.Props.Left!.Props.Left
            .Should().BeNull("the object reaching the target through another one is dropped as well");
    }

    /// <summary>
    /// A cluster: a referenced object changed by another node, whose cache this node never hears about. The graph
    /// cached here is checked against the rows when it is read, and the change shows up.
    /// </summary>
    [Fact]
    public async Task Load_WithPropsCache_ReferenceChangedOnAnotherNode_IsReloaded()
    {
        if (!ProPropsCache) return;

        await using var sp = Build(propsCache: true, cacheDomain: "trashed-reference-node-1");
        await using var other = Build(propsCache: true, cacheDomain: "trashed-reference-node-2");
        var redb = sp.GetRequiredService<IRedbService>();
        var node2 = other.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await node2.InitializeAsync();
        await redb.SyncSchemeAsync<GraphNodeProps>();

        var tag = Guid.NewGuid().ToString("N")[..8];
        var b = await redb.SaveAsync(new RedbObject<GraphNodeProps> { name = $"trashref-nb-{tag}", Props = new GraphNodeProps { Title = "before" } });
        var a = await redb.SaveAsync(new RedbObject<GraphNodeProps>
        {
            name = $"trashref-na-{tag}",
            Props = new GraphNodeProps { Title = "a", Left = new RedbObject<GraphNodeProps> { id = b } }
        });
        (await redb.LoadAsync<GraphNodeProps>(a, depth: 10))!.Props.Left!.Props.Title.Should().Be("before");

        var onNode2 = (await node2.LoadAsync<GraphNodeProps>(b))!;
        onNode2.Props.Title = "after";
        await node2.SaveAsync(onNode2);

        (await redb.LoadAsync<GraphNodeProps>(a, depth: 10))!.Props.Left!.Props.Title
            .Should().Be("after", "the cached graph is checked against the row of its loaded reference");
    }

    /// <summary>
    /// A nested object is cached by its own id whichever way its root was loaded: LoadAsync and a query alike.
    /// </summary>
    [Fact]
    public async Task Load_WithPropsCache_NestedObjectCachedByItsOwnId()
    {
        await using var sp = Build(propsCache: true, cacheDomain: "trashed-reference-nested-by-id");
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<GraphNodeProps>();

        var tag = Guid.NewGuid().ToString("N")[..8];
        var viaLoadLeaf = await redb.SaveAsync(new RedbObject<GraphNodeProps> { name = $"trashref-ll-{tag}", Props = new GraphNodeProps { Title = "leaf" } });
        var viaLoadRoot = await redb.SaveAsync(new RedbObject<GraphNodeProps>
        {
            name = $"trashref-lr-{tag}",
            Props = new GraphNodeProps { Title = $"load-{tag}", Left = new RedbObject<GraphNodeProps> { id = viaLoadLeaf } }
        });
        var viaQueryLeaf = await redb.SaveAsync(new RedbObject<GraphNodeProps> { name = $"trashref-ql-{tag}", Props = new GraphNodeProps { Title = "leaf" } });
        var queryTitle = $"query-{tag}";
        await redb.SaveAsync(new RedbObject<GraphNodeProps>
        {
            name = $"trashref-qr-{tag}",
            Props = new GraphNodeProps { Title = queryTitle, Left = new RedbObject<GraphNodeProps> { id = viaQueryLeaf } }
        });

        await redb.LoadAsync<GraphNodeProps>(viaLoadRoot, depth: 10);
        (await redb.Query<GraphNodeProps>().Where(p => p.Title == queryTitle).ToListAsync()).Should().HaveCount(1);

        redb.PropsCache.GetWithoutHashValidation<GraphNodeProps>(viaLoadLeaf).Should().NotBeNull("LoadAsync caches the nested object by its id");
        if (ProPropsCache)
            redb.PropsCache.GetWithoutHashValidation<GraphNodeProps>(viaQueryLeaf).Should().NotBeNull("a query caches the nested object by its id");
    }

    /// <summary>
    /// References inside collections (a list and a dictionary of references) take part in the check of a cached graph
    /// the same way, and the referenced objects are cached by their own ids.
    /// </summary>
    [Fact]
    public async Task Load_WithPropsCache_ReferencesInCollections_TrashedAfterCaching_AreNull()
    {
        if (!ProPropsCache) return;

        await using var sp = Build(propsCache: true, cacheDomain: "trashed-reference-collections");
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var tag = Guid.NewGuid().ToString("N")[..8];
        var kept = new RedbObject<CtProbeChildProps> { name = $"trashref-ckept-{tag}", Props = new CtProbeChildProps { Tag = "kept" } };
        var gone = new RedbObject<CtProbeChildProps> { name = $"trashref-cgone-{tag}", Props = new CtProbeChildProps { Tag = "gone" } };
        await redb.SaveAsync(kept);
        await redb.SaveAsync(gone);
        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps>
        {
            name = $"trashref-croot-{tag}",
            Props = new CtProbeProps { Label = "root", Refs = [kept, gone], RefDict = new() { ["kept"] = kept, ["gone"] = gone } }
        });

        var before = (await redb.LoadAsync<CtProbeProps>(rootId, depth: 10))!;
        before.Props.Refs![1].Props.Tag.Should().Be("gone");
        redb.PropsCache.GetWithoutHashValidation<CtProbeChildProps>(kept.Id).Should().NotBeNull("a referenced object is cached by its id");

        await redb.SoftDeleteAsync(new[] { gone.Id });

        var after = (await redb.LoadAsync<CtProbeProps>(rootId, depth: 10))!;
        after.Props.Refs.Should().HaveCount(2);
        after.Props.Refs![0].Props.Tag.Should().Be("kept");
        after.Props.Refs[1].Should().BeNull("the list reference to the trashed object is checked against its row");
        after.Props.RefDict!["gone"].Should().BeNull("the dictionary reference is checked the same way");
        after.Props.RefDict["kept"].Props.Tag.Should().Be("kept");
    }

    /// <summary>A hard delete drops the object from the cache, and a later load finds nothing.</summary>
    [Fact]
    public async Task Delete_WithPropsCache_RemovesTheObjectFromTheCache()
    {
        await using var sp = Build(propsCache: true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<GraphNodeProps>();

        var id = await redb.SaveAsync(new RedbObject<GraphNodeProps> { name = $"trashref-del-{Guid.NewGuid():N}", Props = new GraphNodeProps { Title = "x" } });
        (await redb.LoadAsync<GraphNodeProps>(id)).Should().NotBeNull();
        redb.PropsCache.GetWithoutHashValidation<GraphNodeProps>(id).Should().NotBeNull("the load cached it");

        (await redb.DeleteAsync(id)).Should().BeTrue();

        redb.PropsCache.GetWithoutHashValidation<GraphNodeProps>(id).Should().BeNull("the delete dropped it from the cache");
        (await redb.LoadAsync<GraphNodeProps>(id)).Should().BeNull();
    }
}
