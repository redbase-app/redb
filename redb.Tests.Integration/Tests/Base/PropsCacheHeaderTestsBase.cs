using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Cluster review of the props cache (2026-09-11). <c>_hash</c> used to cover Props only, while
/// the header - name, note, value_*, value_unique, parent, owner, dates - was written on every
/// save and never hashed: a save on another node could change the header without moving the
/// hash, and the point load, answering from the cache, handed out a stale header with a valid
/// hash. Now <c>_hash</c> covers the header too (full-object-hash plan), and the partial
/// <c>UPDATE _objects</c> paths that bypass the save (tree move, trash) reset it.
///
/// "Another node" here is a second ServiceProvider with its own cache domain on the same
/// database - a real second process as far as the cache is concerned. Every test builds its own
/// hosts: the shared fixtures run with the cache off.
/// </summary>
public abstract class PropsCacheHeaderTestsBase
{
    /// <summary>
    /// Registers the provider on the options builder (Free or Pro, see <see cref="Register"/>).
    /// Called once per node; a suite that prepares a database file must do so once per test,
    /// not per node - both nodes share one database.
    /// </summary>
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(string node)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = true;
                c.SkipHashValidationOnCacheCheck = false;
                // One cache domain per node AND per suite: the Free and Pro suites of one database
                // run in parallel on the same connection string, and the hit counters are exact.
                c.CacheDomain = $"header-probe-{GetType().Name}-{node}";
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<IRedbService> BootAsync(ServiceProvider sp)
    {
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<HeaderProbeProps>();
        await redb.InitializeTypeRegistryAsync();
        ((RedbServiceBase)redb).PropsCache.Instance.Should().NotBeNull("the props cache must be active, or this test proves nothing");
        return redb;
    }

    /// <summary>Saves a keyed object on node A and loads it once through the cache, so the next load is a hit.</summary>
    private static async Task<long> SeedCachedAsync(IRedbService nodeA, string tag)
    {
        var id = await nodeA.SaveAsync(new RedbObject<HeaderProbeProps>
        {
            name = "before", note = "note-before", ValueUnique = $"HP-{tag}-{Guid.NewGuid():N}",
            Props = new HeaderProbeProps { Title = "title" }
        });
        // The save caches the very instance it wrote; evict, so the first load goes to the
        // database through the caching path and the next one is a validated hit.
        ((RedbServiceBase)nodeA).PropsCache.Instance!.Clear();
        (await nodeA.LoadAsync<HeaderProbeProps>(id, depth: 1))!.name.Should().Be("before");
        return id;
    }

    /// <summary>Node B changes the HEADER only, through the library; the Props stay as they are.</summary>
    private static async Task<string> HeaderChangedOnNodeBAsync(IRedbService nodeB, long id)
    {
        var theirs = (await nodeB.LoadAsync<HeaderProbeProps>(id, depth: 1))!;
        theirs.name = "after";
        theirs.note = "note-after";
        theirs.ValueUnique = $"HP-after-{Guid.NewGuid():N}";
        await nodeB.SaveAsync(theirs);
        return theirs.ValueUnique;
    }

    private static void AssertFreshHeader(RedbObject<HeaderProbeProps>? served, string expectedKey)
    {
        served.Should().NotBeNull();
        served!.name.Should().Be("after", "a header change on another node moves _hash, so the cached copy must not be served");
        served.note.Should().Be("note-after");
        served.ValueUnique.Should().Be(expectedKey, "the object key is header state and must be as fresh as the name");
        served.Props.Title.Should().Be("title");
    }

    [Fact]
    public async Task HeaderChange_OnAnotherNode_IsSeenByThePointLoad()
    {
        await using var spA = Build("a");
        await using var spB = Build("b");
        var nodeA = await BootAsync(spA);
        var nodeB = await BootAsync(spB);
        var id = await SeedCachedAsync(nodeA, "async");

        var newKey = await HeaderChangedOnNodeBAsync(nodeB, id);

        AssertFreshHeader(await nodeA.LoadAsync<HeaderProbeProps>(id, depth: 1), newKey);
    }

    [Fact]
    public async Task HeaderChange_OnAnotherNode_IsSeenByTheSyncPointLoad()
    {
        // The thread-pool-free twin (RedbListItem.Object's road) has its own hit branch.
        await using var spA = Build("a");
        await using var spB = Build("b");
        var nodeA = await BootAsync(spA);
        var nodeB = await BootAsync(spB);
        var id = await SeedCachedAsync(nodeA, "sync");

        var newKey = await HeaderChangedOnNodeBAsync(nodeB, id);

        AssertFreshHeader(nodeA.Load<HeaderProbeProps>(id, depth: 1), newKey);
    }

    [Fact]
    public async Task UnchangedObject_IsServedFromTheCache()
    {
        // The other side of the contract: the header is covered by the hash, so an untouched
        // object still costs one narrow probe and no reload.
        await using var spA = Build("a");
        var nodeA = await BootAsync(spA);
        var id = await SeedCachedAsync(nodeA, "hit");
        var cache = ((RedbServiceBase)nodeA).PropsCache;

        var first = await nodeA.LoadAsync<HeaderProbeProps>(id, depth: 1);
        var hitsBefore = cache.GetStats().HitCount;
        var again = await nodeA.LoadAsync<HeaderProbeProps>(id, depth: 1);

        cache.GetStats().HitCount.Should().Be(hitsBefore + 1, "nothing changed - the load must be a validated hit");
        again.Should().BeSameAs(first, "a hit serves the cached instance whole");
    }

    [Fact]
    public async Task Move_OnAnotherNode_IsSeenByThePointLoad()
    {
        // A tree move is a partial UPDATE of the row outside the save path: it must reset _hash,
        // or a cached copy would keep answering with the old parent.
        await using var spA = Build("a");
        await using var spB = Build("b");
        var nodeA = await BootAsync(spA);
        var nodeB = await BootAsync(spB);

        var oldParent = await nodeA.SaveAsync(new RedbObject<HeaderProbeProps> { name = "old-parent", Props = new HeaderProbeProps { Title = "p1" } });
        var newParent = await nodeA.SaveAsync(new RedbObject<HeaderProbeProps> { name = "new-parent", Props = new HeaderProbeProps { Title = "p2" } });
        var childId = await nodeA.SaveAsync(new RedbObject<HeaderProbeProps> { name = "child", parent_id = oldParent, Props = new HeaderProbeProps { Title = "c" } });
        ((RedbServiceBase)nodeA).PropsCache.Instance!.Clear();
        (await nodeA.LoadAsync<HeaderProbeProps>(childId, depth: 1))!.parent_id.Should().Be(oldParent);

        var theirs = (await nodeB.LoadAsync<HeaderProbeProps>(childId, depth: 1))!;
        await nodeB.MoveObjectAsync(theirs, new RedbObject<HeaderProbeProps> { id = newParent });

        (await nodeA.LoadAsync<HeaderProbeProps>(childId, depth: 1))!.parent_id.Should().Be(newParent,
            "a move on another node resets the hash, so the cached copy with the old parent must not be served");
    }

    [Fact]
    public async Task TreeLoads_CarryTheObjectKey()
    {
        // The row-to-object mappers and tree conversions had not learned value_unique (found by
        // the header pins on the way).
        await using var spA = Build("a");
        var redb = await BootAsync(spA);
        var rootKey = $"HP-root-{Guid.NewGuid():N}";
        var childKey = $"HP-child-{Guid.NewGuid():N}";
        var rootId = await redb.SaveAsync(new RedbObject<HeaderProbeProps>
            { name = "root", ValueUnique = rootKey, Props = new HeaderProbeProps { Title = "root" } });
        await redb.SaveAsync(new RedbObject<HeaderProbeProps>
            { name = "child", parent_id = rootId, ValueUnique = childKey, Props = new HeaderProbeProps { Title = "child" } });
        var root = (await redb.LoadAsync<HeaderProbeProps>(rootId, depth: 1))!;

        var typed = await redb.LoadTreeAsync<HeaderProbeProps>(root, maxDepth: 2);
        typed.ValueUnique.Should().Be(rootKey, "the typed tree root is converted from the loaded object and must keep its key");
        typed.Children.Single().ValueUnique.Should().Be(childKey, "typed children are mapped from rows and must keep their key");

        var poly = await redb.LoadPolymorphicTreeAsync(root, maxDepth: 2);
        poly.ValueUnique.Should().Be(rootKey, "the polymorphic root goes through the dynamic mapper and must keep its key");
        poly.Children.Single().ValueUnique.Should().Be(childKey);
    }
}
