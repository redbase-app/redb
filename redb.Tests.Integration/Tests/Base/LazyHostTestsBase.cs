using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Utils;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// V4 (Л2, review): what the shared-database fixtures cannot see. They hold one context for the
/// whole run and never return its connection to the pool, so (a) the GLOBAL lazy option armed at
/// connection open was never checked on a second scope - and on PostgreSQL <c>DISCARD ALL</c> wiped
/// it on every pooled hand-out - and (b) <c>EnablePropsCache</c> is off everywhere, so the props-cache
/// walker was never run over a graph with stubs. Every test here builds its own host.
/// </summary>
public abstract class LazyHostTestsBase
{
    /// <summary>Registers the provider on the options builder (Free or Pro, see <see cref="Register"/>).</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options);

    /// <summary>Reads the session flag the builders consult; "1" when armed.</summary>
    protected abstract string ReadFlagSql { get; }

    /// <summary>
    /// The Free builders read the session flag; the Pro materializer reads the global option live
    /// and its registrations do not arm the flag (a recorded Л2 boundary).
    /// </summary>
    protected virtual bool SessionFlagIsTheChannel => true;

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
                // Own cache domain per suite: this process also holds the shared fixtures on the same database, and the
                // hosts of the six suites run in parallel - one database is registered per domain.
                c.CacheDomain = $"lazy-host-{GetType().Name}";
                configure(c);
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<long> SaveTriangleAsync(IRedbService redb, string tag)
    {
        var nextId = await redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lhost-next-{tag}", Props = new LazyOptionNodeProps { Label = $"next-{tag}" } });
        var plainId = await redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
            { name = $"lhost-plain-{tag}", Props = new LazyOptionNodeProps { Label = $"plain-{tag}" } });
        return await redb.SaveAsync(new RedbObject<LazyOptionNodeProps>
        {
            name = $"lhost-root-{tag}",
            Props = new LazyOptionNodeProps
            {
                Label = $"root-{tag}",
                Next = new RedbObject<LazyOptionNodeProps> { id = nextId },
                Plain = new RedbObject<LazyOptionNodeProps> { id = plainId },
            }
        });
    }

    private static async Task<long> SaveChainAsync(IRedbService redb, string tag)
    {
        var leafId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"lhost-leaf-{tag}", Props = new LazyNodeProps { Label = $"leaf-{tag}" } });
        var midId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lhost-mid-{tag}",
            Props = new LazyNodeProps { Label = $"mid-{tag}", Next = new RedbObject<LazyNodeProps> { id = leafId } }
        });
        return await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"lhost-root-{tag}",
            Props = new LazyNodeProps { Label = $"root-{tag}", Next = new RedbObject<LazyNodeProps> { id = midId } }
        });
    }

    [Fact]
    public async Task GlobalOption_HoldsOnEveryScope_NotOnlyTheFirstPhysicalConnection()
    {
        await using var sp = Build(c => c.EnableLazyReferences = true);
        var boot = sp.GetRequiredService<IRedbService>();
        await boot.InitializeAsync(ensureCreated: true);
        await boot.SyncSchemeAsync<LazyOptionNodeProps>();
        await boot.InitializeTypeRegistryAsync();

        long rootId;
        using (var seed = sp.CreateScope())
            rootId = await SaveTriangleAsync(seed.ServiceProvider.GetRequiredService<IRedbService>(), "scopes");

        // Three scopes in a row: after the first one its connection went back to the pool and the
        // next scope gets it again. DISCARD ALL (PostgreSQL) / sp_reset_connection (MSSQL) wipe the
        // session state in between; the option must be armed again on every hand-out.
        for (var scope = 1; scope <= 3; scope++)
        {
            using var s = sp.CreateScope();
            var redb = s.ServiceProvider.GetRequiredService<IRedbService>();

            if (SessionFlagIsTheChannel)
            {
                var flag = await redb.Context.ExecuteScalarAsync<object>(ReadFlagSql);
                Convert.ToString(flag).Should().Be("1",
                    $"scope {scope}: the session flag must be armed on the pooled connection, not only on a fresh backend");
            }

            var root = await redb.LoadAsync<LazyOptionNodeProps>(rootId, depth: 10);
            root!.Props.Next!.IsPropsLoaded.Should().BeFalse(
                $"scope {scope}: a virtual reference is a stub at any depth while the option is on");
            root.Props.Plain!.IsPropsLoaded.Should().BeTrue("a non-virtual reference stays eager");
        }
    }

    [Fact]
    public async Task PropsCacheOn_LazyLoadsStayLazy()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
        var rootId = await SaveChainAsync(redb, "cache");
        ((RedbServiceBase)redb).PropsCache.Instance.Should().NotBeNull("the props cache must be active, or this test proves nothing");
        // The save itself caches what it wrote (the very in-memory instances); evict, so the loads
        // below really go to the database and through the caching walker.
        ((RedbServiceBase)redb).PropsCache.Instance!.Clear();

        // The single load (the Pro path that walked the graph after attaching the loaders) and the
        // bulk load (the Free path that did the same), both at depth 1: caching the loaded graph
        // must never read a stub's Props - that read IS the lazy load, synchronous, for the whole
        // reachable graph.
        var single = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        single!.Props.Next!.IsPropsLoaded.Should().BeFalse("the single load must not wake the boundary stub while caching");

        // A second chain for the bulk form: the first root is already in the cache and would be
        // served from there, walker untouched.
        var bulkRootId = await SaveChainAsync(redb, "cache-bulk");
        ((RedbServiceBase)redb).PropsCache.Instance!.Clear();
        var bulk = (RedbObject<LazyNodeProps>)(await redb.LoadAsync(new[] { bulkRootId }, depth: 1))[0];
        bulk.Props.Next!.IsPropsLoaded.Should().BeFalse("the bulk load must not wake the boundary stub while caching");

        // And the cache serves the parent consistently: the live hash equals the persisted one.
        var again = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        again!.hash.Should().Be(single.hash);
        RedbHash.ComputeFor(again).Should().Be(single.hash,
            "a cache entry whose live hash never matches its stored one is a miss for ever");
    }

    // A reference of a cached object read inside a transaction: ReaderConnectionTestsBase (owner decision 2026-09-15 -
    // the load runs on the reader's connection and the shared instance keeps nothing).

    [Fact]
    public async Task ThrowMode_GetterRefuses_AsyncApisStillLoad()
    {
        await using var sp = Build(c => c.LazyReferenceAccess = LazyReferenceAccessMode.Throw);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
        var rootId = await SaveChainAsync(redb, "throw");

        var root = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var stub = root!.Props.Next!;
        stub.IsPropsLoaded.Should().BeFalse();

        Action act = () => _ = stub.Props;
        act.Should().Throw<RedbSynchronousLazyLoadException>("the host declared a blocking load on property access a defect")
            .Which.ObjectId.Should().Be(stub.id);
        stub.IsPropsLoaded.Should().BeFalse("a refusal loads nothing and memoises nothing");

        await stub.LoadPropsAsync();
        stub.IsPropsLoaded.Should().BeTrue();
        stub.Props.Label.Should().Be("mid-throw", "the explicit async load works in every mode");
    }

    [Fact]
    public async Task BlockingGetter_DoesNotDeadlock_UnderASynchronizationContext()
    {
        await using var sp = Build(_ => { });
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
        var rootId = await SaveChainAsync(redb, "uictx");

        var root = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var stub = root!.Props.Next!;

        // A UI-like context: continuations posted to it never run while its thread is blocked in
        // the getter - exactly Blazor Server, WPF, MAUI. Without the pool hand-off this deadlocks.
        string? label = null;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            SynchronizationContext.SetSynchronizationContext(new BlockedUiContext());
            try { label = stub.Props?.Label; }
            catch (Exception ex) { error = ex; }
        }) { IsBackground = true };
        thread.Start();
        thread.Join(TimeSpan.FromSeconds(30)).Should().BeTrue(
            "the getter must finish on a host with a SynchronizationContext, not deadlock on its own continuations");
        error.Should().BeNull();
        label.Should().Be("mid-uictx");
    }

    [Fact]
    public async Task SkipHashValidation_NeverServesTheCacheInsideATransaction()
    {
        // Bug report п.1 (2026-09-02): with EnablePropsCache + SkipHashValidationOnCacheCheck the
        // single load answered from the cache with ZERO database queries - even inside
        // ExecuteAtomicAsync + LockForUpdateAsync, where the re-read is the read of a
        // read-modify-write. A pre-lock cached copy there is the lost update the lock exists to
        // prevent. Inside a transaction the load now always consults the database.
        await using var sp = Build(c => { c.EnablePropsCache = true; c.SkipHashValidationOnCacheCheck = true; });
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();

        var obj = new RedbObject<LazyNodeProps> { name = "rmw", Props = new LazyNodeProps { Label = "v1" } };
        var id = await redb.SaveAsync(obj);
        (await redb.LoadAsync<LazyNodeProps>(id, depth: 1))!.Props.Label.Should().Be("v1"); // cached

        // Another node commits: the row changes behind this process's cache.
        var labelStructure = await redb.Context.ExecuteScalarAsync<long?>(
            $"SELECT _id FROM _structures WHERE _name = 'Label' AND _id_scheme = (SELECT _id_scheme FROM _objects WHERE _id = {id})");
        await redb.Context.ExecuteAsync($"UPDATE _values SET _String = 'v2' WHERE _id_object = {id} AND _id_structure = {labelStructure}");
        await redb.Context.ExecuteAsync($"UPDATE _objects SET _hash = NULL WHERE _id = {id}");

        // Outside a transaction the zero-DB shortcut is the documented single-writer trade-off:
        // the stale copy is served. That contract stays.
        (await redb.LoadAsync<LazyNodeProps>(id, depth: 1))!.Props.Label.Should().Be("v1",
            "outside a transaction the flag trades freshness for zero-latency hits, as documented");

        // Inside the transaction of a read-modify-write the database must be consulted.
        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            (await redb.LoadAsync<LazyNodeProps>(id, depth: 1))!.Props.Label.Should().Be("v2",
                "a load inside a transaction is the read of a read-modify-write and must see the database's row");
        });
    }
    [Fact]
    public async Task MutatedCachedInstance_AfterAFailedSave_IsNotServed()
    {
        // Bug report п.3 (2026-09-02) - pinned, not red-before: the dirty-snapshot guard already
        // refused a mutated root (pre-V4), and the graph walk of this review covers nested edits.
        // The scenario from the report: read from cache, mutate the shared Props, save FAILS
        // (unique violation) - the next read must serve the committed state, not the mutation.
        await using var sp = Build(c => c.EnablePropsCache = true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();

        // Keys are per run: the Free and Pro fixtures share one database, and a fixed key would
        // trip over the previous run's row at setup.
        var k1 = $"PIN-{Guid.NewGuid():N}";
        await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = "pin-a", ValueUnique = k1, Props = new LazyNodeProps { Label = "a" } });
        var b = new RedbObject<LazyNodeProps>
            { name = "pin-b", ValueUnique = $"PIN-{Guid.NewGuid():N}", Props = new LazyNodeProps { Label = "committed" } };
        var bId = await redb.SaveAsync(b);

        var cached = await redb.LoadAsync<LazyNodeProps>(bId, depth: 1);
        cached!.Props.Label = "phantom";           // mutate the SHARED cached instance
        cached.ValueUnique = k1;                   // and make the save fail on the unique index
        var act = async () => await redb.SaveAsync(cached);
        await act.Should().ThrowAsync<RedbUniqueViolationException>();

        var served = await redb.LoadAsync<LazyNodeProps>(bId, depth: 1);
        served!.Props.Label.Should().Be("committed",
            "a failed save must not leave the mutated copy answering from the cache");
    }
    [Fact]
    public async Task CachedGraph_WithAnUnsavedNestedEdit_IsNotServed()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
        var rootId = await SaveChainAsync(redb, "dirtyguard");
        ((RedbServiceBase)redb).PropsCache.Instance!.Clear();

        // Eager load, so the nested object is LOADED inside the cached graph; an untouched graph
        // is served as a hit - the same shared instance.
        var cached = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 10);
        (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 10)).Should().BeSameAs(cached, "an untouched graph is a hit");

        // The bug the guard exists for, one level down: edit the nested object, do not save.
        cached!.Props.Next!.Props.Label = "phantom";

        var served = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 10);
        served.Should().NotBeSameAs(cached, "a graph with an unsaved nested edit must not be served from the cache");
        served!.Props.Next!.Props.Label.Should().Be("mid-dirtyguard", "the caller gets the committed state");

        // The batch path refuses it the same way (served is the cached instance now).
        served.Props.Next.Props.Label = "phantom-2";
        var batch = (RedbObject<LazyNodeProps>)(await redb.LoadAsync(new[] { rootId }, depth: 10))[0];
        batch.Props.Next!.Props.Label.Should().Be("mid-dirtyguard");

        // The inspection never wakes a stub: a depth-1 graph stays lazy through a hit.
        ((RedbServiceBase)redb).PropsCache.Instance!.Clear();
        var lazyRoot = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        var again = await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1);
        again.Should().BeSameAs(lazyRoot);
        again!.Props.Next!.IsPropsLoaded.Should().BeFalse("the walk must not wake a stub");
    }
    // A reference of a cached object read in another live scope: ReaderConnectionTestsBase (owner decision 2026-09-15 -
    // the load runs on the reader's connection, no scope of its own).

    /// <summary>
    /// The writer keeps its own cached instance - an application-level cache - and touches a reference after its scope
    /// ended. A data object owns no connection (owner decision 2026-09-15): with no live redb scope reading, the load is
    /// refused by default and runs in a fresh scope only when the configuration asks for it. Seeding and the writer's scope
    /// run in this helper, so nothing it resolves stays current for the caller.
    /// </summary>
    private async Task<RedbObject<LazyNodeProps>> WritersCachedInstanceAfterItsScopeEndedAsync(ServiceProvider sp, string tag)
    {
        long rootId;
        await using (var seed = sp.CreateAsyncScope())
        {
            var redb = seed.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<LazyNodeProps>();
            await redb.InitializeTypeRegistryAsync();
            rootId = await SaveChainAsync(redb, tag);
            ((RedbServiceBase)redb).PropsCache.Instance!.Clear();
        }

        RedbObject<LazyNodeProps> cachedRoot;
        await using (var writer = sp.CreateAsyncScope())
            cachedRoot = (await writer.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<LazyNodeProps>(rootId, depth: 1))!;

        cachedRoot.Props.Next!.IsPropsLoaded.Should().BeFalse("precondition: the reference is a stub");
        return cachedRoot;
    }

    [Fact]
    public async Task CachedObject_WriterScopeEnded_ReadRefuses_WithoutALiveScope()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var cachedRoot = await WritersCachedInstanceAfterItsScopeEndedAsync(sp, "writer-ended-refuse");
        var stub = cachedRoot.Props.Next!;

        Action read = () => _ = stub.Props;
        read.Should().Throw<RedbLazyLoadScopeEndedException>(
            "no live redb scope reads here, and no scope is opened behind the reader's back");
        stub.IsPropsLoaded.Should().BeFalse("a refusal loads nothing and memoises nothing");
    }

    [Fact]
    public async Task CachedObject_WriterScopeEnded_SyncGetter_LoadsInAFreshScope_WhenConfigured()
    {
        await using var sp = Build(c =>
        {
            c.EnablePropsCache = true;
            c.LazyLoadWithoutScope = LazyLoadWithoutScopeMode.FreshScope;
        });
        var cachedRoot = await WritersCachedInstanceAfterItsScopeEndedAsync(sp, "writer-ended-sync");

        cachedRoot.Props.Next!.Props.Label.Should().Be("mid-writer-ended-sync",
            "the configuration asks for a fresh scope when no live scope reads");
    }

    [Fact]
    public async Task CachedObject_WriterScopeEnded_AsyncLoad_LoadsInAFreshScope_WhenConfigured()
    {
        await using var sp = Build(c =>
        {
            c.EnablePropsCache = true;
            c.LazyLoadWithoutScope = LazyLoadWithoutScopeMode.FreshScope;
        });
        var cachedRoot = await WritersCachedInstanceAfterItsScopeEndedAsync(sp, "writer-ended-async");

        await cachedRoot.Props.Next!.LoadPropsAsync();
        cachedRoot.Props.Next.IsPropsLoaded.Should().BeTrue();
        cachedRoot.Props.Next.Props.Label.Should().Be("mid-writer-ended-async",
            "the configuration asks for a fresh scope when no live scope reads");
    }

    /// <summary>Seeds a chain and loads its root in a scope that ends - both in this helper, so nothing stays current.</summary>
    private async Task<RedbObject<LazyNodeProps>> StubFromAScopeThatEndsAsync(ServiceProvider sp)
    {
        long rootId;
        await using (var seed = sp.CreateAsyncScope())
        {
            var boot = seed.ServiceProvider.GetRequiredService<IRedbService>();
            await boot.InitializeAsync(ensureCreated: true);
            await boot.SyncSchemeAsync<LazyNodeProps>();
            await boot.InitializeTypeRegistryAsync();
            rootId = await SaveChainAsync(boot, "scope-ended");
        }

        await using var a = sp.CreateAsyncScope();
        var root = (await a.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        return root.Props.Next!;
    }

    [Fact]
    public async Task ScopeEnded_ReferenceNotCached_GetterRefusesLoudly()
    {
        await using var sp = Build(_ => { });
        var stub = await StubFromAScopeThatEndsAsync(sp);

        // The scope that loaded the object is gone, and no other redb scope reads here.
        Action act = () => _ = stub.Props;
        act.Should().Throw<RedbLazyLoadScopeEndedException>("no resurrected pooled connection, a clear refusal instead")
            .Which.ObjectId.Should().Be(stub.id);
        var actAsync = async () => await stub.LoadPropsAsync();
        await actAsync.Should().ThrowAsync<RedbLazyLoadScopeEndedException>();
        stub.IsPropsLoaded.Should().BeFalse("a refusal loads nothing and memoises nothing");

        // The way out the message names: read it inside a live scope - the very same stub.
        await using var b = sp.CreateAsyncScope();
        b.ServiceProvider.GetRequiredService<IRedbService>().Should().NotBeNull("this scope is now current for the reads below");
        stub.Props.Label.Should().Be("mid-scope-ended");
    }
    /// <summary>A message loop that is busy blocking: whatever is posted to it never runs.</summary>
    private sealed class BlockedUiContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state) { }
        public override void Send(SendOrPostCallback d, object? state)
            => throw new InvalidOperationException("Send on a blocked UI context");
    }
}
