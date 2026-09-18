using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Owner decision 2026-09-15 (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §4.1): a data object owns no connection. A
/// lazy load - <see cref="RedbListItem.Object"/>, the Props of a reference stub - runs on the live redb scope of whoever
/// reads it. The old model opened a fresh scope and pooled connection per read for every list item without a loader of
/// its own (all Pro and Free materialization) and for every reference inside a cached object.
/// <para>
/// The detector is the context a load's SQL ran on: the reader's own context, and no context created meanwhile. Seeding
/// and "a scope that ended" run in async helper methods: whatever they resolve never stays current for the test body.
/// </para>
/// </summary>
public abstract class ReaderConnectionTestsBase
{
    private readonly SqlRecorder _recorder = new();

    /// <summary>Registers the provider on the options builder (Free or Pro, see <see cref="Register"/>).</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    /// <summary>A scalar command that holds the connection for a couple of seconds on this database.</summary>
    protected abstract string SlowScalarSql { get; }

    private ServiceProvider Build(Action<RedbServiceConfiguration> configure, string domainSuffix = "")
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
                // One domain per suite (the Free and Pro suites of one database run in parallel) and per cache setting:
                // a props cache is kept per domain for the process, so a test with the cache on would leave it to the next.
                c.CacheDomain = $"reader-connection-{GetType().Name}-{(c.EnablePropsCache ? "cache" : "nocache")}{domainSuffix}";
            });
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<CtProbeProps>();
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    private sealed record ItemSeed(long RootId, long LinkedObjectId);

    /// <summary>A root whose list item is linked to an object.</summary>
    private static async Task<ItemSeed> SeedLinkedItemAsync(ServiceProvider sp)
    {
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var linkedObjectId = await redb.SaveAsync(new RedbObject<SimpleProps>
            { name = $"reader-linked-{tag}", Props = new SimpleProps { Title = "linked" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"reader-{tag}", "reader"));
        var linked = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = linkedObjectId });
        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps>
            { name = $"reader-root-{tag}", Props = new CtProbeProps { Label = "root", Status = linked } });
        return new ItemSeed(rootId, linkedObjectId);
    }

    /// <summary>root → mid → leaf; the props cache is cleared, so the next load goes to the database.</summary>
    private static async Task<long> SeedChainAsync(ServiceProvider sp, string tag)
    {
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var leafId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"reader-leaf-{tag}", Props = new LazyNodeProps { Label = $"leaf-{tag}" } });
        var midId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"reader-mid-{tag}", Props = new LazyNodeProps { Label = $"mid-{tag}", Next = new RedbObject<LazyNodeProps> { id = leafId } } });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
            { name = $"reader-root-{tag}", Props = new LazyNodeProps { Label = $"root-{tag}", Next = new RedbObject<LazyNodeProps> { id = midId } } });
        ((RedbServiceBase)redb).PropsCache.Instance?.Clear();
        return rootId;
    }

    /// <summary>A scope loads the root at depth 1 - caching it when the props cache is on - and ends.</summary>
    private static async Task<RedbObject<LazyNodeProps>> LoadRootInAScopeThatEndsAsync(ServiceProvider sp, long rootId)
    {
        await using var scope = sp.CreateAsyncScope();
        var root = (await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        root.Props.Next!.IsPropsLoaded.Should().BeFalse("precondition: the reference is a stub");
        return root;
    }

    private static async Task<RedbListItem> ItemFromAScopeThatEndsAsync(ServiceProvider sp, ItemSeed seed)
    {
        await using var scope = sp.CreateAsyncScope();
        var root = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<CtProbeProps>(seed.RootId, depth: 10);
        return root!.Props.Status!;
    }

    private void AssertOnTheReadersContext(string path, IReadOnlyList<RecordedCommand> commands, IRedbContext reader)
    {
        _recorder.ContextsCreated.Should().Be(0, $"{path}: a lazy load uses the reader's scope and opens no scope of its own");
        commands.Should().NotBeEmpty($"{path}: the load ran its SQL");
        commands.Should().OnlyContain(c => ReferenceEquals(c.Context, reader),
            $"{path}: every command of the load ran on the reader's context - its one connection");
    }

    private static void AssertLinkedItem(RedbListItem? item, long linkedObjectId)
    {
        item.Should().NotBeNull("precondition: the list item is materialized");
        item!.IdObject.Should().Be(linkedObjectId, "precondition: the item carries its object link");
        item.IsObjectLoaded.Should().BeFalse("precondition: the object is still lazy");
    }

    [Fact]
    public async Task ListItemFromALoad_ObjectLoadsOnTheReadersContext()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var root = (await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 10))!;
        AssertLinkedItem(root.Props.Status, seed.LinkedObjectId);

        _recorder.Start();
        var linked = root.Props.Status!.Object;
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item from LoadAsync", commands, reader);
    }

    [Fact]
    public async Task ListItemFromAQuery_ObjectLoadsOnTheReadersContext()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var rows = await redb.Query<CtProbeProps>().WhereRedb(o => o.Id == seed.RootId).ToListAsync();
        var item = rows.Should().ContainSingle().Subject.Props.Status;
        AssertLinkedItem(item, seed.LinkedObjectId);

        _recorder.Start();
        var linked = await item!.GetObjectAsync();
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item from a query", commands, reader);
    }

    [Fact]
    public async Task CachedObjectInAnotherScope_ReferenceLoadsOnTheReadersContext()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var tag = NewTag();
        var rootId = await SeedChainAsync(sp, tag);
        var cached = await LoadRootInAScopeThatEndsAsync(sp, rootId);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        served.Should().BeSameAs(cached, "precondition: the cache serves the shared instance");
        var stub = served.Props.Next!;
        stub.IsPropsLoaded.Should().BeFalse("precondition: the reference is a stub");

        _recorder.Start();
        var label = stub.Props.Label;
        var commands = _recorder.Stop();

        label.Should().Be($"mid-{tag}");
        AssertOnTheReadersContext("reference inside a cached object", commands, reader);
    }

    [Fact]
    public async Task ListItemRead_WithoutALiveScope_Refuses()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);
        var item = await ItemFromAScopeThatEndsAsync(sp, seed);
        AssertLinkedItem(item, seed.LinkedObjectId);

        _recorder.Start();
        Action read = () => _ = item.Object;
        read.Should().Throw<InvalidOperationException>().WithMessage("*BeginAccess*",
            "no live redb scope reads here: a clear refusal naming the explicit ways, not a hidden scope and connection");
        _recorder.Stop();
        _recorder.ContextsCreated.Should().Be(0);
    }

    [Fact]
    public async Task ReferenceRead_WithoutALiveScope_Refuses()
    {
        await using var sp = Build(_ => { });
        var rootId = await SeedChainAsync(sp, NewTag());
        var root = await LoadRootInAScopeThatEndsAsync(sp, rootId);
        var stub = root.Props.Next!;

        _recorder.Start();
        Action read = () => _ = stub.Props;
        read.Should().Throw<InvalidOperationException>().WithMessage("*BeginAccess*",
            "no live redb scope reads here: a clear refusal naming the explicit ways, not a hidden scope and connection");
        _recorder.Stop();
        _recorder.ContextsCreated.Should().Be(0);
    }

    [Fact]
    public async Task CachedReferenceInsideATransaction_SeesTheTransaction_AndIsNotMemoized()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var tag = NewTag();
        var rootId = await SeedChainAsync(sp, tag);
        var cached = await LoadRootInAScopeThatEndsAsync(sp, rootId);
        var midId = cached.Props.Next!.id;

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = sp.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var mid = (await redb.LoadAsync<LazyNodeProps>(midId, depth: 1))!;
            mid.Props.Label = $"uncommitted-{tag}";
            await redb.SaveAsync(mid);

            var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
            served.Should().BeSameAs(cached, "precondition: the cache serves the shared instance");
            var stub = served.Props.Next!;

            stub.Props.Label.Should().Be($"uncommitted-{tag}",
                "the load runs on the reader's connection, inside the reader's transaction");
            stub.IsPropsLoaded.Should().BeFalse(
                "a shared instance does not keep what one transaction saw - the write may roll back");
            // not completed: the write rolls back
        }

        await using (var after = sp.CreateAsyncScope())
        {
            var redb = after.ServiceProvider.GetRequiredService<IRedbService>();
            var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
            served.Props.Next!.Props.Label.Should().Be($"mid-{tag}", "the shared instance never kept the rolled-back write");
        }
    }

    [Fact]
    public async Task InjectedListProvider_IsTheServicesOwn()
    {
        await using var sp = Build(_ => { });
        await using var scope = sp.CreateAsyncScope();

        scope.ServiceProvider.GetRequiredService<IListProvider>().Should().BeSameAs(
            scope.ServiceProvider.GetRequiredService<IRedbService>().ListProvider,
            "one list provider per scope - a second instance in the same scope behaves differently");
    }

    // === New API of the model: BeginAccess, the FreshScope option, the explicit batch ===

    /// <summary>Runs <paramref name="action"/> on a thread that carries none of the caller's execution context.</summary>
    private static void RunInAnotherExecutionContext(Action action)
    {
        Exception? failure = null;
        Thread thread;
        using (ExecutionContext.SuppressFlow())
        {
            thread = new Thread(() =>
            {
                try { action(); }
                catch (Exception ex) { failure = ex; }
            });
            thread.Start();
        }
        thread.Join();
        if (failure != null)
            throw new InvalidOperationException("the action failed on the other thread", failure);
    }

    [Fact]
    public async Task ItemReadWhereItsScopeIsNotCurrent_LoadsOnTheScopeThatMaterializedIt()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var origin = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var item = (await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 10))!.Props.Status!;
        AssertLinkedItem(item, seed.LinkedObjectId);

        // A test fixture, a service kept in a field, a UI event handler: the scope that materialized the item lives, but
        // is not current where the item is read (owner decision 2026-09-15: then the origin answers, while it lives).
        _recorder.Start();
        Core.Models.Contracts.IRedbObject? linked = null;
        RunInAnotherExecutionContext(() => linked = item.Object);
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item read where its scope is not current", commands, origin);
    }

    [Fact]
    public async Task SharedObjectReadWhereNoScopeIsCurrent_Refuses_AndLoadsOnTheScope_InsideBeginAccess()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var tag = NewTag();
        var rootId = await SeedChainAsync(sp, tag);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        // The load caches the root: from here on it is shared by every scope, not bound to this one.
        var stub = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!.Props.Next!;
        stub.IsPropsLoaded.Should().BeFalse("precondition: the reference is a stub");

        Exception? refused = null;
        RunInAnotherExecutionContext(() =>
        {
            try { _ = stub.Props; }
            catch (Exception ex) { refused = ex; }
        });
        refused.Should().BeOfType<Core.Exceptions.RedbLazyLoadScopeEndedException>(
            "a shared instance loads on the reader's scope only - it is never bound to the scope that cached it");
        stub.IsPropsLoaded.Should().BeFalse();

        _recorder.Start();
        string? label = null;
        RunInAnotherExecutionContext(() =>
        {
            using (redb.BeginAccess())
                label = stub.Props.Label;
        });
        var commands = _recorder.Stop();

        label.Should().Be($"mid-{tag}");
        AssertOnTheReadersContext("shared instance inside BeginAccess", commands, reader);
    }

    [Fact]
    public async Task ListItemRead_WithoutALiveScope_LoadsInAFreshScope_WhenConfigured()
    {
        await using var sp = Build(c => c.LazyLoadWithoutScope = LazyLoadWithoutScopeMode.FreshScope);
        var seed = await SeedLinkedItemAsync(sp);
        var item = await ItemFromAScopeThatEndsAsync(sp, seed);
        AssertLinkedItem(item, seed.LinkedObjectId);

        _recorder.Start();
        var linked = item.Object;
        _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        _recorder.ContextsCreated.Should().Be(1, "the option opens exactly one scope for the load");
    }

    [Fact]
    public async Task LoadLinkedObjectsAsync_LoadsTheItemsOnTheServicesContext()
    {
        await using var sp = Build(c => c.PreloadListItemLinkedObjects = false);
        long listId;
        long[] objectIds;
        await using (var seedScope = sp.CreateAsyncScope())
        {
            var seeder = seedScope.ServiceProvider.GetRequiredService<IRedbService>();
            await BootAsync(seeder);
            var tag = NewTag();
            objectIds = new long[3];
            for (var i = 0; i < objectIds.Length; i++)
                objectIds[i] = await seeder.SaveAsync(new RedbObject<SimpleProps> { name = $"reader-batch-{tag}-{i}", Props = new SimpleProps { Title = $"batch-{i}" } });
            var list = await seeder.ListProvider.SaveListAsync(RedbList.Create($"reader-batch-{tag}", "reader-batch"));
            listId = list.Id;
            foreach (var objectId in objectIds)
                await seeder.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = $"item-{objectId}", IdObject = objectId });
        }

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var items = await redb.ListProvider.GetListItemsAsync(listId);
        items.Should().HaveCount(objectIds.Length).And.OnlyContain(i => !i.IsObjectLoaded, "precondition: the preload is off");

        _recorder.Start();
        var loaded = await redb.LoadLinkedObjectsAsync(items);
        var commands = _recorder.Stop();

        loaded.Keys.Should().BeEquivalentTo(objectIds);
        items.Should().OnlyContain(i => i.IsObjectLoaded && i.Object != null && i.Object.Id == i.IdObject);
        AssertOnTheReadersContext("LoadLinkedObjectsAsync", commands, reader);
    }

    // === The rest of the §4.1 matrix: what a shared instance loads, parallel reads, two databases, every road ===

    private static async Task<RedbListItem> ListItemFromAScopeThatEndsAsync(ServiceProvider sp, long listId)
    {
        await using var scope = sp.CreateAsyncScope();
        var items = await scope.ServiceProvider.GetRequiredService<IRedbService>().ListProvider.GetListItemsAsync(listId);
        return items.Should().ContainSingle().Subject;
    }

    /// <summary>A list of <paramref name="count"/> items, each linked to an object of its own.</summary>
    private static async Task<long> SeedListOfLinkedObjectsAsync(ServiceProvider sp, int count, string label)
    {
        await using var scope = sp.CreateAsyncScope();
        var seeder = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(seeder);
        var tag = NewTag();
        var list = await seeder.ListProvider.SaveListAsync(RedbList.Create($"reader-{label}-{tag}", $"reader-{label}"));
        for (var i = 0; i < count; i++)
        {
            var objectId = await seeder.SaveAsync(new RedbObject<SimpleProps>
                { name = $"reader-{label}-{tag}-{i}", Props = new SimpleProps { Title = $"{label}-{i}" } });
            await seeder.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = $"item-{i}", IdObject = objectId });
        }
        return list.Id;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SharedObject_WhatItLoaded_IsShared_NotBoundToTheReadersScope(bool explicitAsyncLoad)
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var tag = NewTag();
        var rootId = await SeedChainAsync(sp, tag);
        var cached = await LoadRootInAScopeThatEndsAsync(sp, rootId);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        served.Should().BeSameAs(cached, "precondition: the cache serves the shared instance");
        var mid = served.Props.Next!;
        if (explicitAsyncLoad)
            await mid.LoadPropsAsync();
        else
            _ = mid.Props;
        mid.IsPropsLoaded.Should().BeTrue("outside a transaction a shared instance keeps what it loaded");
        var leaf = mid.Props.Next!;
        leaf.IsPropsLoaded.Should().BeFalse("precondition: a lazy load reaches depth 1, the next reference is a stub");

        Exception? refused = null;
        RunInAnotherExecutionContext(() =>
        {
            try { _ = leaf.Props; }
            catch (Exception ex) { refused = ex; }
        });
        refused.Should().BeOfType<Core.Exceptions.RedbLazyLoadScopeEndedException>(
            "a graph hung on a shared instance is shared too: it loads on the reader's scope only, never on the scope " +
            "that happened to load it");
    }

    [Fact]
    public async Task SharedObject_LoadPropsAsyncInsideATransaction_IsNotMemoized()
    {
        await using var sp = Build(c => c.EnablePropsCache = true);
        var tag = NewTag();
        var rootId = await SeedChainAsync(sp, tag);
        var cached = await LoadRootInAScopeThatEndsAsync(sp, rootId);
        var midId = cached.Props.Next!.id;

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = sp.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var mid = (await redb.LoadAsync<LazyNodeProps>(midId, depth: 1))!;
            mid.Props.Label = $"uncommitted-{tag}";
            await redb.SaveAsync(mid);

            var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
            served.Should().BeSameAs(cached, "precondition: the cache serves the shared instance");
            var stub = served.Props.Next!;

            await stub.LoadPropsAsync();
            stub.IsPropsLoaded.Should().BeFalse(
                "a shared instance does not keep what one transaction saw - the write may roll back");
            stub.Props.Label.Should().Be($"uncommitted-{tag}", "the reader still sees its own transaction");
            // not completed: the write rolls back
        }

        await using (var after = sp.CreateAsyncScope())
        {
            var redb = after.ServiceProvider.GetRequiredService<IRedbService>();
            var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
            served.Props.Next!.Props.Label.Should().Be($"mid-{tag}", "the shared instance never kept the rolled-back write");
        }
    }

    [Fact]
    public async Task CachedListItemInsideATransaction_SeesTheTransaction_AndIsNotMemoized()
    {
        await using var sp = Build(c => c.PreloadListItemLinkedObjects = false);
        var tag = NewTag();
        var listId = await SeedListOfLinkedObjectsAsync(sp, 1, $"cached-item-{tag}");
        var cachedItem = await ListItemFromAScopeThatEndsAsync(sp, listId);
        var objectId = cachedItem.IdObject!.Value;

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var scope = sp.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var obj = (await redb.LoadAsync<SimpleProps>(objectId))!;
            obj.Props.Title = $"uncommitted-{tag}";
            await redb.SaveAsync(obj);

            var item = (await redb.ListProvider.GetListItemsAsync(listId)).Should().ContainSingle().Subject;
            item.Should().BeSameAs(cachedItem, "precondition: the list cache serves the shared item");
            item.IsObjectLoaded.Should().BeFalse("precondition: the preload is off");

            ((RedbObject<SimpleProps>)item.Object!).Props.Title.Should().Be($"uncommitted-{tag}",
                "the load runs on the reader's connection, inside the reader's transaction");
            item.IsObjectLoaded.Should().BeFalse(
                "a shared item does not keep what one transaction saw - the write may roll back");
            // not completed: the write rolls back
        }

        await using (var after = sp.CreateAsyncScope())
        {
            var redb = after.ServiceProvider.GetRequiredService<IRedbService>();
            var item = (await redb.ListProvider.GetListItemsAsync(listId)).Should().ContainSingle().Subject;
            ((RedbObject<SimpleProps>)item.Object!).Props.Title.Should().NotBe($"uncommitted-{tag}",
                "the shared item never kept the rolled-back write");
        }
    }

    [Fact]
    public async Task CachedListItem_TheObjectItLoaded_IsShared_NotBoundToTheReadersScope()
    {
        await using var sp = Build(c => c.PreloadListItemLinkedObjects = false);
        var seed = await SeedLinkedItemAsync(sp);
        var tag = NewTag();
        long listId;
        await using (var seedScope = sp.CreateAsyncScope())
        {
            var seeder = seedScope.ServiceProvider.GetRequiredService<IRedbService>();
            var list = await seeder.ListProvider.SaveListAsync(RedbList.Create($"reader-shared-graph-{tag}", "reader-shared-graph"));
            listId = list.Id;
            await seeder.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "root", IdObject = seed.RootId });
        }
        var cachedItem = await ListItemFromAScopeThatEndsAsync(sp, listId);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var item = (await redb.ListProvider.GetListItemsAsync(listId)).Should().ContainSingle().Subject;
        item.Should().BeSameAs(cachedItem, "precondition: the list cache serves the shared item");
        var root = (RedbObject<CtProbeProps>)item.Object!;
        item.IsObjectLoaded.Should().BeTrue("outside a transaction a shared item keeps what it loaded");
        var nested = root.Props.Status;
        AssertLinkedItem(nested, seed.LinkedObjectId);

        Exception? refused = null;
        RunInAnotherExecutionContext(() =>
        {
            try { _ = nested!.Object; }
            catch (Exception ex) { refused = ex; }
        });
        refused.Should().BeOfType<Core.Exceptions.RedbLazyLoadScopeEndedException>(
            "the object a shared item keeps is shared too: its items load on the reader's scope only, never on the scope " +
            "that happened to load it");
    }

    [Fact]
    public async Task ParallelReadsInOneScope_AreRefused()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var item = (await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 10))!.Props.Status!;
        AssertLinkedItem(item, seed.LinkedObjectId);

        _recorder.Start();
        var slow = Task.Run(() => redb.Context.ExecuteScalarAsync<long>(SlowScalarSql));
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!_recorder.Peek().Any(c => c.Sql == SlowScalarSql) && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        // The recorder notes a command just before the connection takes it.
        await Task.Delay(300);

        Action read = () => _ = item.Object;
        read.Should().Throw<InvalidOperationException>().WithMessage("*concurrently*",
            "one scope is one connection: a second read while a command runs is refused, not given a connection of its own");
        await slow;
        _recorder.Stop();
        _recorder.ContextsCreated.Should().Be(0);
        item.IsObjectLoaded.Should().BeFalse("a refusal loads nothing and keeps nothing");
    }

    [Fact]
    public async Task TwoDatabasesInOneFlow_AnItemLoadsOnItsOwn_AndAnItemOfNoDatabaseIsRefused()
    {
        await using var spA = Build(_ => { }, "-a");
        await using var spB = Build(_ => { }, "-b");
        var seed = await SeedLinkedItemAsync(spA);

        await using var scopeA = spA.CreateAsyncScope();
        var redbA = scopeA.ServiceProvider.GetRequiredService<IRedbService>();
        var contextA = scopeA.ServiceProvider.GetRequiredService<IRedbContext>();
        var item = (await redbA.LoadAsync<CtProbeProps>(seed.RootId, depth: 10))!.Props.Status!;
        AssertLinkedItem(item, seed.LinkedObjectId);

        // A scope of the second database becomes current, nearer than the first.
        await using var scopeB = spB.CreateAsyncScope();
        scopeB.ServiceProvider.GetRequiredService<IRedbService>().Should().NotBeNull();

        _recorder.Start();
        var linked = item.Object;
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("an item of the first database while the second is nearer", commands, contextA);

        var unknown = new RedbListItem { IdObject = seed.LinkedObjectId };
        Action read = () => _ = unknown.Object;
        read.Should().Throw<InvalidOperationException>().WithMessage("*two databases*",
            "an item built in user code carries no database: with two current it is never read from a guessed one");
    }

    [Fact]
    public async Task LoadLinkedObjectsAsync_CommandsDoNotGrowWithTheItems()
    {
        await using var sp = Build(c => c.PreloadListItemLinkedObjects = false);
        var warmUp = await SeedListOfLinkedObjectsAsync(sp, 2, "batch-warm");
        var two = await SeedListOfLinkedObjectsAsync(sp, 2, "batch-two");
        var six = await SeedListOfLinkedObjectsAsync(sp, 6, "batch-six");

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        async Task<int> CommandsToLoad(long listId)
        {
            var items = await redb.ListProvider.GetListItemsAsync(listId);
            items.Should().OnlyContain(i => !i.IsObjectLoaded, "precondition: the preload is off");
            _recorder.Start();
            var loaded = await redb.LoadLinkedObjectsAsync(items);
            var commands = _recorder.Stop();
            loaded.Should().HaveCount(items.Count());
            return commands.Count;
        }

        await CommandsToLoad(warmUp);
        var forTwo = await CommandsToLoad(two);
        var forSix = await CommandsToLoad(six);
        forSix.Should().Be(forTwo, "one batch for the whole list: the commands do not grow with the number of items");
    }

    [Fact]
    public async Task ListItemFromASyncLoad_ObjectLoadsOnTheReadersContext()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var root = redb.Load<CtProbeProps>(seed.RootId, depth: 10)!;
        AssertLinkedItem(root.Props.Status, seed.LinkedObjectId);

        _recorder.Start();
        var linked = root.Props.Status!.Object;
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item from a sync Load", commands, reader);
    }

    [Fact]
    public async Task ListItemFromABatchLoad_ObjectLoadsOnTheReadersContext()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var loaded = await redb.LoadAsync(new[] { seed.RootId }, depth: 10);
        var root = loaded.Should().ContainSingle().Which.Should().BeOfType<RedbObject<CtProbeProps>>().Subject;
        AssertLinkedItem(root.Props.Status, seed.LinkedObjectId);

        _recorder.Start();
        var linked = await root.Props.Status!.GetObjectAsync();
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item from a batch load", commands, reader);
    }

    [Fact]
    public async Task ListItemFromATree_ObjectLoadsOnTheReadersContext()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedLinkedItemAsync(sp);
        long parentId;
        await using (var seedScope = sp.CreateAsyncScope())
        {
            var seeder = seedScope.ServiceProvider.GetRequiredService<IRedbService>();
            var status = (await seeder.LoadAsync<CtProbeProps>(seed.RootId, depth: 10))!.Props.Status!;
            var parent = new RedbObject<CtProbeProps> { name = $"reader-tree-parent-{NewTag()}", Props = new CtProbeProps { Label = "parent" } };
            parent.id = await seeder.SaveAsync(parent);
            parentId = parent.id;
            await seeder.CreateChildAsync(new TreeRedbObject<CtProbeProps>
                { name = "reader-tree-child", Props = new CtProbeProps { Label = "child", Status = status } }, parent);
        }

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reader = scope.ServiceProvider.GetRequiredService<IRedbContext>();
        var parentObj = (await redb.LoadAsync<CtProbeProps>(parentId, depth: 1))!;
        var child = (await redb.GetChildrenAsync<CtProbeProps>(parentObj)).Should().ContainSingle().Subject;
        AssertLinkedItem(child.Props.Status, seed.LinkedObjectId);

        _recorder.Start();
        var linked = await child.Props.Status!.GetObjectAsync();
        var commands = _recorder.Stop();

        linked.Should().NotBeNull();
        linked!.Id.Should().Be(seed.LinkedObjectId);
        AssertOnTheReadersContext("item from a tree", commands, reader);
    }
}
