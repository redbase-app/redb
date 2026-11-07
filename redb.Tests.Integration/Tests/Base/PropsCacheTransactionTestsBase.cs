using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The props cache and the list cache publish committed state only (review after 4.0.0, findings 1-2). A cache Set
/// marked the graph shared at once, and a shared instance does not keep what one transaction saw (owner decision
/// 2026-09-15) - so inside a transaction the writer's own loaded object never kept its lazy references either:
/// <c>root.Props.Next.Props.Label = ...; SaveAsync(root.Props.Next)</c> saved a fresh reload and the edit was lost, and
/// every read of <c>item.Object</c> was a query. Now a Set inside a transaction waits for its commit: until then the
/// instance is the writer's own, and a rolled-back transaction leaves nothing in the cache.
/// </summary>
public abstract class PropsCacheTransactionTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(string domain, bool skipHashValidation = false)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = true;
                c.SkipHashValidationOnCacheCheck = skipHashValidation;
                c.CacheDomain = $"props-cache-tx-{GetType().Name}-{domain}";
            });
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    /// <summary>
    /// Seeds in a cache domain of its own: the objects reach the domain under test only through the loads of the test - the
    /// scenario is the writer's own load, not an instance served from the cache.
    /// </summary>
    private async Task<(long RootId, long NextId)> SeedChainInAnotherDomainAsync(string domain)
    {
        await using var seeder = Build($"{domain}-seed");
        await using var scope = seeder.CreateAsyncScope();
        var writer = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(writer);
        return await SeedChainAsync(writer, NewTag());
    }

    private static async Task<(long RootId, long NextId)> SeedChainAsync(IRedbService redb, string tag)
    {
        var nextId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"cachetx-next-{tag}", Props = new LazyNodeProps { Label = "v1" } });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"cachetx-root-{tag}",
            Props = new LazyNodeProps { Label = "root", Next = new RedbObject<LazyNodeProps> { id = nextId } }
        });
        return (rootId, nextId);
    }

    private static async Task EditNextAsync(IRedbService redb, long rootId, long nextId)
    {
        var root = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        root.Props.Next!.IsPropsLoaded.Should().BeFalse("precondition: at depth 1 the reference is a stub");
        root.Props.Next.id.Should().Be(nextId, "precondition: the stub names the referenced object");
        root.Props.Next._lazyLoader.Should().NotBeNull("precondition: the stub carries the loader of its database");
        root.Props.Next.Props.Label = "v2";
        await redb.SaveAsync(root.Props.Next);
    }

    private static async Task<string?> LabelInAFreshScopeAsync(ServiceProvider sp, long id)
    {
        await using var scope = sp.CreateAsyncScope();
        var reloaded = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<LazyNodeProps>(id);
        return reloaded!.Props.Label;
    }

    /// <summary>
    /// The instance a save caches is the caller's own graph, hand-made references included: a reference written as
    /// <c>new RedbObject&lt;T&gt; { id = x }</c> carried no loader, so a load served from the cache returned that very
    /// stub and its <c>Props</c> came back null - no query, no exception. The load path installs the loaders before
    /// the cache Set; the save path must too.
    /// </summary>
    [Fact]
    public async Task ASavedObjectServedFromTheCache_LoadsItsHandMadeReferences()
    {
        await using var sp = Build("saved");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, _) = await SeedChainAsync(redb, NewTag());

        var served = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;

        served.Props.Next!._lazyLoader.Should().NotBeNull("a cached graph's stubs load on the reader's scope");
        served.Props.Next.Props.Should().NotBeNull("the reference loads on first touch");
        served.Props.Next.Props.Label.Should().Be("v1");
    }

    [Fact]
    public async Task AnEditOfALazyReference_InsideAnExplicitTransaction_IsSaved()
    {
        await using var sp = Build("explicit");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, nextId) = await SeedChainInAnotherDomainAsync("explicit");

        await redb.Context.ExecuteAtomicAsync(() => EditNextAsync(redb, rootId, nextId));

        (await LabelInAFreshScopeAsync(sp, nextId)).Should().Be("v2",
            "the edit was made on the instance the save read: the object a writer loaded is not shared before its " +
            "transaction commits");
    }

    [Fact]
    public async Task AnEditOfALazyReference_InsideAnAmbientTransaction_IsSaved()
    {
        await using var sp = Build("ambient");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, nextId) = await SeedChainInAnotherDomainAsync("ambient");

        using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await EditNextAsync(redb, rootId, nextId);
            tx.Complete();
        }

        (await LabelInAFreshScopeAsync(sp, nextId)).Should().Be("v2");
    }

    // A root served from the cache, edited through its stub inside a transaction: see PropsCacheTransactionKeepTestsBase -
    // kept while the transaction has written nothing, refused at save once it has (owner decision 2026-09-17).

    [Fact]
    public async Task ALoadInsideARolledBackTransaction_LeavesNoCacheEntry()
    {
        // Seeded in one cache domain, exercised in another: the object reaches the second cache only through the load
        // inside the transaction, if at all.
        long id;
        await using (var seeder = Build("rollback-seed", skipHashValidation: true))
        await using (var seedScope = seeder.CreateAsyncScope())
        {
            var writer = seedScope.ServiceProvider.GetRequiredService<IRedbService>();
            await BootAsync(writer);
            id = await writer.SaveAsync(new RedbObject<SimpleProps> { name = $"cachetx-rollback-{NewTag()}", Props = new SimpleProps { Title = "seed" } });
        }

        await using var sp = Build("rollback", skipHashValidation: true);
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);

        var act = async () => await redb.Context.ExecuteAtomicAsync(async () =>
        {
            await redb.LoadAsync<SimpleProps>(id);
            throw new InvalidOperationException("boom");
        });
        await act.Should().ThrowAsync<InvalidOperationException>();

        _recorder.Start();
        await redb.LoadAsync<SimpleProps>(id);
        var commands = _recorder.Stop();

        commands.Should().NotBeEmpty(
            "nothing of a rolled-back transaction reaches the cache, so the next load reads the database - without hash " +
            "validation a cache hit would have read nothing");
    }

    [Fact]
    public async Task AnEditOfAListItemObject_InsideATransaction_IsSaved()
    {
        await using var sp = Build("list");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();

        var objectId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"cachetx-item-{tag}", Props = new SimpleProps { Title = "v1" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"cachetx-list-{tag}", "cachetx"));
        await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = objectId });

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            var item = (await redb.ListProvider.GetListItemsAsync(list.Id)).Single();
            ((RedbObject<SimpleProps>)item.Object!).Props.Title = "v2";
            await redb.SaveAsync((RedbObject<SimpleProps>)item.Object!);
        });

        await using var reader = sp.CreateAsyncScope();
        var reloaded = await reader.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<SimpleProps>(objectId);
        reloaded!.Props.Title.Should().Be("v2",
            "item.Object read twice inside a transaction is one instance: the items a reader loaded are not shared " +
            "before the transaction commits");
    }
}
