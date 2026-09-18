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
/// A shared instance inside a transaction (owner decision 2026-09-17, replacing the rule of 2026-09-15). While the
/// transaction has written nothing, what its connection reads is committed state, so a shared instance keeps the lazy
/// reference it loads - the writer's edit of <c>root.Props.Next.Props</c> followed by <c>SaveAsync(root.Props.Next)</c>
/// is saved, whether the root was loaded before the transaction or served from the cache inside it - and forgets it
/// if the transaction rolls back. Once the transaction has written, what it reads may be its own uncommitted work:
/// the shared instance keeps nothing of it, and saving that stub is refused loudly.
/// </summary>
public abstract class PropsCacheTransactionKeepTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(string domain)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = true;
                c.CacheDomain = $"props-cache-keep-{GetType().Name}-{domain}";
                // The objects behind list items load on first touch, not with the list: the list fact reads item.Object
                // inside the transaction.
                c.PreloadListItemLinkedObjects = false;
            });
        });
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

    /// <summary>Seeds and loads the root once, so the root the test gets is the cached, shared instance.</summary>
    private static async Task<(long RootId, long NextId)> SeedSharedChainAsync(IRedbService redb, string tag)
    {
        var nextId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"keep-next-{tag}", Props = new LazyNodeProps { Label = "v1" } });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"keep-root-{tag}",
            Props = new LazyNodeProps { Label = "root", Next = new RedbObject<LazyNodeProps> { id = nextId } }
        });
        return (rootId, nextId);
    }

    private static async Task<RedbObject<LazyNodeProps>> LoadSharedRootAsync(IRedbService redb, long rootId)
    {
        var root = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        root.Props.Next!.IsPropsLoaded.Should().BeFalse("precondition: at depth 1 the reference is a stub");
        (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1)).Should().BeSameAs(root,
            "precondition: the root is served from the cache - a shared instance");
        return root;
    }

    private static async Task<string?> LabelInAFreshScopeAsync(ServiceProvider sp, long id)
    {
        await using var scope = sp.CreateAsyncScope();
        var reloaded = await scope.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<LazyNodeProps>(id);
        return reloaded!.Props.Label;
    }

    [Fact]
    public async Task AnEditOfALazyReference_OfAnObjectLoadedBeforeTheTransaction_IsSaved()
    {
        await using var sp = Build("before");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, nextId) = await SeedSharedChainAsync(redb, NewTag());
        var root = await LoadSharedRootAsync(redb, rootId);

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            root.Props.Next!.Props.Label = "v2";
            await redb.SaveAsync(root.Props.Next);
        });

        (await LabelInAFreshScopeAsync(sp, nextId)).Should().Be("v2",
            "the transaction had written nothing when the reference loaded: what it read is committed state, kept on the instance");
    }

    [Fact]
    public async Task AnEditOfALazyReference_OfACachedObject_InsideACleanTransaction_IsSaved()
    {
        await using var sp = Build("clean");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, nextId) = await SeedSharedChainAsync(redb, NewTag());

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            var root = await LoadSharedRootAsync(redb, rootId);
            root.Props.Next!.Props.Label = "v2";
            await redb.SaveAsync(root.Props.Next);
        });

        (await LabelInAFreshScopeAsync(sp, nextId)).Should().Be("v2");
    }

    [Fact]
    public async Task AnEditOfALazyReference_OfACachedObject_InsideACleanAmbientTransaction_IsSaved()
    {
        await using var sp = Build("clean-ambient");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, nextId) = await SeedSharedChainAsync(redb, NewTag());

        using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            var root = await LoadSharedRootAsync(redb, rootId);
            root.Props.Next!.Props.Label = "v2";
            await redb.SaveAsync(root.Props.Next);
            tx.Complete();
        }

        (await LabelInAFreshScopeAsync(sp, nextId)).Should().Be("v2");
    }

    /// <summary>
    /// The boundary of the rule: after the transaction has written, what its connection reads may be its own
    /// uncommitted work, so the shared instance keeps nothing - and saving the stub is refused, never a fresh reload.
    /// </summary>
    [Fact]
    public async Task AnEditOfALazyReference_AfterAWriteInTheTransaction_IsRefusedAtSave()
    {
        await using var sp = Build("dirty");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, _) = await SeedSharedChainAsync(redb, NewTag());

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"keep-write-{NewTag()}", Props = new SimpleProps { Title = "written" } });
            var root = await LoadSharedRootAsync(redb, rootId);
            root.Props.Next!.Props.Label = "v2";
            root.Props.Next.IsPropsLoaded.Should().BeFalse("a shared stub keeps nothing a writing transaction reads");

            var act = async () => await redb.SaveAsync(root.Props.Next);
            await act.Should().ThrowAsync<RedbUnloadedReferenceException>();
        });
    }

    [Fact]
    public async Task ALazyLoadKeptInsideARolledBackTransaction_IsForgotten()
    {
        await using var sp = Build("rollback");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var (rootId, _) = await SeedSharedChainAsync(redb, NewTag());
        var root = await LoadSharedRootAsync(redb, rootId);

        var act = async () => await redb.Context.ExecuteAtomicAsync(async () =>
        {
            root.Props.Next!.Props.Label.Should().Be("v1");
            root.Props.Next.IsPropsLoaded.Should().BeTrue("inside a clean transaction the shared stub keeps what it loaded");
            await Task.Yield();
            throw new InvalidOperationException("boom");
        });
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("boom");

        root.Props.Next!.IsPropsLoaded.Should().BeFalse("what a rolled-back transaction loaded is forgotten");
        root.Props.Next.Props.Label.Should().Be("v1", "the next read loads again");
    }

    [Fact]
    public async Task AnEditOfAListItemObject_OfACachedList_InsideATransaction_IsSaved()
    {
        await using var sp = Build("list");
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();

        var objectId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"keep-item-{tag}", Props = new SimpleProps { Title = "v1" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"keep-list-{tag}", "keep"));
        await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = objectId });
        // Cached - and so shared - before the transaction.
        var items = await redb.ListProvider.GetListItemsAsync(list.Id);
        (await redb.ListProvider.GetListItemsAsync(list.Id)).Should().BeSameAs(items, "precondition: the list is served from the cache");

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            var item = items.Single();
            ((RedbObject<SimpleProps>)item.Object!).Props.Title = "v2";
            await redb.SaveAsync((RedbObject<SimpleProps>)item.Object!);
        });

        await using var reader = sp.CreateAsyncScope();
        var reloaded = await reader.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<SimpleProps>(objectId);
        reloaded!.Props.Title.Should().Be("v2", "item.Object read twice inside a clean transaction is one instance");
    }
}
