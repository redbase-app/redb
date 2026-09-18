using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// A service resolved from the root provider is captive: it lives as long as the process, and the ambient frame it
/// enters is inherited by every flow (review after 4.0.0, finding 4; owner decision 2026-09-17: the concept stays - the
/// captive service is the reader - but outside a transaction it lends lazy loads its scope factory, never its one
/// connection). Before, every parallel lazy load of a shared instance ran on that one connection and the command gate
/// refused the second. Inside its own transaction the connection is the transaction, and a lazy load stays on it.
/// </summary>
public class SqliteCaptiveServiceReaderTests
{
    private const int Parallelism = 16;

    [Fact]
    public async Task ParallelLazyLoads_OnSharedInstances_ThroughACaptiveService_AllSucceed()
    {
        await using var provider = Build("props");
        // From the container itself, on purpose: the captive service of a Program.cs. Resolved in this flow: the frame the
        // service enters is an AsyncLocal, and one set inside an awaited helper would not flow back here.
        var redb = provider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var rootId = await SaveRootWithChildrenAsync(redb, tag);

        // Cached, so shared: its stubs read on the ambient reader of whoever touches them - the captive service.
        var root = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        var stubs = root.Props.Children!;
        stubs.Should().HaveCount(Parallelism);
        stubs.Should().OnlyContain(s => !s.IsPropsLoaded, "precondition: at depth 1 the references are stubs");

        // Every flow touches its stub at the same moment: on one connection the command gate refuses all but the first.
        var labels = await AtOnce(stubs, stub => stub.Props!.Label!);

        labels.Should().BeEquivalentTo(Enumerable.Range(0, Parallelism).Select(i => $"child-{i}"),
            "a captive reader lends each lazy load a fresh scope of its container instead of its one connection");
    }

    [Fact]
    public async Task ParallelListItemObjects_OfACachedList_ThroughACaptiveService_AllSucceed()
    {
        await using var provider = Build("items");
        // From the container itself, on purpose: the captive service of a Program.cs. Resolved in this flow: the frame the
        // service enters is an AsyncLocal, and one set inside an awaited helper would not flow back here.
        var redb = provider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"captive-list-{tag}", "captive"));
        for (var i = 0; i < Parallelism; i++)
        {
            var objectId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"captive-item-{tag}-{i}", Props = new LazyNodeProps { Label = $"item-{i}" } });
            await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = $"item-{i}", IdObject = objectId });
        }

        // Cached, so shared; the objects behind the items load on first touch (PreloadListItemLinkedObjects = false).
        var items = await redb.ListProvider.GetListItemsAsync(list.Id);
        (await redb.ListProvider.GetListItemsAsync(list.Id)).Should().BeSameAs(items, "precondition: the list is served from the cache");
        items.Should().HaveCount(Parallelism);

        var labels = await AtOnce(items, item => ((RedbObject<LazyNodeProps>)item.Object!).Props.Label!);

        labels.Should().BeEquivalentTo(Enumerable.Range(0, Parallelism).Select(i => $"item-{i}"),
            "a captive reader lends each list item load a fresh scope of its container instead of its one connection");
    }

    [Fact]
    public async Task ALazyLoad_InsideTheCaptiveServicesTransaction_ReadsWhatTheTransactionWrote()
    {
        await using var provider = Build("tx");
        // From the container itself, on purpose: the captive service of a Program.cs. Resolved in this flow: the frame the
        // service enters is an AsyncLocal, and one set inside an awaited helper would not flow back here.
        var redb = provider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();

        await using var transaction = await redb.Context.BeginTransactionAsync();
        var childId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"captive-tx-child-{tag}", Props = new LazyNodeProps { Label = "written-here" } });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"captive-tx-root-{tag}",
            Props = new LazyNodeProps { Label = "root", Next = new RedbObject<LazyNodeProps> { id = childId } }
        });
        var root = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        root.Props.Next!.IsPropsLoaded.Should().BeFalse("precondition: at depth 1 the reference is a stub");

        // Inside its transaction the captive service's connection is the transaction: the stub loads on it and sees the
        // uncommitted child. A fresh scope would read committed state, where the child does not exist yet.
        root.Props.Next.Props!.Label.Should().Be("written-here");
        await transaction.RollbackAsync();
    }

    [Fact]
    public async Task AnObjectLoadedThroughALentScope_LeavesStubsThatLoadAgain()
    {
        // No props cache: a cached instance is shared, and a shared instance never loads on an origin (only on a live
        // scope of the reader, owner decision 2026-09-15). This fact is about the origin of what a lent scope materializes.
        await using var provider = Build("chain", propsCache: false);
        // Resolved on another thread, like a test fixture or a host built elsewhere: the frame the service enters stays
        // there, and this flow has no live scope - a read here relies on the origin of what it touches.
        var redb = await Task.Run(() => provider.GetRequiredService<IRedbService>());
        await BootAsync(redb);
        var tag = NewTag();
        var leafId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"captive-leaf-{tag}", Props = new LazyNodeProps { Label = "leaf" } });
        var midId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"captive-mid-{tag}",
            Props = new LazyNodeProps { Label = "mid", Next = new RedbObject<LazyNodeProps> { id = leafId } }
        });
        var rootId = await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"captive-root-{tag}",
            Props = new LazyNodeProps { Label = "root", Next = new RedbObject<LazyNodeProps> { id = midId } }
        });

        // No scope is current here: the stub loads on its origin, the captive service, which lends a scope.
        var root = (await redb.LoadAsync<LazyNodeProps>(rootId, depth: 1))!;
        var mid = root.Props.Next!.Props!;
        mid.Label.Should().Be("mid");
        var leaf = mid.Next!;
        leaf.IsPropsLoaded.Should().BeFalse("precondition: the lent scope loads one object, its references stay stubs");

        // The lent scope ended with the load. What it materialized belongs to the captive reader, not to that scope:
        // the stub under it names the captive service as its origin, and loads through a lent scope of its own.
        leaf.Props!.Label.Should().Be("leaf");
    }

    private static ServiceProvider Build(string suffix, bool propsCache = true)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_captive_{suffix}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c =>
            {
                c.EnablePropsCache = propsCache;
                c.PreloadListItemLinkedObjects = false;
                c.CacheDomain = $"captive-{suffix}-{Guid.NewGuid():N}";
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<LazyNodeProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    private static async Task<long> SaveRootWithChildrenAsync(IRedbService redb, string tag)
    {
        var children = new List<RedbObject<LazyNodeProps>>();
        for (var i = 0; i < Parallelism; i++)
        {
            var childId = await redb.SaveAsync(new RedbObject<LazyNodeProps> { name = $"captive-child-{tag}-{i}", Props = new LazyNodeProps { Label = $"child-{i}" } });
            children.Add(new RedbObject<LazyNodeProps> { id = childId });
        }
        return await redb.SaveAsync(new RedbObject<LazyNodeProps>
        {
            name = $"captive-root-{tag}",
            Props = new LazyNodeProps { Label = "root", Children = children }
        });
    }

    /// <summary>Runs <paramref name="read"/> on every element at the same moment, each on its own thread pool thread.</summary>
    private static async Task<string[]> AtOnce<T>(IEnumerable<T> elements, Func<T, string> read)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = elements.Select(element => Task.Run(async () =>
        {
            await start.Task;
            return read(element);
        })).ToArray();
        start.SetResult();
        return await Task.WhenAll(reads);
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];
}
