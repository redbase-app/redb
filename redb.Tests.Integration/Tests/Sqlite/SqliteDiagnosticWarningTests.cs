using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Diagnostics a production host relies on (props cache under load, 2026-09-15): the service created its props cache
/// without a logger, and loads that rent a scope of their own went unnoticed when they ran one at a time. Such loads now
/// happen only by explicit option (LazyLoadWithoutScope = FreshScope) and are warned by rate. Every test builds its own
/// host and database file; services are resolved in helper methods, so no redb scope stays current for the test body.
/// </summary>
public class SqliteDiagnosticWarningTests
{
    private static ServiceProvider Build(string dbFile, CapturingLoggerProvider capture, Action<RedbServiceConfiguration> configure)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source={dbFile}";
        SqliteTestSupport.DeleteDbFiles(cs);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(capture).SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite(cs).Configure(c =>
        {
            c.CacheDomain = $"diagnostic-{dbFile}";
            configure(c);
        }));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task PropsCache_GetsTheServiceLogger_WorkingSetAboveTheLimitIsWarned()
    {
        var capture = new CapturingLoggerProvider();
        await using var sp = Build("redb_tests_diag_propscache.db", capture, c =>
        {
            c.EnablePropsCache = true;
            c.PropsCacheMaxSize = 10;
        });
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        ((RedbServiceBase)redb).PropsCache.Instance.Should().NotBeNull("the props cache must be active, or this test proves nothing");

        var ids = new List<long>();
        for (var i = 0; i < 40; i++)
            ids.Add(await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"diag-{i}", Props = new SimpleProps { Title = $"diag-{i}" } }));
        foreach (var id in ids)
            (await redb.LoadAsync<SimpleProps>(id)).Should().NotBeNull();

        capture.Warnings.Should().ContainSingle(m => m.Contains("PropsCacheMaxSize"),
            "the service hands its logger to the props cache it creates");
    }

    private const int ItemCount = 220;

    /// <summary>Seeds a list of linked items and returns them from a scope that has ended.</summary>
    private static async Task<List<RedbListItem>> ItemsFromAScopeThatEndsAsync(ServiceProvider sp)
    {
        long listId;
        await using (var seed = sp.CreateAsyncScope())
        {
            var redb = seed.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            await redb.InitializeTypeRegistryAsync();
            var targetId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "diag-target", Props = new SimpleProps { Title = "target" } });
            var list = await redb.ListProvider.SaveListAsync(RedbList.Create("diag-fresh-scope", "diag-fresh-scope"));
            listId = list.Id;
            for (var i = 0; i < ItemCount; i++)
                await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = $"item-{i}", IdObject = targetId });
        }

        await using var reader = sp.CreateAsyncScope();
        return await reader.ServiceProvider.GetRequiredService<IRedbService>().ListProvider.GetListItemsAsync(listId);
    }

    [Fact]
    public async Task FreshScopeLoads_OneAtATime_AreWarnedByRate()
    {
        var capture = new CapturingLoggerProvider();
        await using var sp = Build("redb_tests_diag_freshscope.db", capture, c =>
        {
            c.PreloadListItemLinkedObjects = false;
            c.LazyLoadWithoutScope = LazyLoadWithoutScopeMode.FreshScope;
        });
        var items = await ItemsFromAScopeThatEndsAsync(sp);
        items.Should().HaveCount(ItemCount);
        items.Should().OnlyContain(i => !i.IsObjectLoaded, "precondition: with the preload off every object is still lazy");

        // No redb scope is live here: with the option each read opens a scope of its own.
        foreach (var item in items)
            item.Object.Should().NotBeNull();

        capture.Warnings.Should().ContainSingle(m => m.Contains("Lazy loads in fresh scopes"),
            "a steady stream of fresh-scope loads drains the pool even when they run one at a time");
    }
}
