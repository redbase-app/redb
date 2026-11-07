using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.SQLite.Pro.Extensions;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The thread-pool-free sync path of <see cref="RedbListItem.Object"/> end to end: the getter runs the
/// whole load - scheme lookup, permission check, get_object_json, deserialization - on the calling
/// thread down to ADO.NET, on the reader's scope. A saturated thread pool can no longer slow or
/// deadlock a lazy touch. Preload is OFF here so every touch really exercises the lazy chain.
/// </summary>
public class SqliteListItemSyncGetterTests
{
    private static ServiceProvider Build(Action<RedbServiceConfiguration>? configure = null)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()
            .GetConnectionString("Sqlite")!
            .Replace("redb_tests_sqlite.db", "redb_tests_sqlite_syncgetter.db");
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(o =>
        {
            o.UseSqlite(cs);
            o.Configure(c =>
            {
                c.PreloadListItemLinkedObjects = false;
                c.CacheDomain = "listitem-syncgetter";
                configure?.Invoke(c);
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<long> SeedListAsync(IRedbService redb, string marker)
    {
        var list = await redb.ListProvider.SaveListAsync(new RedbList { Name = $"SyncGetterProbe-{marker}-{Guid.NewGuid():N}" });
        for (var i = 0; i < 2; i++)
        {
            var obj = new RedbObject<Models.SimpleProps>
            {
                name = $"syncget-{marker}-{i}",
                Props = new Models.SimpleProps { Title = $"linked-{i}" },
            };
            var objId = await redb.SaveAsync(obj);
            await redb.ListProvider.SaveListItemAsync(new RedbListItem
            { IdList = list.Id, Value = "v" + i, IdObject = objId });
        }
        return list.Id;
    }

    [Fact]
    public async Task LazyTouch_LoadsThroughTheSyncChain()
    {
        await using var sp = Build();
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<Models.SimpleProps>();
        var listId = await SeedListAsync(redb, "live");

        var items = (await redb.ListProvider.GetListItemsAsync(listId))
            .Where(i => i.IdObject.HasValue).ToList();

        items.Should().HaveCount(2);
        items.Should().OnlyContain(i => !i.IsObjectLoaded, "preload is off - the items stay lazy");

        foreach (var item in items)
        {
            var obj = item.Object;
            obj.Should().NotBeNull();
            obj!.Name.Should().StartWith("syncget-live-");
            item.IsObjectLoaded.Should().BeTrue();
        }
    }

    /// <summary>Seeds and hands the items out of a scope that ends - in this helper, so nothing stays current.</summary>
    private static async Task<List<RedbListItem>> ItemsOfADeadScopeAsync(ServiceProvider sp)
    {
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<Models.SimpleProps>();
        var listId = await SeedListAsync(redb, "dead");
        return (await redb.ListProvider.GetListItemsAsync(listId)).Where(i => i.IdObject.HasValue).ToList();
    }

    [Fact]
    public async Task LazyTouch_AfterTheScopeDied_Refuses()
    {
        await using var sp = Build();
        var survivors = await ItemsOfADeadScopeAsync(sp);
        survivors.Should().HaveCount(2);

        // The scope that handed the items out is gone and no other scope reads here: a clear refusal, never a scope
        // opened behind the reader's back (owner decision 2026-09-15).
        foreach (var item in survivors)
        {
            Action touch = () => _ = item.Object;
            touch.Should().Throw<RedbLazyLoadScopeEndedException>();
            item.IsObjectLoaded.Should().BeFalse();
        }
    }

    [Fact]
    public async Task LazyTouch_AfterTheScopeDied_LoadsInAFreshScopeSynchronously_WhenConfigured()
    {
        await using var sp = Build(c => c.LazyLoadWithoutScope = LazyLoadWithoutScopeMode.FreshScope);
        var survivors = await ItemsOfADeadScopeAsync(sp);

        foreach (var item in survivors)
        {
            var obj = item.Object;
            obj.Should().NotBeNull("the option opens a fresh scope for the load, on the calling thread");
            obj!.Name.Should().StartWith("syncget-dead-");
        }
    }
}
