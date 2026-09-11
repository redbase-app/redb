using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.SQLite.Pro.Extensions;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The thread-pool-free sync path of <see cref="RedbListItem.Object"/> end to end: the list
/// provider attaches a synchronous loader alongside the async one, the getter prefers it, and
/// the whole load - scheme lookup, permission check, get_object_json, deserialization - runs on
/// the calling thread down to ADO.NET. A saturated thread pool can no longer slow or deadlock a
/// lazy touch (the old blocking-over-async fallback parked a thread waiting for pool-scheduled
/// continuations). Preload is OFF here so every touch really exercises the lazy chain.
/// </summary>
public class SqliteListItemSyncGetterTests
{
    private static ServiceProvider Build()
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

        // The getter prefers the attached sync loader (pinned by the unit tests), so this touch
        // runs the full load - scheme lookup, get_object_json, deserialize - on this thread.
        foreach (var item in items)
        {
            var obj = item.Object;
            obj.Should().NotBeNull();
            obj!.Name.Should().StartWith("syncget-live-");
            item.IsObjectLoaded.Should().BeTrue();
        }
    }

    [Fact]
    public async Task LazyTouch_AfterTheScopeDied_BorrowsAFreshScopeSynchronously()
    {
        await using var sp = Build();

        List<RedbListItem> survivors;
        using (var scope = sp.CreateScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<Models.SimpleProps>();
            var listId = await SeedListAsync(redb, "dead");
            survivors = (await redb.ListProvider.GetListItemsAsync(listId))
                .Where(i => i.IdObject.HasValue).ToList();
        }

        // The scope that attached the loaders is gone; the sync path must borrow a fresh scope
        // from the root container on the calling thread, never touch the dead context.
        foreach (var item in survivors)
        {
            var obj = item.Object;
            obj.Should().NotBeNull("a surviving item loads through a fresh scope, synchronously");
            obj!.Name.Should().StartWith("syncget-dead-");
        }
    }
}
