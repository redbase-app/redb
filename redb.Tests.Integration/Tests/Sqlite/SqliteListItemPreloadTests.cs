using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.SQLite.Pro.Extensions;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Hand-out preload of list-item linked objects (tsum freeze, 2026-09-09): a production loop
/// touching lazy Object on fresh items caused ~150 detached loads per second and, through the
/// old getter, a process-wide freeze. With PreloadListItemLinkedObjects (default ON) the list
/// provider resolves the linked objects in ONE batch at hand-out, so touching Object is a
/// field read. The OFF branch pins the previous lazy behaviour.
/// </summary>
public class SqliteListItemPreloadTests
{
    private static ServiceProvider Build(bool preload)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()
            .GetConnectionString("Sqlite")!
            .Replace("redb_tests_sqlite.db", "redb_tests_sqlite_preload.db");
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(o =>
        {
            o.UseSqlite(cs);
            o.Configure(c =>
            {
                c.PreloadListItemLinkedObjects = preload;
                c.CacheDomain = "listitem-preload-" + preload;
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<long> SeedListAsync(IRedbService redb, string marker)
    {
        var list = await redb.ListProvider.SaveListAsync(new RedbList { Name = $"PreloadProbe-{marker}-{Guid.NewGuid():N}" });
        for (var i = 0; i < 3; i++)
        {
            var obj = new RedbObject<Models.SimpleProps>
            {
                name = $"preload-{marker}-{i}",
                Props = new Models.SimpleProps { Title = $"linked-{i}" },
            };
            var objId = await redb.SaveAsync(obj);
            await redb.ListProvider.SaveListItemAsync(new RedbListItem
            { IdList = list.Id, Value = "v" + i, IdObject = objId });
        }
        return list.Id;
    }

    [Fact]
    public async Task HandOut_PreloadsLinkedObjects_InOneBatch()
    {
        await using var sp = Build(preload: true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<Models.SimpleProps>();
        var listId = await SeedListAsync(redb, "on");

        var items = (await redb.ListProvider.GetListItemsAsync(listId))
            .Where(i => i.IdObject.HasValue).ToList();

        items.Should().HaveCount(3);
        items.Should().OnlyContain(i => i.IsObjectLoaded,
            "the hand-out resolves linked objects up front - touching Object must be a field read");
        items.Select(i => i.Object!.Name).Should().OnlyContain(n => n!.StartsWith("preload-on-"));
    }

    [Fact]
    public async Task HandOut_WithPreloadDisabled_StaysLazy()
    {
        await using var sp = Build(preload: false);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<Models.SimpleProps>();
        var listId = await SeedListAsync(redb, "off");

        var items = (await redb.ListProvider.GetListItemsAsync(listId))
            .Where(i => i.IdObject.HasValue).ToList();

        items.Should().HaveCount(3);
        items.Should().OnlyContain(i => !i.IsObjectLoaded,
            "with the option off the items keep the previous lazy contract");
        items[0].Object.Should().NotBeNull("the lazy path still works when actually touched");
    }

    [Fact]
    public async Task GetObjectAsync_ResolvesWithoutBlocking()
    {
        await using var sp = Build(preload: false);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<Models.SimpleProps>();
        var listId = await SeedListAsync(redb, "async");

        var item = (await redb.ListProvider.GetListItemsAsync(listId)).First(i => i.IdObject.HasValue);
        item.IsObjectLoaded.Should().BeFalse();

        var obj = await item.GetObjectAsync();
        obj.Should().NotBeNull();
        item.IsObjectLoaded.Should().BeTrue("the async path publishes exactly like the getter");
    }
}
