using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The production shape of the tsum leak (2026-09). Scope B is alive and serving a request; scope A
/// was created after it, materialized a list item that links to an object, and died. B then touches
/// <see cref="RedbListItem.Object"/> on that item. Before the fix the item resolved its object through
/// a process-global loader that pointed at whichever <c>IRedbService</c> was constructed LAST (A,
/// already disposed); A's context re-opened a physical connection that no one would ever release.
/// <para>
/// The container under test carries its own <c>Application Name</c>, so <c>pg_stat_activity</c>
/// counts exactly its sessions. Disposing the data source closes every pooled connection; a session
/// that survives it is one that was checked out of the pool and abandoned.
/// </para>
/// </summary>
public abstract class PostgresListItemObjectLoaderLeakTestsBase
{
    protected abstract bool IsPro { get; }

    private static string BaseConnectionString() =>
        new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;

    private ServiceProvider BuildContainer(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        if (IsPro)
        {
            services.AddRedbPro(options =>
            {
                redb.Postgres.Pro.Extensions.PostgresProOptionsExtensions.UsePostgres(options, cs);
                options.Configure(c => c.EnablePropsCache = false);
            });
        }
        else
        {
            services.AddRedb(options =>
            {
                redb.Postgres.Extensions.PostgresOptionsExtensions.UsePostgres(options, cs);
                options.Configure(c => c.EnablePropsCache = false);
            });
        }
        return services.BuildServiceProvider();
    }

    private static async Task<int> CountSessionsAsync(string cs, string applicationName)
    {
        // The observer must not be counted and must not sit in a pool of its own.
        var observerCs = new NpgsqlConnectionStringBuilder(cs)
        {
            ApplicationName = "redb-leak-observer",
            Pooling = false
        }.ConnectionString;

        await using var conn = new NpgsqlConnection(observerCs);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", conn);
        cmd.Parameters.Add(new NpgsqlParameter { Value = applicationName });
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    /// <summary>Server-side teardown of a closed socket is asynchronous; give it a moment.</summary>
    private static async Task<int> WaitForSessionCountAsync(string cs, string applicationName, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        var count = await CountSessionsAsync(cs, applicationName);
        while (count != expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
            count = await CountSessionsAsync(cs, applicationName);
        }
        return count;
    }

    [Fact]
    public async Task ListItemObject_TouchedFromAnotherScopeAfterOwnerDied_LeavesNoSessionBehind()
    {
        var baseCs = BaseConnectionString();
        var applicationName = $"redb-leak-{Guid.NewGuid():N}";
        var cs = new NpgsqlConnectionStringBuilder(baseCs) { ApplicationName = applicationName }.ConnectionString;
        var listName = $"LeakProbe_{Guid.NewGuid():N}";

        var root = BuildContainer(cs);
        var dataSource = root.GetRequiredService<NpgsqlDataSource>();
        long objectId = 0, listId = 0, itemId = 0;
        try
        {
            // Scope B: the long-running request that will touch the item later.
            var scopeB = root.CreateAsyncScope();
            try
            {
                var redbB = scopeB.ServiceProvider.GetRequiredService<IRedbService>();
                await redbB.SyncSchemeAsync<SimpleProps>();

                // Scope A: created AFTER B (so any "last service wins" global state points at A),
                // materializes the item, then dies.
                RedbListItem item;
                var scopeA = root.CreateAsyncScope();
                try
                {
                    var redbA = scopeA.ServiceProvider.GetRequiredService<IRedbService>();

                    var obj = TestDataFactory.CreateSimple("leak-probe");
                    objectId = await redbA.SaveAsync(obj);

                    var list = await redbA.ListProvider.SaveListAsync(RedbList.Create(listName, listName));
                    listId = list.Id;
                    await redbA.ListProvider.AddItemsAsync(list,
                        new IRedbListItem[] { RedbListItem.ForList(list, "linked", null, objectId) });

                    item = (await redbA.ListProvider.GetListItemsAsync(list.Id)).Single();
                    itemId = item.Id;
                }
                finally
                {
                    await scopeA.DisposeAsync();
                }

                // Back in B: the audit/response path touches the linked object of the item.
                var linked = item.Object;
                linked.Should().NotBeNull("a list item must still resolve its linked object after the scope that loaded it is gone");
                linked!.Id.Should().Be(objectId);
            }
            finally
            {
                await scopeB.DisposeAsync();
            }
        }
        finally
        {
            // Cleanup through a fresh scope, then tear the whole container down.
            await using (var cleanup = root.CreateAsyncScope())
            {
                var redb = cleanup.ServiceProvider.GetRequiredService<IRedbService>();
                if (itemId != 0) await redb.ListProvider.DeleteListItemAsync(itemId);
                if (listId != 0) await redb.ListProvider.DeleteListAsync(listId);
                if (objectId != 0) await redb.DeleteAsync(objectId);
            }
            await root.DisposeAsync();
            await dataSource.DisposeAsync();
        }

        var survivors = await WaitForSessionCountAsync(baseCs, applicationName, expected: 0);
        survivors.Should().Be(0,
            "every connection the container opened must be back in the pool when the data source is disposed; " +
            "a survivor was opened on a disposed context and can never be returned");
    }
}
