using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using Xunit;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Ownership of the SQLite connection pool (the SQLite counterpart of the NpgsqlDataSource
/// ownership fix): Microsoft.Data.Sqlite pools are static, keyed by connection string, and a
/// pooled handle keeps the database FILE locked. The container that registered the
/// <see cref="SqliteDataSource"/> owns that pool: when the container dies (test host teardown,
/// a hot-reloaded module), the file must be deletable again.
/// </summary>
public class SqliteDataSourceOwnershipTests
{
    [Fact]
    public async Task DisposingContainer_ReleasesTheDatabaseFile()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"redb_ownership_{Guid.NewGuid():N}.db");
        var cs = $"Data Source={dbPath}";
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();

        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(o => o
            .UseSqlite(cs)
            .Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.DeleteInsert;
                c.EnablePropsCache = false;
            }));

        var sp = services.BuildServiceProvider();
        try
        {
            var redb = sp.GetRequiredService<IRedbService>();
            try { await redb.InitializeAsync(ensureCreated: true); }
            catch { await redb.InitializeAsync(); }
        }
        finally
        {
            await sp.DisposeAsync();
        }

        // The container is gone; nothing of this application is using the database any more.
        // The pooled connections belonged to that container's data source - a file still locked
        // here is an orphaned pool (the exact failure the test-suite worked around for years
        // with process exits and manual ClearAllPools).
        var act = () =>
        {
            File.Delete(dbPath);
            var wal = dbPath + "-wal";
            var shm = dbPath + "-shm";
            if (File.Exists(wal)) File.Delete(wal);
            if (File.Exists(shm)) File.Delete(shm);
        };
        act.Should().NotThrow("disposing the container must release every pooled connection of its data source");
    }
}
