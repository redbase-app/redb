using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Schema step 13 of <c>ApplySchemaUpgradesAsync</c> in 4.7.5: a file of schema version 12 says String for TimeOnly and TimeSpan in
/// _types and in the metadata cache; reopened, both say Long, so the file writes their Ticks. Staged the way the other
/// upgrade tests stage it: create a current file, put the old rows back, stamp version 14, reopen.
/// </summary>
public class SqliteTimeTicksUpgradeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"redb_timeticks_{Guid.NewGuid():N}.db");

    private const string Ids = "(-9223372036854775685, -9223372036854775684)";

    private ServiceProvider BuildServices()
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite($"Data Source={_dbPath}"));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Version14File_SaysLongForTimeOnlyAndTimeSpan()
    {
        await using (var sp = BuildServices())
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            await sp.GetRequiredService<IRedbService>().SyncSchemeAsync<redb.Tests.Integration.Models.TimeTicksProps>();
            var ctx = sp.GetRequiredService<IRedbContext>();
            (await ctx.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _types WHERE _id IN {Ids} AND _db_type = 'Long'"))
                .Should().Be(2, "a new file is created with Long for both");

            await ctx.ExecuteAsync($"UPDATE _types SET _db_type = 'String' WHERE _id IN {Ids}");
            await ctx.ExecuteAsync($"UPDATE _scheme_metadata_cache SET db_type = 'String' WHERE _type_id IN {Ids}");
            await ctx.ExecuteAsync("PRAGMA user_version = 12");
        }

        await using (var sp = BuildServices())
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync();
            var ctx = sp.GetRequiredService<IRedbContext>();
            (await ctx.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _types WHERE _id IN {Ids} AND _db_type = 'Long'"))
                .Should().Be(2, "step 13 sets Long for TimeOnly and TimeSpan");
            (await ctx.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _scheme_metadata_cache WHERE _type_id IN {Ids} AND db_type <> 'Long'"))
                .Should().Be(0, "and the copy in the metadata cache");
            (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().Be(13);
        }
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var f in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }
}
