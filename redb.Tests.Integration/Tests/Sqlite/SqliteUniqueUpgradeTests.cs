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
/// The SQLite delivery of the V4 unique-key DDL: a database file created before the columns existed
/// gets them on the next open — <c>ApplySchemaUpgradesAsync</c> step 2, gated by
/// <c>PRAGMA user_version</c>. Modeled the only honest way: create a current file, strip the columns
/// and the index the way a pre-V4 build never had them, stamp the old schema version, reopen.
/// </summary>
public class SqliteUniqueUpgradeTests : IDisposable
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"redb_uniqueup_{Guid.NewGuid():N}.db");

    private string ConnectionString => $"Data Source={_dbPath}";

    protected virtual ServiceProvider BuildServices(string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite(connectionString));
        return services.BuildServiceProvider();
    }

    private static async Task<bool> ColumnExistsAsync(IRedbContext ctx, string table, string column)
        => (await ctx.ExecuteScalarAsync<long?>(
            $"SELECT 1 FROM pragma_table_info('{table}') WHERE name = '{column}' LIMIT 1")).HasValue;

    [Fact]
    public async Task PreV4File_GetsUniqueColumns_OnNextOpen()
    {
        await using (var sp = BuildServices(ConnectionString))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            await StagePreV4Async(sp.GetRequiredService<IRedbContext>());
        }

        await using (var sp = BuildServices(ConnectionString))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            var ctx = sp.GetRequiredService<IRedbContext>();
            (await ColumnExistsAsync(ctx, "_values", "_unique")).Should().BeTrue();
            (await ColumnExistsAsync(ctx, "_structures", "_unique")).Should().BeTrue();
            (await ColumnExistsAsync(ctx, "_structures", "_unique_version")).Should().BeTrue();
            (await ColumnExistsAsync(ctx, "_scheme_metadata_cache", "_unique")).Should().BeTrue();

            (await ctx.ExecuteScalarAsync<long?>(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'UIX__values__structure_unique'"))
                .Should().NotBeNull("the partial unique index rides the same step");
            await AssertV4DdlAsync(ctx);

            (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().BeGreaterThanOrEqualTo(2,
                "the pass stamps the file so the next open skips it");
        }
    }

    /// <summary>
    /// Strips every V4 column and index the way a pre-V4 build never had them and stamps the old
    /// schema version: _values._unique, _structures._unique/_unique_version/_lazy,
    /// _scheme_metadata_cache._unique/_unique_version/_lazy, _objects._value_unique, two indexes.
    /// </summary>
    private static async Task StagePreV4Async(IRedbContext ctx)
    {
        await ctx.ExecuteAsync("DROP INDEX IF EXISTS \"UIX__values__structure_unique\"");
        await ctx.ExecuteAsync("DROP INDEX IF EXISTS \"UIX__objects__scheme_unique\"");
        foreach (var (table, column) in new[]
                 {
                     ("_values", "_unique"),
                     ("_structures", "_unique"), ("_structures", "_unique_version"), ("_structures", "_lazy"),
                     ("_scheme_metadata_cache", "_unique"), ("_scheme_metadata_cache", "_unique_version"),
                     ("_scheme_metadata_cache", "_lazy"),
                     ("_objects", "_value_unique"),
                 })
        {
            await ctx.ExecuteAsync($"ALTER TABLE {table} DROP COLUMN {column}");
        }
        await ctx.ExecuteAsync("PRAGMA user_version = 1");

        (await ColumnExistsAsync(ctx, "_values", "_unique")).Should().BeFalse("the pre-V4 state must be staged");
        (await ColumnExistsAsync(ctx, "_structures", "_lazy")).Should().BeFalse();
    }

    private static async Task AssertV4DdlAsync(IRedbContext ctx)
    {
        (await ColumnExistsAsync(ctx, "_structures", "_lazy")).Should().BeTrue("Л2 marker column");
        (await ColumnExistsAsync(ctx, "_scheme_metadata_cache", "_lazy")).Should().BeTrue();
        (await ColumnExistsAsync(ctx, "_scheme_metadata_cache", "_unique_version")).Should().BeTrue();
        (await ColumnExistsAsync(ctx, "_objects", "_value_unique")).Should().BeTrue("Э1 key column");
        (await ctx.ExecuteScalarAsync<long?>(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'UIX__objects__scheme_unique'"))
            .Should().NotBeNull();
        (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().BeGreaterThan(1, "the pass stamps the schema version");
    }

    [Fact]
    public async Task PreV4File_IsUpgraded_ByInitializeAsync_WithoutEnsureCreated()
    {
        await using (var sp = BuildServices(ConnectionString))
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            await StagePreV4Async(sp.GetRequiredService<IRedbContext>());
        }

        // The plain start-up, the one PostgreSQL and MSSQL reach through the module block
        // "0. Schema upgrades": no EnsureDatabaseAsync, the upgrade pass must still run.
        await using (var sp = BuildServices(ConnectionString))
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync();
            await AssertV4DdlAsync(sp.GetRequiredService<IRedbContext>());
        }
    }
    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
        {
            try { File.Delete(_dbPath + suffix); } catch { /* best effort */ }
        }
    }
}
