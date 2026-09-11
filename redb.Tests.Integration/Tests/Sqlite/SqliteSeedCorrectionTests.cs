using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Utils;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Upgrading a package must fix seeded metadata in a database that already exists.
///
/// <para>
/// PostgreSQL and MSSQL carry such corrections in <c>sql/v2-pvt/*.sql</c>, which
/// <c>EnsurePvtModuleDeployedAsync</c> reapplies whenever <c>pvt_module_version()</c> disagrees with
/// the dialect. SQLite has no module and no version function, and <c>redbSqlite.sql</c> is applied only
/// when the tables are absent — so it needs its own step, and that step needs its own test.
/// </para>
///
/// <para>
/// This suite deliberately does NOT use <see cref="SqliteFixture"/>: that fixture deletes the database
/// file on every run, so it can only ever model a NEW database. The one case that matters here is an
/// OLD one, which is why each test owns a temporary file and opens it twice.
/// </para>
/// </summary>
public class SqliteSeedCorrectionTests : IDisposable
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"redb_seedfix_{Guid.NewGuid():N}.db");

    private string ConnectionString => $"Data Source={_dbPath}";

    private static ServiceProvider BuildServices(string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();

        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite(connectionString));
        return services.BuildServiceProvider();
    }

    private async Task<string?> DateOnlyDbTypeAsync(IRedbContext ctx) =>
        await ctx.ExecuteScalarAsync<string>(
            $"SELECT _db_type FROM _types WHERE _id = {RedbTypeIds.DateOnly}");

    /// <summary>
    /// The correction has to run when the database was created by an older build, and it has to be
    /// driven by opening the database — not by anything the caller remembers to do.
    /// </summary>
    [Fact]
    public async Task ExistingDatabase_WithStaleDateOnlySeed_IsCorrectedOnNextOpen()
    {
        // First open: creates the schema, as an older build would have.
        await using (var sp = BuildServices(ConnectionString))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            var ctx = sp.GetRequiredService<IRedbContext>();

            // Roll the seed back to the value the older build wrote. 'DateTime' is a _db_type no JSON
            // projection branches on, which is why every DateOnly property used to materialise as
            // 0001-01-01.
            await ctx.ExecuteAsync(
                $"UPDATE _types SET _db_type = 'DateTime' WHERE _id = {RedbTypeIds.DateOnly}");

            (await DateOnlyDbTypeAsync(ctx)).Should().Be("DateTime", "the stale state must be staged");
        }

        // Second open: the same file, a new service — the upgrade path.
        await using (var sp = BuildServices(ConnectionString))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            var ctx = sp.GetRequiredService<IRedbContext>();
            (await DateOnlyDbTypeAsync(ctx)).Should().Be("DateTimeOffset",
                "opening an existing database must correct the seed; without this SQLite has no channel " +
                "for such a fix at all and it would reach new databases only");
        }
    }

    /// <summary>
    /// The correction runs on every start, so it must be a no-op the second time — and must not disturb
    /// a database that was already right.
    /// </summary>
    [Fact]
    public async Task CorrectSeed_IsLeftAloneAcrossRepeatedOpens()
    {
        for (var i = 0; i < 3; i++)
        {
            await using var sp = BuildServices(ConnectionString);
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            var ctx = sp.GetRequiredService<IRedbContext>();
            (await DateOnlyDbTypeAsync(ctx)).Should().Be("DateTimeOffset", $"open #{i + 1}");
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
