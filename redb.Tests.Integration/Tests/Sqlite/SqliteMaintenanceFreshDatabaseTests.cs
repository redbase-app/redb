using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// A SQLite database nobody has analysed yet - the state every quick-start image starts in.
/// <c>sqlite_stat1</c> does not exist until the first ANALYZE, and the maintenance facade must
/// report what the engine knows (nothing) instead of failing: a dashboard page that reads storage
/// health would answer 500 while the very button that would fix it sits on that page
/// (redb.Tsak BR-13, 2026-09-21).
/// </summary>
public class SqliteMaintenanceFreshDatabaseTests
{
    [Fact]
    public async Task OnADatabaseNeverAnalysed_TableStatsSayThereAreNoStatistics()
    {
        await using var provider = Build();
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        // No ANALYZE has run on this file, so sqlite_stat1 is absent.
        var tables = await redb.Maintenance.GetTableStatsAsync();

        tables.Should().NotBeEmpty("the redb schema creates its tables regardless of statistics");
        var objects = tables.First(t => t.Table == "_objects");
        objects.EstimatedRows.Should().BeNull("the engine has no estimate to give yet");
        objects.HasStatistics.Should().BeFalse("that is exactly what this flag is for");
    }

    [Fact]
    public async Task OnADatabaseNeverAnalysed_IndexStatsStillRead()
    {
        await using var provider = Build();
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        var stats = await redb.Maintenance.GetIndexStatsAsync();

        stats.Should().NotBeEmpty();
        stats.Should().OnlyContain(s => s.Columns.Count > 0);
    }

    [Fact]
    public async Task AfterAnalyze_TheSameCallsCarryEstimates()
    {
        await using var provider = Build();
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        await redb.Maintenance.AnalyzeAsync();
        var tables = await redb.Maintenance.GetTableStatsAsync();

        tables.Should().Contain(t => t.Table == "_objects" && t.HasStatistics == true,
            "ANALYZE creates sqlite_stat1, and the same query must then say so");
    }

    private static ServiceProvider Build()
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_fresh_{Guid.NewGuid():N}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"fresh-{Guid.NewGuid():N}");
        });
        return services.BuildServiceProvider();
    }
}
