using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Data;
using redb.Core.Pro.Extensions;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.PostgresPro;

/// <summary>
/// A new database whose tables hold rows but no planner statistics: never analyzed, or analyzed while empty. There
/// PostgreSQL took every stream of the streams form for one row and joined them by nested loops that rescan an index
/// range per object: a grouping and a window over a range of Props values, and a window over a narrow OR, ran for
/// minutes on 600 objects (a test of the equivalence suite ran ten minutes). Each test owns its database, so the
/// state of the statistics is its own, and autovacuum is off on both tables so nothing analyzes them meanwhile.
/// </summary>
public class PostgresProFreshStatisticsTests : IAsyncLifetime
{
    private static readonly string AdminBase =
        new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;

    // xUnit makes an instance per test: each owns its database and drops it when the test ends, passed or failed.
    private readonly string _suffix = Guid.NewGuid().ToString("N")[..8];
    private string? _database;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_database == null) return;
        NpgsqlConnection.ClearAllPools();
        await AdminAsync("postgres", $"DROP DATABASE IF EXISTS \"{_database}\" WITH (FORCE)");
    }

    /// <summary>
    /// The range grouping, the range window and the OR window answer within 30 seconds each while the tables have no
    /// statistics. Before the guard each of them ran past it.
    /// </summary>
    [Fact]
    public async Task NoStatistics_GroupingAndWindows_AnswerInBoundedTime()
    {
        var (redb, database, provider) = await CreateSeededAsync("q");
        (await ReltuplesAsync(database)).Should().BeLessThanOrEqualTo(0, "the test is about tables without statistics");

        IRedbQueryable<EmployeeProps> Range() => redb.Query<EmployeeProps>().Where(e => e.Salary > 70000m && e.Age >= 30);
        IRedbQueryable<EmployeeProps> Or() => redb.Query<EmployeeProps>().Where(e => e.EmployeeCode == "EMP-0010" || e.Age == 33);
        var limit = TimeSpan.FromSeconds(30);

        var groups = await Range().GroupBy(x => x.Department)
            .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, x => x.Salary) })
            .WaitAsync(limit);
        groups.Should().NotBeEmpty();

        var ranked = await Range().Take(20).WithWindow(w => w.PartitionBy(x => x.Department).OrderByDesc(x => x.Salary))
            .SelectAsync(x => new { x.Props.Salary, Rank = Win.RowNumber() })
            .WaitAsync(limit);
        ranked.Should().NotBeEmpty();

        var orRanked = await Or().Take(20).WithWindow(w => w.PartitionBy(x => x.Department).OrderByDesc(x => x.Salary))
            .SelectAsync(x => new { x.Props.Salary, Rank = Win.RowNumber() })
            .WaitAsync(limit);
        orRanked.Should().NotBeEmpty();

        (await provider.GetRequiredService<IRedbContext>().ExecuteScalarAsync<string>("SELECT current_setting('enable_nestloop')"))
            .Should().Be("on", "the guard puts the setting back after each statement");
    }

    /// <summary>
    /// Start-up analyzes _objects and _values when they hold rows without statistics, as after a pg_restore or a load
    /// by COPY: a second service starting on the seeded database finds reltuples above zero afterwards.
    /// </summary>
    [Fact]
    public async Task Start_AnalyzesTablesThatHoldRowsWithoutStatistics()
    {
        var (_, database, _) = await CreateSeededAsync("s");
        (await ReltuplesAsync(database)).Should().BeLessThanOrEqualTo(0);

        await using var provider = BuildServices(database);
        await provider.GetRequiredService<IRedbService>().InitializeAsync();

        (await ReltuplesAsync(database)).Should().BeGreaterThan(0, "start-up analyzed the tables");
    }

    private async Task<(IRedbService Redb, string Database, ServiceProvider Provider)> CreateSeededAsync(string tag)
    {
        var database = $"redb_freshstats_{tag}_{_suffix}";
        _database = database;
        await AdminAsync("postgres", $"DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)");
        await AdminAsync("postgres", $"CREATE DATABASE \"{database}\"");

        var provider = BuildServices(database);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await AdminAsync(database, "ALTER TABLE _objects SET (autovacuum_enabled = false); ALTER TABLE _values SET (autovacuum_enabled = false)");
        await redb.SyncSchemeAsync<EmployeeProps>();
        await TestDataFactory.SeedEmployees(redb, 600);
        return (redb, database, provider);
    }

    private static ServiceProvider BuildServices(string database)
    {
        var cs = new NpgsqlConnectionStringBuilder(AdminBase) { Database = database }.ConnectionString;
        var license = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()["Redb:License"];
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedbPro(options =>
        {
            options.UsePostgres(cs).Configure(c => c.EnablePropsCache = false);
            if (!string.IsNullOrWhiteSpace(license))
                options.WithLicense(license);
        });
        return services.BuildServiceProvider();
    }

    private static async Task<float> ReltuplesAsync(string database)
        => await AdminScalarAsync<float>(database, "SELECT max(reltuples) FROM pg_class WHERE oid IN (to_regclass('_objects'), to_regclass('_values'))");

    private static async Task AdminAsync(string database, string sql)
    {
        var cs = new NpgsqlConnectionStringBuilder(AdminBase) { Database = database, Pooling = false }.ConnectionString;
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<T> AdminScalarAsync<T>(string database, string sql)
    {
        var cs = new NpgsqlConnectionStringBuilder(AdminBase) { Database = database, Pooling = false }.ConnectionString;
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (T)(await cmd.ExecuteScalarAsync())!;
    }
}
