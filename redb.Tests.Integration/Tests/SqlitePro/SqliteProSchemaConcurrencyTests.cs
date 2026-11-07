using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.SqlitePro;

/// <summary>
/// Services opening one SQLite file at once, as a panel and a worker of one host do: the schema upgrades of an old file
/// and the creation of a new one run under the schema lock (an immediate transaction of the service's own connection),
/// the version or the tables asked again under it. Without it a step that checks and then acts (an index rebuilt under
/// its name, a column added) failed in the second service. Each service has a connection and a cache domain of its own.
/// </summary>
public class SqliteProSchemaConcurrencyTests : IDisposable
{
    private readonly List<string> _files = [];

    private string NewFile()
    {
        var path = Path.Combine(Path.GetTempPath(), $"redb_schemalock_{Guid.NewGuid():N}.db");
        _files.Add(path);
        return path;
    }

    private static ServiceProvider Build(string path, ILoggerProvider? logs = null, Action<IServiceCollection>? more = null)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            b.SetMinimumLevel(LogLevel.Warning);
            if (logs != null)
                b.AddProvider(logs);
        });
        services.AddRedbPro(options => options.UseSqlite($"Data Source={path}")
            .Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"schema-lock-{Guid.NewGuid():N}";
            }));
        more?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static async Task<List<Exception>> StartTogetherAsync(string path, int nodes, bool ensureCreated)
    {
        var providers = Enumerable.Range(0, nodes).Select(_ => Build(path)).ToList();
        try
        {
            var starts = providers.Select(sp => Task.Run(async () =>
            {
                try
                {
                    await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated);
                    return null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            }));
            return (await Task.WhenAll(starts)).Where(e => e != null).Select(e => e!).ToList();
        }
        finally
        {
            foreach (var sp in providers)
                await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task OldFile_ServicesOpeningItTogether_AllStart_AndTheFileIsUpgraded()
    {
        var path = NewFile();
        await using (var sp = Build(path))
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        for (var round = 0; round < 10; round++)
        {
            // A file of schema version 10: the indexes of steps 11 and 12 missing, the stamp behind.
            await using (var sp = Build(path))
            {
                var ctx = sp.GetRequiredService<IRedbContext>();
                await ctx.ExecuteAsync("DROP INDEX IF EXISTS \"IX__values__structure_object_string\"");
                await ctx.ExecuteAsync("DROP INDEX IF EXISTS \"IX__values__structure_long_object\"");
                await ctx.ExecuteAsync("PRAGMA user_version = 10");
            }

            var failures = await StartTogetherAsync(path, nodes: 6, ensureCreated: true);
            failures.Should().BeEmpty($"every service starts (round {round})");

            await using (var sp = Build(path))
            {
                var ctx = sp.GetRequiredService<IRedbContext>();
                (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().BeGreaterThanOrEqualTo(12);
                (await ctx.ExecuteScalarAsync<long?>(
                        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'IX__values__structure_object_string'"))
                    .Should().Be(1, "step 12 built the index once");
            }
        }
    }

    [Fact]
    public async Task NewFile_ServicesCreatingTheSchemaTogether_AllStart()
    {
        for (var round = 0; round < 5; round++)
        {
            var failures = await StartTogetherAsync(NewFile(), nodes: 4, ensureCreated: true);
            failures.Should().BeEmpty($"every service starts (round {round})");
        }
    }

    [Fact]
    public async Task Scheme_ServicesSyncingItTogether_AllSucceed()
    {
        var path = NewFile();
        var providers = Enumerable.Range(0, 4).Select(_ => Build(path)).ToList();
        try
        {
            var nodes = new List<IRedbService>();
            foreach (var sp in providers)
            {
                var redb = sp.GetRequiredService<IRedbService>();
                await redb.InitializeAsync(ensureCreated: true);
                nodes.Add(redb);
            }

            var failures = await redb.Tests.Integration.Tests.Base.SchemeSyncRace.RunAsync(
                nodes, providers[0].GetRequiredService<IRedbContext>(), rounds: 8);
            failures.Should().BeEmpty();
        }
        finally
        {
            foreach (var sp in providers)
                await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task RootDuplicates_EmptyOnesRemoved_OneWithValuesWaitsForTheHandFix()
    {
        // As on PostgreSQL, but the file tries again at every start, not at a module upgrade.
        var path = NewFile();
        await using var owner = Build(path);
        var redbService = owner.GetRequiredService<IRedbService>();
        await redbService.InitializeAsync(ensureCreated: true);
        var context = owner.GetRequiredService<IRedbContext>();
        var scenario = await redb.Tests.Integration.Tests.Base.RootDuplicatesScenario.PlantAsync(redbService, context);

        for (var start = 0; start < 2; start++)
        {
            // Every start: the error again, the same list.
            var logs = new redb.Tests.Integration.Tests.Base.CollectingLoggerProvider();
            await using var sp = Build(path, logs);
            var started = sp.GetRequiredService<IRedbService>();
            await started.InitializeAsync(ensureCreated: true);
            scenario.ShouldReportCountOnly(started, logs);
        }

        async Task<bool> Exists(long id) => await redb.Tests.Integration.Tests.Base.RootDuplicatesScenario.StructureExistsAsync(context, id);
        async Task<bool> KeyExists() => await context.ExecuteScalarAsync<long?>(
            $"SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = '{RootStructureKey.IndexName}'") != null;

        (await Exists(scenario.TitleDuplicate)).Should().BeFalse();
        (await Exists(scenario.PartDuplicate)).Should().BeFalse();
        (await Exists(scenario.PartDuplicateChild)).Should().BeFalse();
        (await Exists(scenario.CountDuplicate)).Should().BeTrue("it holds a value");
        (await KeyExists()).Should().BeFalse();

        // A copy written after the start (a node of the previous version) is met by the next sync of the scheme.
        var syncLogs = new redb.Tests.Integration.Tests.Base.CollectingLoggerProvider();
        await using (var sp = Build(path, syncLogs))
        {
            var running = sp.GetRequiredService<IRedbService>();
            await running.InitializeAsync(ensureCreated: true);
            var titleCopy = await scenario.CopyRootAsync(context, "Title");
            await running.SyncSchemeAsync<redb.Tests.Integration.Models.SchemaLockSyncProbeProps>();
            running.SchemaProblems.Should().Contain(p => p.Field == "Title" && p.StructureIds.Contains(titleCopy));
            syncLogs.Errors.Should().ContainSingle(e => e.Contains("'Title'"));
            (await Exists(titleCopy)).Should().BeTrue("a sync never removes a copy of a field");
            await context.ExecuteAsync($"DELETE FROM _structures WHERE _id = {titleCopy}");
        }

        await scenario.ResolveByHandAsync(context);
        await using (var sp = Build(path))
        {
            var started = sp.GetRequiredService<IRedbService>();
            await started.InitializeAsync(ensureCreated: true);
            started.SchemaProblems.Should().BeEmpty();
        }
        (await KeyExists()).Should().BeTrue();
    }

    /// <summary>The health check of the README, registered with AddCheck: IRedbService injected from a scope of its own.</summary>
    private sealed class SchemaHealthCheck(IRedbService redb) : Microsoft.Extensions.Diagnostics.HealthChecks.IHealthCheck
    {
        public Task<Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult> CheckHealthAsync(
            Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var problems = redb.SchemaProblems;
            return Task.FromResult(problems.Count > 0
                ? Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Degraded(string.Join("; ", problems.Select(p => p.Message)))
                : Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy());
        }
    }

    [Fact]
    public async Task SchemaProblems_FoundByTheStartInItsScope_SeenFromAnotherScope()
    {
        // IRedbService is scoped and the host starts it in a scope of its own (RedbInitHostedService): what the start
        // found must reach the instance a health check gets in a later scope, without a database read or a second log.
        var path = NewFile();
        await using (var owner = Build(path))
        {
            var redbService = owner.GetRequiredService<IRedbService>();
            await redbService.InitializeAsync(ensureCreated: true);
            await redb.Tests.Integration.Tests.Base.RootDuplicatesScenario.PlantAsync(redbService, owner.GetRequiredService<IRedbContext>());
        }

        var logs = new redb.Tests.Integration.Tests.Base.CollectingLoggerProvider();
        await using var host = Build(path, logs, services => services.AddHealthChecks().AddCheck<SchemaHealthCheck>("redb-schema"));
        await using (var startScope = host.CreateAsyncScope())
            await startScope.ServiceProvider.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
        logs.Errors.Should().ContainSingle(e => e.Contains("'Count'"));

        // The health service of the host, as /health runs it: a scope per check run.
        var report = await host.GetRequiredService<Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckService>().CheckHealthAsync();
        report.Status.Should().Be(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Degraded);
        report.Entries["redb-schema"].Description.Should().Contain("'Count'");

        await using (var healthScope = host.CreateAsyncScope())
        {
            var redbService = healthScope.ServiceProvider.GetRequiredService<IRedbService>();
            redbService.SchemaProblems.Should().ContainSingle(p => p.Field == "Count");

            // Asked again from the new instance: the same list, no second error.
            (await redbService.CheckSchemaAsync()).Should().ContainSingle(p => p.Field == "Count");
            logs.Errors.Should().ContainSingle(e => e.Contains("'Count'"));
        }
    }

    [Fact]
    public async Task OldFile_LockHeldByAnotherProcess_StartUpStopsAtTheTimeOut()
    {
        var path = NewFile();
        await using (var sp = Build(path))
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            await sp.GetRequiredService<IRedbContext>().ExecuteAsync("PRAGMA user_version = 10");
        }

        // Another process holds the write lock of the file and does not let go.
        await using var holder = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={path}");
        await holder.OpenAsync();
        await using var held = holder.BeginTransaction(deferred: false);

        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        // A short command time-out: Microsoft.Data.Sqlite retries a busy BEGIN until it, so each attempt lasts that long.
        services.AddRedbPro(options => options.UseSqlite($"Data Source={path};Default Timeout=1")
            .Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"schema-lock-{Guid.NewGuid():N}";
                c.SchemaLockTimeout = TimeSpan.FromSeconds(3);
            }));
        await using var sp2 = services.BuildServiceProvider();

        var start = () => sp2.GetRequiredService<IRedbService>().InitializeAsync();
        (await start.Should().ThrowAsync<redb.Core.Exceptions.RedbSchemaLockTimeoutException>())
            .Which.Waited.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(3));
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var file in _files)
            foreach (var suffix in new[] { "", "-wal", "-shm" })
                try { File.Delete(file + suffix); } catch (IOException) { }
    }
}
