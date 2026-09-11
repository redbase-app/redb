using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// SQLite <b>Pro</b> fixture — pure C# (ProSqlBuilder + C# materialization, no DB-side functions),
/// so no native extension is needed. Uses a separate fresh DB file from the Free fixture to avoid
/// cross-collection contention on the single-writer SQLite file.
/// </summary>
public sealed class SqliteProFixture : IAsyncLifetime
{
    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        // Own DB file: the Free and Pro collections run in PARALLEL (the old comment assumed
        // sequential and was wrong), and SQLite is a single-writer engine - two collections
        // hammering one file starve each other on the write lock for minutes (gate hang,
        // 2026-09-09: NextResult lost the non-FIFO lock race to the other collection's stream
        // of short transactions over and over).
        var cs = System.Text.RegularExpressions.Regex.Replace(
            config.GetConnectionString("Sqlite")!,
            @"Data Sources*=s*[^;]+", "Data Source=redb_tests_sqlite_pro.db");
        var license = config["Redb:License"];

        SqliteTestSupport.DeleteDbFiles(cs);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedbPro(options =>
        {
            options.UseSqlite(cs)
                .Configure(c =>
                {
                    c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;
                    c.SkipHashValidationOnCacheCheck = false;
                    c.EnablePropsCache = false;
                    c.EnablePvtPrefilter = ProTestOptions.PvtPrefilter;
                });
            if (!string.IsNullOrWhiteSpace(license))
                options.WithLicense(license);
        });

        ServiceProvider = services.BuildServiceProvider();
        Redb = ServiceProvider.GetRequiredService<IRedbService>();

        try { await Redb.InitializeAsync(ensureCreated: true); }
        catch { await Redb.InitializeAsync(); }

        await SyncSchemes();
        await Cleanup();
    }

    private async Task SyncSchemes()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        await Redb.SyncSchemeAsync<EmployeeProps>();
        await Redb.SyncSchemeAsync<ProjectMetricsProps>();
        await Redb.SyncSchemeAsync<TreeNodeProps>();
        await Redb.SyncSchemeAsync<DepartmentProps>();
        await Redb.SyncSchemeAsync<OrgRootProps>();
        await Redb.SyncSchemeAsync<DivisionProps>();
        await Redb.SyncSchemeAsync<TeamProps>();
        await Redb.SyncSchemeAsync<PersonProps>();
        await Redb.SyncSchemeAsync<CityProps>();
    }

    private async Task Cleanup()
    {
        var ctx = ServiceProvider.GetRequiredService<IRedbContext>();
        try { await ctx.ExecuteAsync("DELETE FROM _tree"); } catch { }
        await ctx.ExecuteAsync("DELETE FROM _values");
        await ctx.ExecuteAsync("DELETE FROM _objects");
    }

    public async Task DisposeAsync()
    {
        if (ServiceProvider is IAsyncDisposable ad)
            await ad.DisposeAsync();
        else
            ServiceProvider?.Dispose();
    }
}
