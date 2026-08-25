using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// SQLite with Unicode case folding enabled.
///
/// <para>
/// Unlike PostgreSQL, no special database is needed: SQLite's ASCII-only folding is unconditional,
/// so an ordinary file reproduces the defect. What the fixture does need is its own file, because
/// the feature is installed per connection and the other SQLite fixtures share one database.
/// </para>
/// </summary>
public sealed class SqliteCollationFixture : IAsyncLifetime
{
    private const string DbFile = "redb_tests_sqlite_collation.db";

    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        // Reuse the configured shape (pragmas, pooling) but a different file.
        var baseCs = config.GetConnectionString("Sqlite")!;
        var cs = System.Text.RegularExpressions.Regex.Replace(
            baseCs, @"Data Source\s*=\s*[^;]+", "Data Source=" + DbFile,
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        SqliteTestSupport.DeleteDbFiles(cs);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UseSqlite(cs)
            .Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.DeleteInsert;
                c.SkipHashValidationOnCacheCheck = false;
                c.EnableLazyLoadingForProps = false;
                c.EnablePropsCache = false;
                // On SQLite the value is not a collation name — there is nothing to attach it to.
                // It is the switch that installs the Unicode-aware like/lower/upper overrides on
                // every connection. The name is validated all the same, so a typo fails at startup
                // rather than turning into silence.
                c.StringCollation = PostgresCollationFixture.Collation;
                c.CacheDomain = "collation-sqlite";
            }));

        ServiceProvider = services.BuildServiceProvider();
        Redb = ServiceProvider.GetRequiredService<IRedbService>();

        try { await Redb.InitializeAsync(ensureCreated: true); }
        catch { await Redb.InitializeAsync(); }

        await Redb.SyncSchemeAsync<CollationProps>();
        await Redb.InitializeTypeRegistryAsync();

        var ctx = ServiceProvider.GetRequiredService<IRedbContext>();
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
