using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Fixtures;

public sealed class PostgresProFixture : IAsyncLifetime
{
    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        var cs = config.GetConnectionString("Postgres")!;
        var license = config["Redb:License"];

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedbPro(options =>
        {
            options.UsePostgres(cs)
                .Configure(c =>
                {
                    c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;
                    c.SkipHashValidationOnCacheCheck = false;
                    c.EnableLazyLoadingForProps = false;
                    c.EnablePropsCache = false;
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
