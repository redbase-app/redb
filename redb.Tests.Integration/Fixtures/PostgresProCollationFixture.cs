using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// PostgreSQL <b>Pro</b> on the C-ctype database, feature ON.
///
/// <para>
/// Free and Pro do not share a code path here. Free builds its SQL inside the database and reads the
/// collation from a GUC; Pro builds it in C# and attaches the clause through the dialect. Testing
/// only Free would leave every Pro user unprotected, and the two could drift apart without a single
/// test going red. This fixture exists because the first version of the suite covered Free alone.
/// </para>
///
/// <para>
/// The PVT prefilter is forced ON regardless of <c>ProTestOptions</c>, because
/// <c>PvtPrefilterRenderer</c> is a separate emitter with its own copy of the ILIKE mapping. With
/// the prefilter off that file is never executed and its folding is untested.
/// </para>
/// </summary>
public sealed class PostgresProCollationFixture : IAsyncLifetime
{
    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var baseCs = config.GetConnectionString("Postgres")!;
        var license = config["Redb:License"];

        // The Free collation fixture owns creation of the C-ctype database; reuse it if it has run,
        // otherwise create it here so this suite can also run alone.
        var cs = new NpgsqlConnectionStringBuilder(baseCs)
        {
            Database = PostgresCollationFixture.DatabaseName
        }.ConnectionString;

        await EnsureDatabaseAsync(baseCs);

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
                    c.EnablePvtPrefilter = true;   // see the class remark
                    c.StringCollation = PostgresCollationFixture.Collation;
                    c.CacheDomain = "collation-c-pro";
                });
            if (!string.IsNullOrWhiteSpace(license))
                options.WithLicense(license);
        });

        ServiceProvider = services.BuildServiceProvider();
        Redb = ServiceProvider.GetRequiredService<IRedbService>();

        try { await Redb.InitializeAsync(ensureCreated: true); }
        catch { await Redb.InitializeAsync(); }

        await Redb.SyncSchemeAsync<CollationProps>();
        await Redb.InitializeTypeRegistryAsync();
    }

    private static async Task EnsureDatabaseAsync(string baseConnectionString)
    {
        var admin = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = "postgres" };
        await using var conn = new NpgsqlConnection(admin.ConnectionString);
        await conn.OpenAsync();

        await using (var check = conn.CreateCommand())
        {
            check.CommandText = "SELECT 1 FROM pg_database WHERE datname = $1";
            check.Parameters.Add(new NpgsqlParameter { Value = PostgresCollationFixture.DatabaseName });
            if (await check.ExecuteScalarAsync() is not null) return;
        }

        await using var create = conn.CreateCommand();
        create.CommandText =
            $"CREATE DATABASE \"{PostgresCollationFixture.DatabaseName}\" TEMPLATE template0 " +
            "LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8'";
        await create.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (ServiceProvider is IAsyncDisposable ad)
            await ad.DisposeAsync();
        else
            ServiceProvider?.Dispose();
    }
}
