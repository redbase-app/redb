using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// A PostgreSQL database created with <c>LC_CTYPE=C</c>, which is the only environment where the
/// case-folding defect is visible at all.
///
/// <para>
/// This fixture exists because the ordinary test database is created with <c>en_US.utf8</c>, where
/// <c>'Привет' ILIKE '%привет%'</c> is already true and every assertion below would pass with the
/// feature removed. That is precisely how the defect survived: the environment masked it, the same
/// way an all-UTC test set masked the DateTime one. A suite that cannot fail on the unfixed code is
/// not a regression test.
/// </para>
///
/// <para>
/// The database is created once and reused, so a re-run costs nothing. It is deliberately NOT
/// dropped on dispose: recreating a database with a non-default locale is slow, and leaving it
/// keeps the next run fast. It carries a name no other suite uses.
/// </para>
/// </summary>
public sealed class PostgresCollationFixture : IAsyncLifetime
{
    /// <summary>Name of the C-ctype database. Distinct from every other suite's.</summary>
    public const string DatabaseName = "redb_collation_c";

    /// <summary>Collation used by the tests. Deterministic, which LIKE requires.</summary>
    public const string Collation = "und-x-icu";

    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    /// <summary>Connection string of the C-ctype database, for a second fixture on the same data.</summary>
    public static string ConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var baseCs = config.GetConnectionString("Postgres")!;

        ConnectionString = await EnsureCtypeCDatabaseAsync(baseCs);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UsePostgres(ConnectionString)
            .Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.DeleteInsert;
                c.SkipHashValidationOnCacheCheck = false;
                c.EnableLazyLoadingForProps = false;
                c.EnablePropsCache = false;
                c.StringCollation = Collation;
                // A distinct cache domain: this process also holds services pointed at the ordinary
                // test database, and the metadata caches are keyed by domain. Sharing one would let
                // a scheme id from the other database answer a lookup here.
                c.CacheDomain = "collation-c";
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

    /// <summary>
    /// Creates the database if it is absent. TEMPLATE template0 is required: template1 carries the
    /// cluster's own locale and CREATE DATABASE refuses a different one from it.
    /// </summary>
    private static async Task<string> EnsureCtypeCDatabaseAsync(string baseConnectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(baseConnectionString);
        var target = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = DatabaseName };

        builder.Database = "postgres";
        await using var admin = new NpgsqlConnection(builder.ConnectionString);
        await admin.OpenAsync();

        await using (var check = admin.CreateCommand())
        {
            check.CommandText = "SELECT 1 FROM pg_database WHERE datname = $1";
            check.Parameters.Add(new NpgsqlParameter { Value = DatabaseName });
            if (await check.ExecuteScalarAsync() is not null)
                return target.ConnectionString;
        }

        await using (var create = admin.CreateCommand())
        {
            // The name is a compile-time constant, not input, so interpolation here is not a hole.
            create.CommandText =
                $"CREATE DATABASE \"{DatabaseName}\" TEMPLATE template0 " +
                "LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8'";
            await create.ExecuteNonQueryAsync();
        }

        return target.ConnectionString;
    }

    public async Task DisposeAsync()
    {
        if (ServiceProvider is IAsyncDisposable ad)
            await ad.DisposeAsync();
        else
            ServiceProvider?.Dispose();
    }
}

/// <summary>
/// The same C-ctype database, with the feature OFF. Proves the other half of the contract: without
/// the setting the behaviour is exactly what it was, so nobody's existing deployment changes under
/// them. Runs after <see cref="PostgresCollationFixture"/> has created the database and schema.
/// </summary>
public sealed class PostgresNoCollationFixture : IAsyncLifetime
{
    public IRedbService Redb { get; private set; } = null!;
    public ServiceProvider ServiceProvider { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var baseCs = config.GetConnectionString("Postgres")!;
        var target = new NpgsqlConnectionStringBuilder(baseCs)
        {
            Database = PostgresCollationFixture.DatabaseName
        };

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UsePostgres(target.ConnectionString)
            .Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.DeleteInsert;
                c.SkipHashValidationOnCacheCheck = false;
                c.EnableLazyLoadingForProps = false;
                c.EnablePropsCache = false;
                // StringCollation deliberately left unset — that is the point of this fixture.
                c.CacheDomain = "collation-c";
            }));

        ServiceProvider = services.BuildServiceProvider();
        Redb = ServiceProvider.GetRequiredService<IRedbService>();

        try { await Redb.InitializeAsync(ensureCreated: true); }
        catch { await Redb.InitializeAsync(); }

        await Redb.SyncSchemeAsync<CollationProps>();
        await Redb.InitializeTypeRegistryAsync();
    }

    public async Task DisposeAsync()
    {
        if (ServiceProvider is IAsyncDisposable ad)
            await ad.DisposeAsync();
        else
            ServiceProvider?.Dispose();
    }
}
