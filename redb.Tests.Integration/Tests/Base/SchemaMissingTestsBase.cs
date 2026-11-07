using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Extensions;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// <c>InitializeAsync()</c> without <c>ensureCreated: true</c> on a database that has no redb
/// schema. Creation is opt-in by design (owner, 2026-09-12: the application role usually may not
/// run DDL, and an unexpected empty database is more often a wrong connection string than a
/// fresh installation) - but the failure used to be a raw driver error from the first start-up
/// query, saying nothing about ensureCreated. Pinned: the situation is named by
/// <see cref="RedbSchemaMissingException"/> with the way out in the message, and the way out works.
/// Each suite provides a throwaway empty database on its own server.
/// </summary>
public abstract class SchemaMissingTestsBase
{
    /// <summary>Creates an empty database (no redb schema) and returns its connection string.</summary>
    protected abstract Task<string> CreateEmptyDatabaseAsync();

    /// <summary>Drops the database created by <see cref="CreateEmptyDatabaseAsync"/>.</summary>
    protected abstract Task DropDatabaseAsync(string connectionString);

    protected abstract void UseProvider(RedbOptionsBuilder options, string connectionString);

    protected static string ConnString(string name) =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build()
            .GetConnectionString(name)!;

    private ServiceProvider Build(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            UseProvider(options, connectionString);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"schema-missing-{GetType().Name}";
            });
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task InitializeWithoutEnsureCreated_OnAnEmptyDatabase_NamesTheSituation()
    {
        var cs = await CreateEmptyDatabaseAsync();
        try
        {
            await using (var sp = Build(cs))
            {
                var redb = sp.GetRequiredService<IRedbService>();
                var act = async () => await redb.InitializeAsync();
                var thrown = await act.Should().ThrowAsync<RedbSchemaMissingException>(
                    "an empty database must be named as such, not surface a raw driver error from the first start-up query");
                thrown.Which.Message.Should().Contain("ensureCreated: true", "the message must say how to get out of it");
            }

            // The way out the message names works on the very same database.
            await using (var sp = Build(cs))
            {
                var redb = sp.GetRequiredService<IRedbService>();
                await redb.InitializeAsync(ensureCreated: true);
            }
        }
        finally
        {
            await DropDatabaseAsync(cs);
        }
    }
}
