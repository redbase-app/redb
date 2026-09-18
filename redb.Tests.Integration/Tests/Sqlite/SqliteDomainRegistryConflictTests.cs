using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Every service registers its database for the lazy loads that find no live scope, and the most recent
/// registration wins - a host rebuilt in place (tests, hot reload) must take over from the disposed one. Two hosts of
/// one database that disagree on <see cref="RedbServiceConfiguration.LazyLoadWithoutScope"/> therefore flip the policy
/// with every service constructed; that was silent.
/// </summary>
public class SqliteDomainRegistryConflictTests
{
    [Fact]
    public void TwoHostsOfOneDatabase_WithDifferentLazyLoadPolicies_AreReportedOnce()
    {
        const string cs = "Data Source=redb_tests_domain_conflict.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        var logs = new CapturingLoggerProvider();

        using var refusing = Build(cs, LazyLoadWithoutScopeMode.Refuse, logs);
        using var fresh = Build(cs, LazyLoadWithoutScopeMode.FreshScope, logs);
        using var freshAgain = Build(cs, LazyLoadWithoutScopeMode.FreshScope, logs);

        Construct(refusing);
        Construct(fresh);
        Construct(freshAgain);
        Construct(refusing);

        logs.Warnings.Where(w => w.Contains("LazyLoadWithoutScope")).Should().ContainSingle(
                "the conflict is reported when first seen, not on every construction")
            .Which.Should().Contain("Refuse").And.Contain("FreshScope");
    }

    private static void Construct(ServiceProvider host)
    {
        using var scope = host.CreateScope();
        scope.ServiceProvider.GetRequiredService<IRedbService>();
    }

    private static ServiceProvider Build(string cs, LazyLoadWithoutScopeMode mode, CapturingLoggerProvider logs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning).AddProvider(logs));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.LazyLoadWithoutScope = mode);
        });
        return services.BuildServiceProvider();
    }
}
