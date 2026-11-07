using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;
using redb.MSSql.Data;
using redb.MSSql.Providers;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlMaintenanceTests : MaintenanceTestsBase
{
    private readonly MsSqlFixture _fixture;

    public MsSqlMaintenanceTests(MsSqlFixture fixture) : base(fixture.Redb) => _fixture = fixture;

    [Fact]
    public async Task StatisticsWindow_WithoutServerStateRight_HasNoStart()
    {
        // A database owner without VIEW SERVER STATE: a temporary login, db_owner of the test
        // database and nothing at server level. Pooling is off so the login can be dropped at once.
        var adminCs = ((SqlRedbContext)_fixture.ServiceProvider.GetRequiredService<IRedbContext>()).ConnectionString;
        const string login = "redb_test_no_server_state";
        var password = "Rt1!" + Guid.NewGuid().ToString("N");
        var ownerCs = new SqlConnectionStringBuilder(adminCs) { UserID = login, Password = password, Pooling = false }
            .ConnectionString;

        await using var admin = new SqlRedbContext(adminCs);
        await DropLoginAsync(admin, login);
        await admin.ExecuteAsync($"CREATE LOGIN [{login}] WITH PASSWORD = '{password}', CHECK_POLICY = OFF");
        try
        {
            await admin.ExecuteAsync($"CREATE USER [{login}] FOR LOGIN [{login}]");
            await admin.ExecuteAsync($"ALTER ROLE db_owner ADD MEMBER [{login}]");

            await using var owner = new SqlRedbContext(ownerCs);
            var maintenance = new MssqlMaintenanceProvider(owner, 1000);

            var window = await maintenance.GetStatisticsWindowAsync();
            window.CountersSince.Should().BeNull("the instance start time needs a server-level right");

            (await maintenance.GetTableStatsAsync()).Should().Contain(t => t.Table == "_values");
            // The usage counters come from a server-level DMV as well: without the right the
            // indexes and their sizes are still listed, the counters are null.
            var indexes = await maintenance.GetIndexStatsAsync();
            indexes.Should().Contain(s => s.Table == "_values" && s.SizeBytes > 0);
            indexes.Should().OnlyContain(s => s.Seeks == null && s.Scans == null && s.Updates == null);
        }
        finally
        {
            await DropLoginAsync(admin, login);
        }
    }

    private static async Task DropLoginAsync(IRedbContext admin, string login)
    {
        await admin.ExecuteAsync($"DROP USER IF EXISTS [{login}]");
        await admin.ExecuteAsync(
            $"IF SUSER_ID('{login}') IS NOT NULL DROP LOGIN [{login}]");
    }
}
