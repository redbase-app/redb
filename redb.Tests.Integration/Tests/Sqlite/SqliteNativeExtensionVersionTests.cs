using redb.Core;
using redb.SQLite.Sql;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteNativeExtensionVersionTests
{
    private readonly IRedbService _redb;

    public SqliteNativeExtensionVersionTests(SqliteFixture fixture) => _redb = fixture.Redb;

    /// <summary>
    /// The extension the tests load reports the version the dialect requires. A stale build next to the tests is
    /// caught here by name, not by a query that answers in an old shape; and the dialect names the function the
    /// initialization gate asks, as the PostgreSQL and SQL Server dialects do.
    /// </summary>
    [Fact]
    public async Task TheLoadedExtension_ReportsTheRequiredVersion()
    {
        var dialect = new SqliteDialect();
        dialect.Query_PvtModuleVersionFunction().Should().Be("pvt_module_version");
        dialect.Query_PvtRequiredVersion().Should().NotBeNullOrEmpty();

        var deployed = await _redb.Context.ExecuteScalarAsync<string>("SELECT pvt_module_version()");

        deployed.Should().Be(dialect.Query_PvtRequiredVersion());
    }
}
