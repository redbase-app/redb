using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;
using redb.SQLite.Data;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProAmbientTransactionTests : AmbientTransactionTestsBase
{
    public SqliteProAmbientTransactionTests(SqliteProFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string CreateProbeTableSql =>
        "CREATE TABLE IF NOT EXISTS ambient_tx_rows (tag TEXT NOT NULL)";

    // Case folding, not lazy references: the lazy flag needs the native extension, the case folding functions do not.
    protected override IRedbConnection CreateStandaloneConnection(IServiceProvider services, bool differentSessionSettings)
        => new SqliteRedbConnection(SqliteDataSource.Create(
            services.GetService<SqliteDataSource>()?.ConnectionString ?? ConnString("Sqlite"),
            unicodeCaseFolding: differentSessionSettings, lazyReferences: false));
}
