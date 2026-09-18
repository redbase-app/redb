using redb.Core.Data;
using redb.Postgres.Data;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresAmbientTransactionTests : AmbientTransactionTestsBase
{
    public PostgresAmbientTransactionTests(PostgresFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string CreateProbeTableSql =>
        "CREATE TABLE IF NOT EXISTS ambient_tx_rows (tag varchar(100) NOT NULL)";

    protected override IRedbConnection CreateStandaloneConnection(IServiceProvider services, bool differentSessionSettings)
        => new NpgsqlRedbConnection(NpgsqlDataSourceFactory.Create(
            ConnString("Postgres"), stringCollation: null, lazyReferences: differentSessionSettings));
}
