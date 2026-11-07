using redb.Core.Data;
using redb.MSSql.Data;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlAmbientTransactionTests : AmbientTransactionTestsBase
{
    public MsSqlAmbientTransactionTests(MsSqlFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string CreateProbeTableSql =>
        "IF OBJECT_ID(N'dbo.ambient_tx_rows', N'U') IS NULL CREATE TABLE dbo.ambient_tx_rows (tag nvarchar(100) NOT NULL);";

    protected override IRedbConnection CreateStandaloneConnection(IServiceProvider services, bool differentSessionSettings)
        => new SqlRedbConnection(ConnString("MSSql"), lazyReferences: differentSessionSettings);
}
