using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresTransactionIsolationTests : TransactionIsolationTestsBase
{
    public PostgresTransactionIsolationTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override string? ReadLevelSql => "SHOW transaction_isolation";
}
