using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresOrphanTransactionTests : OrphanTransactionTestsBase
{
    public PostgresOrphanTransactionTests(PostgresFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string FailingBatchThatOpensATransaction => "BEGIN; SELECT 1/0;";
}
