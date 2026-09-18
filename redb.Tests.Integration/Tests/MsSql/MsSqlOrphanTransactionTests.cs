using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlOrphanTransactionTests : OrphanTransactionTestsBase
{
    public MsSqlOrphanTransactionTests(MsSqlFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string FailingBatchThatOpensATransaction =>
        "BEGIN TRANSACTION; RAISERROR(N'redb orphan-transaction probe', 16, 1);";
}
