using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteTransactionIsolationTests : TransactionIsolationTestsBase
{
    public SqliteTransactionIsolationTests(SqliteFixture fixture) : base(fixture.Redb) { }

    /// <summary>SQLite has no isolation-level readout; the base asserts usability instead.</summary>
    protected override string? ReadLevelSql => null;
}
