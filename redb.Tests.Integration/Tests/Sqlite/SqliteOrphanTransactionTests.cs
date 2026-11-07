using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteOrphanTransactionTests : OrphanTransactionTestsBase
{
    public SqliteOrphanTransactionTests(SqliteFixture fixture) : base(fixture.ServiceProvider) { }

    // A root SAVEPOINT opens a transaction; the INSERT takes the write lock; the third statement fails.
    protected override string FailingBatchThatOpensATransaction =>
        "SAVEPOINT redb_orphan_probe; INSERT INTO _global_identity DEFAULT VALUES; SELECT * FROM redb_no_such_table;";
}
