using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("SqlitePro")]
public class SqliteProKeygenInTransactionTests : SqliteKeygenInTransactionTestsBase
{
    public SqliteProKeygenInTransactionTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
