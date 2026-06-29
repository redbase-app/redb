using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteWhereTests : WhereTestsBase
{
    public SqliteWhereTests(SqliteFixture fixture) : base(fixture.Redb) { }
}
