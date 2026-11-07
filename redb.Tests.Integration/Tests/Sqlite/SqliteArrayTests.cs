using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteArrayTests : ArrayTestsBase
{
    public SqliteArrayTests(SqliteFixture fixture) : base(fixture.Redb) { }
}
