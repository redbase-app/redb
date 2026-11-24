using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteConcurrentWritersTests : ConcurrentWritersTestsBase
{
    public SqliteConcurrentWritersTests(SqliteFixture fixture) : base(fixture.ServiceProvider) { }
}