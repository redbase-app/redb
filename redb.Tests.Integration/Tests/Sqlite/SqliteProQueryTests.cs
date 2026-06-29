using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteProQueryTests : ProQueryTestsBase
{
    public SqliteProQueryTests(SqliteFixture fixture) : base(fixture.Redb, isPro: false) { }
}
