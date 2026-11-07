using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProTreeDistinctTests : TreeDistinctTestsBase
{
    public SqliteProTreeDistinctTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
