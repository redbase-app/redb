using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProGroupByTests : GroupByTestsBase
{
    public SqliteProGroupByTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
