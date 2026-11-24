using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProQueryOrderTests : QueryOrderTestsBase
{
    public SqliteProQueryOrderTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
