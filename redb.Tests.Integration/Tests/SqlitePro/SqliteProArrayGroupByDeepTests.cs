using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProArrayGroupByDeepTests : ArrayGroupByDeepTestsBase
{
    public SqliteProArrayGroupByDeepTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
