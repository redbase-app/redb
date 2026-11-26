using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProPageValuesGraphTests : PageValuesGraphTestsBase
{
    public SqliteProPageValuesGraphTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}