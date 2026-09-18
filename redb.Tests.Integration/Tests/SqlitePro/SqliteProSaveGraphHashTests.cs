using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProSaveGraphHashTests : SaveGraphHashTestsBase
{
    public SqliteProSaveGraphHashTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
