using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProAbsentValuesTests : AbsentValuesTestsBase
{
    public SqliteProAbsentValuesTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
