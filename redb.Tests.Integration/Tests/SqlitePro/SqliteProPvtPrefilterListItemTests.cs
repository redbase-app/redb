using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProPvtPrefilterListItemTests : PvtPrefilterListItemTestsBase
{
    public SqliteProPvtPrefilterListItemTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
