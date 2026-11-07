using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProTagsFieldTests : TagsFieldTestsBase
{
    public SqliteProTagsFieldTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
