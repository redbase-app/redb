using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresTreeSqlPreviewTests : TreeSqlPreviewTestsBase
{
    public PostgresTreeSqlPreviewTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
