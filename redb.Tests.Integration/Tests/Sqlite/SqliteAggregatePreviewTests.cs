using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteAggregatePreviewTests : FreeAggregatePreviewTestsBase
{
    public SqliteAggregatePreviewTests(SqliteFixture fixture) : base(fixture.Redb) { }
}
