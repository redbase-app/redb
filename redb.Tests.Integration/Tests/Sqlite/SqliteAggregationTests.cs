using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteAggregationTests : AggregationTestsBase
{
    public SqliteAggregationTests(SqliteFixture fixture) : base(fixture.Redb) { }
}
