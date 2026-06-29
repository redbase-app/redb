using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteAggregationOverArrayTests : AggregationOverArrayTestsBase
{
    public SqliteAggregationOverArrayTests(SqliteFixture fixture) : base(fixture.Redb) { }
}
