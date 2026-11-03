using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public SqliteAnalyticsTelemetryTests(SqliteFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 2;
}
