using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public SqliteProAnalyticsTelemetryTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 1;
}
