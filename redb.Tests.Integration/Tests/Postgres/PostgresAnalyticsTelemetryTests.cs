using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public PostgresAnalyticsTelemetryTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 2;
}
