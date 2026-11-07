using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public PostgresProAnalyticsTelemetryTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 1;
}
