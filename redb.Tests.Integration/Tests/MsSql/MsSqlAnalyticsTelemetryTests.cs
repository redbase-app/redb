using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public MsSqlAnalyticsTelemetryTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 2;
}
