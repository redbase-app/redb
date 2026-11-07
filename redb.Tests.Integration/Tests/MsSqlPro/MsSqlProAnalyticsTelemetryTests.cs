using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProAnalyticsTelemetryTests : AnalyticsTelemetryTestsBase
{
    public MsSqlProAnalyticsTelemetryTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override int CommandsPerCall => 1;
}
