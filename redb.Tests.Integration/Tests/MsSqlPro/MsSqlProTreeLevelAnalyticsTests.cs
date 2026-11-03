using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProTreeLevelAnalyticsTests : TreeLevelAnalyticsTestsBase
{
    public MsSqlProTreeLevelAnalyticsTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
