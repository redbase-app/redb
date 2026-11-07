using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlTreeLevelAnalyticsTests : TreeLevelAnalyticsTestsBase
{
    public MsSqlTreeLevelAnalyticsTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
