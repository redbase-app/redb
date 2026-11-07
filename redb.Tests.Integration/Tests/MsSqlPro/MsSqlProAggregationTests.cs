using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProAggregationTests : AggregationTestsBase
{
    public MsSqlProAggregationTests(MsSqlProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
