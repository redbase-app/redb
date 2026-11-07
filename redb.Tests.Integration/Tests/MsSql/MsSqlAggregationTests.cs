using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlAggregationTests : AggregationTestsBase
{
    public MsSqlAggregationTests(MsSqlFixture fixture) : base(fixture.Redb, isPro: false) { }
}
