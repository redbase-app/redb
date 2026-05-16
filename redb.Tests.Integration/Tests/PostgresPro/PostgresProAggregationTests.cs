using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProAggregationTests : AggregationTestsBase
{
    public PostgresProAggregationTests(PostgresProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
