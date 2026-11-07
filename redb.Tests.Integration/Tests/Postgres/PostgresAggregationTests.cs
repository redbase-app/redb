using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresAggregationTests : AggregationTestsBase
{
    public PostgresAggregationTests(PostgresFixture fixture) : base(fixture.Redb, isPro: false) { }
}
