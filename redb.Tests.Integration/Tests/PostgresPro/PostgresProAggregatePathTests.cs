using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProAggregatePathTests : AggregatePathTestsBase
{
    public PostgresProAggregatePathTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}