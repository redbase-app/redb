using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresComputedComparisonTests : ComputedComparisonTestsBase
{
    public PostgresComputedComparisonTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
