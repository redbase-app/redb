using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresOrderingTests : OrderingTestsBase
{
    public PostgresOrderingTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
