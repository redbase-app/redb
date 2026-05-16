using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresListTests : ListTestsBase
{
    public PostgresListTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
