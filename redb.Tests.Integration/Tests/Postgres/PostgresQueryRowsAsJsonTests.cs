using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresQueryRowsAsJsonTests : QueryRowsAsJsonTestsBase
{
    public PostgresQueryRowsAsJsonTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
