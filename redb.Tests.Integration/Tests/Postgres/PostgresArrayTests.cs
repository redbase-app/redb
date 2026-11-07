using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresArrayTests : ArrayTestsBase
{
    public PostgresArrayTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
