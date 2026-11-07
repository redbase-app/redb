using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresUniqueKeyTests : UniqueKeyTestsBase
{
    public PostgresUniqueKeyTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
