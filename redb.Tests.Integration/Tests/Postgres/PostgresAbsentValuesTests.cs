using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresAbsentValuesTests : AbsentValuesTestsBase
{
    public PostgresAbsentValuesTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
