using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresSaveGraphHashTests : SaveGraphHashTestsBase
{
    public PostgresSaveGraphHashTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
