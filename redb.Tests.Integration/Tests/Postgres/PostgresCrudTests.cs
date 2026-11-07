using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresCrudTests : CrudTestsBase
{
    public PostgresCrudTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
