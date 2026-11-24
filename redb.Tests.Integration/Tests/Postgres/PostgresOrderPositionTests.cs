using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresOrderPositionTests : FreeOrderPositionTestsBase
{
    public PostgresOrderPositionTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
