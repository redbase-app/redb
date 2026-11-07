using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresTrashPurgeTests : TrashPurgeTestsBase
{
    public PostgresTrashPurgeTests(PostgresFixture fixture) : base(fixture.ServiceProvider) { }
}
