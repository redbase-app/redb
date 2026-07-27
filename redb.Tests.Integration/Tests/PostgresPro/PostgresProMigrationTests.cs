using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProMigrationTests : MigrationTestsBase
{
    public PostgresProMigrationTests(PostgresProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }
}
