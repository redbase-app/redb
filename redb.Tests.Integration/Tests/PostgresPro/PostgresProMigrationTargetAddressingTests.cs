using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProMigrationTargetAddressingTests : MigrationTargetAddressingTestsBase
{
    public PostgresProMigrationTargetAddressingTests(PostgresProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }
}
