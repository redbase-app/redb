using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProMigrationTargetAddressingTests : MigrationTargetAddressingTestsBase
{
    public MsSqlProMigrationTargetAddressingTests(MsSqlProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }
}
