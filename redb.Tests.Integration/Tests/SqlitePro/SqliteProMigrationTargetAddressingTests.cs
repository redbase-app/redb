using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProMigrationTargetAddressingTests : MigrationTargetAddressingTestsBase
{
    public SqliteProMigrationTargetAddressingTests(SqliteProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }
}
