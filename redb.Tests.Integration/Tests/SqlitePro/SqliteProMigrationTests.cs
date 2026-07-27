using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProMigrationTests : MigrationTestsBase
{
    public SqliteProMigrationTests(SqliteProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }
}
