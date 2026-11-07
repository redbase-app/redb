using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresTypeMigrationSyncTests : TypeMigrationSyncTestsBase
{
    public PostgresTypeMigrationSyncTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override string TextTypeName => "TEXT";

    protected override string BoolTrue => "TRUE";

    protected override string BoolFalse => "FALSE";
}
