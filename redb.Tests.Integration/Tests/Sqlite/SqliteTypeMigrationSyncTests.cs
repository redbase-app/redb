using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteTypeMigrationSyncTests : TypeMigrationSyncTestsBase
{
    public SqliteTypeMigrationSyncTests(SqliteFixture fixture) : base(fixture.Redb) { }

    protected override string TextTypeName => "TEXT";

    protected override string BoolTrue => "1";

    protected override string BoolFalse => "0";
}
