using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlTypeMigrationSyncTests : TypeMigrationSyncTestsBase
{
    public MsSqlTypeMigrationSyncTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override string TextTypeName => "NVARCHAR(50)";

    protected override string BoolTrue => "1";

    protected override string BoolFalse => "0";
}
