using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlMaintenanceTests : MaintenanceTestsBase
{
    public MsSqlMaintenanceTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
