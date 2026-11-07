using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlTrashPurgeTests : TrashPurgeTestsBase
{
    public MsSqlTrashPurgeTests(MsSqlFixture fixture) : base(fixture.ServiceProvider) { }
}
