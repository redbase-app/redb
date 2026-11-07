using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProTrashPurgeTests : TrashPurgeTestsBase
{
    public MsSqlProTrashPurgeTests(MsSqlProFixture fixture) : base(fixture.ServiceProvider) { }
}
