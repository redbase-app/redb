using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProUnregisteredTreeTargetTests : UnregisteredTreeTargetTestsBase
{
    public MsSqlProUnregisteredTreeTargetTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
