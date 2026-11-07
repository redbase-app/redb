using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProDisposedScopeGuardTests : DisposedScopeGuardTestsBase
{
    public MsSqlProDisposedScopeGuardTests(MsSqlProFixture fixture) : base(fixture.ServiceProvider) { }
}
