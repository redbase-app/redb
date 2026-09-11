using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProDisposedScopeGuardTests : DisposedScopeGuardTestsBase
{
    public PostgresProDisposedScopeGuardTests(PostgresProFixture fixture) : base(fixture.ServiceProvider) { }
}
