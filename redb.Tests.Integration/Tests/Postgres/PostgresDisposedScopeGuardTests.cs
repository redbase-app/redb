using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresDisposedScopeGuardTests : DisposedScopeGuardTestsBase
{
    public PostgresDisposedScopeGuardTests(PostgresFixture fixture) : base(fixture.ServiceProvider) { }
}
