using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProDisposedScopeGuardTests : DisposedScopeGuardTestsBase
{
    public SqliteProDisposedScopeGuardTests(SqliteProFixture fixture) : base(fixture.ServiceProvider) { }
}
