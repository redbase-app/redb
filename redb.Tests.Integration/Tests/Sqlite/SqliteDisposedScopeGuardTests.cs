using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteDisposedScopeGuardTests : DisposedScopeGuardTestsBase
{
    public SqliteDisposedScopeGuardTests(SqliteFixture fixture) : base(fixture.ServiceProvider) { }
}
