using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteTrashPurgeTests : TrashPurgeTestsBase
{
    public SqliteTrashPurgeTests(SqliteFixture fixture) : base(fixture.ServiceProvider) { }

    // One writer: see the base.
    protected override bool ConcurrentWriters => false;
}
