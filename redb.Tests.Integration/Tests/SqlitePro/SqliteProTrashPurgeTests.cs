using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProTrashPurgeTests : TrashPurgeTestsBase
{
    public SqliteProTrashPurgeTests(SqliteProFixture fixture) : base(fixture.ServiceProvider) { }

    // One writer: see the base.
    protected override bool ConcurrentWriters => false;
}
