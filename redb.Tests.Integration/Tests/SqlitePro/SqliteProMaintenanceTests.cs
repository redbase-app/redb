using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProMaintenanceTests : MaintenanceTestsBase
{
    public SqliteProMaintenanceTests(SqliteProFixture fixture) : base(fixture.Redb) { }
    // e_sqlite3 may or may not compile dbstat in; the base falls back to the size-less form.
    protected override bool ReportsSizes => false;
}
