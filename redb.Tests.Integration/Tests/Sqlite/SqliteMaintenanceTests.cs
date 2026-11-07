using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteMaintenanceTests : MaintenanceTestsBase
{
    public SqliteMaintenanceTests(SqliteFixture fixture) : base(fixture.Redb) { }
    // e_sqlite3 may or may not compile dbstat in; the base falls back to the size-less form.
    protected override bool ReportsSizes => false;
    protected override bool HasSchemas => false;
    protected override bool ReportsUsageCounters => false;
    protected override bool ReportsAnalyzeTime => false;
    // INTEGER PRIMARY KEY is the rowid itself: SQLite creates no index object for it.
    protected override bool ReportsPrimaryKeyIndexes => false;
}
