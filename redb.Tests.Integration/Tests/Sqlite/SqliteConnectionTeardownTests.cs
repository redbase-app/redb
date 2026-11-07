using redb.Core.Data;
using redb.SQLite.Data;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteConnectionTeardownTests : ConnectionTeardownTestsBase
{
    protected override IRedbConnection CreateConnection()
        => new SqliteRedbConnection(ConnString("Sqlite"));

    // SQLite has no sleep(): a recursive counter burns roughly the requested time
    // (~15-20M rows/s on the CI box; the pin only needs "still running at +300ms").
    protected override string SleepSql(double seconds)
        => "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < " +
           $"{(long)(seconds * 18_000_000)}) SELECT COUNT(*) FROM c";
}
