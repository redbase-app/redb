using redb.Core.Data;
using redb.MSSql.Data;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlConnectionTeardownTests : ConnectionTeardownTestsBase
{
    protected override IRedbConnection CreateConnection()
        => new SqlRedbConnection(ConnString("MSSql"));

    protected override string SleepSql(double seconds)
    {
        var span = TimeSpan.FromSeconds(seconds);
        return $"WAITFOR DELAY '{span:hh\\:mm\\:ss\\.fff}'; SELECT CAST(1 AS BIGINT)";
    }
}
